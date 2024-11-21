package relayer

import (
	"errors"
	"fmt"
	"github.com/pokt-network/gateway-server/internal/apps_registry"
	"github.com/pokt-network/gateway-server/internal/chain_configurations_registry"
	"github.com/pokt-network/gateway-server/internal/global_config"
	"github.com/pokt-network/gateway-server/internal/node_selector_service"
	"github.com/pokt-network/gateway-server/internal/node_selector_service/checks"
	"github.com/pokt-network/gateway-server/internal/session_registry"
	"github.com/pokt-network/gateway-server/pkg/common"
	"github.com/pokt-network/gateway-server/pkg/pokt/pokt_v0"
	"github.com/pokt-network/gateway-server/pkg/pokt/pokt_v0/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var (
	counterRelayRequest                      *prometheus.CounterVec
	histogramRelayRequestLatency             *prometheus.HistogramVec
	pocketClientHistogramRelayRequestLatency *prometheus.HistogramVec
)

const (
	reasonRelayFailedSessionErr = "relay_session_failure"
	reasonRelayFailedPocketErr  = "relay_pocket_error"
)

var (
	errAltruistNotFound = errors.New("altruist url not found")
	errSelectNodeFail   = errors.New("node selector can't find node")
)

func init() {
	counterRelayRequest = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "relay_counter",
			Help: "Request to send an actual relay and if it succeeded",
		},
		[]string{"success", "altruist", "reason", "chain_id", "service_host"},
	)
	histogramRelayRequestLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 15, 20, 30, 40, 50, 60},
			Name:    "relay_latency",
			Help:    "percentile on the request on latency to select a node, sign a request and send it to the network",
		},
		[]string{"success", "altruist", "chain_id", "service_host"},
	)
	pocketClientHistogramRelayRequestLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 15, 20, 30, 40, 50, 60},
			Name:    "pocket_client_relay_latency",
			Help:    "percentile on the request on latency to sign a request and send it to the network",
		},
		[]string{"success", "chain_id", "service_host"},
	)
	prometheus.MustRegister(counterRelayRequest, histogramRelayRequestLatency, pocketClientHistogramRelayRequestLatency)
}

type Relayer struct {
	globalConfigProvider       global_config.GlobalConfigProvider
	pocketClient               pokt_v0.PocketService
	chainConfigurationRegistry chain_configurations_registry.ChainConfigurationsService
	sessionRegistry            session_registry.SessionRegistryService
	nodeSelector               node_selector_service.NodeSelectorService
	applicationRegistry        apps_registry.AppsRegistryService
	httpRequester              httpRequester
	userAgent                  string
	logger                     *zap.Logger
}

func NewRelayer(pocketService pokt_v0.PocketService, sessionRegistry session_registry.SessionRegistryService, applicationRegistry apps_registry.AppsRegistryService, nodeSelector node_selector_service.NodeSelectorService, altruistRegistry chain_configurations_registry.ChainConfigurationsService, userAgent string, globalConfigProvider global_config.GlobalConfigProvider, logger *zap.Logger) *Relayer {
	return &Relayer{
		pocketClient:               pocketService,
		sessionRegistry:            sessionRegistry,
		logger:                     logger,
		chainConfigurationRegistry: altruistRegistry,
		applicationRegistry:        applicationRegistry,
		nodeSelector:               nodeSelector,
		httpRequester:              fastHttpRequester{},
		globalConfigProvider:       globalConfigProvider,
		userAgent:                  userAgent,
	}
}

func (r *Relayer) SendRelay(req *models.SendRelayRequest) (*models.SendRelayResponse, error) {

	success := false
	altruist := false

	var nodeHost string
	startTime := time.Now()
	// Measure end to end latency for send relay
	defer func() {
		histogramRelayRequestLatency.WithLabelValues(strconv.FormatBool(success), strconv.FormatBool(altruist), req.Chain, nodeHost).Observe(time.Since(startTime).Seconds())
	}()

	rsp, host, err := r.sendNodeSelectorRelay(req)
	// Set the host to record service domain
	nodeHost = host

	// Node selector relay was successful
	if err == nil {
		success = true
		counterRelayRequest.WithLabelValues("true", "false", "", req.Chain, nodeHost).Inc()
		return rsp, nil
	}

	altruist = true
	counterRelayRequest.WithLabelValues("false", "true", reasonRelayFailedPocketErr, req.Chain, "").Inc()

	r.logger.Sugar().Errorw("failed to send to pokt", "poktErr", err)
	altruistRsp, altruistErr := r.altruistRelay(req)
	if altruistErr != nil {
		r.logger.Sugar().Errorw("failed to send to altruist", "altruistError", altruistErr)
		// Prefer to return the network error vs altruist error if both fails.
		return nil, err
	}
	return altruistRsp, nil
}

func (r *Relayer) sendNodeSelectorRelay(req *models.SendRelayRequest) (*models.SendRelayResponse, string, error) {
	// find a node to send too first.
	node, ok := r.nodeSelector.FindNode(req.Chain)
	if !ok {
		return nil, "", errSelectNodeFail
	}
	req.Signer = node.MorseSigner
	req.Session = node.MorseSession
	req.SelectedNodePubKey = node.GetPublicKey()
	if err := req.Validate(); err != nil {
		return nil, "", err
	}

	startRequestTime := time.Now()

	rsp, err := r.pocketClient.SendRelay(req)

	// Record latency to prom and latency tracker
	latency := time.Now().Sub(startRequestTime)

	nodeHost := r.extractHostFromServiceUrl(node.MorseNode.ServiceUrl)
	pocketClientHistogramRelayRequestLatency.WithLabelValues(strconv.FormatBool(err == nil), req.Chain, nodeHost).Observe(latency.Seconds())
	node.GetLatencyTracker().RecordMeasurement(float64(latency.Milliseconds()))
	// Node returned an error, potentially penalize the node operator dependent on error
	if err != nil {
		checks.DefaultPunishNode(err, node, r.logger)
	}

	return rsp, nodeHost, err
}

func (r *Relayer) sendRandomNodeRelay(req *models.SendRelayRequest) (*models.SendRelayResponse, error) {
	// Healthy node could not be found, attempting to use random node
	applications, ok := r.applicationRegistry.GetApplicationsByChainId(req.Chain)
	if !ok {
		return nil, fmt.Errorf("no app found for chain id %s", req.Chain)
	}

	// Get a random app stake from the available list.
	appStake, ok := common.GetRandomElement(applications)
	if !ok {
		return nil, fmt.Errorf("random app stake cannot be found")
	}

	sessionResp, err := r.sessionRegistry.GetSession(&models.GetSessionRequest{
		AppPubKey: appStake.Signer.PublicKey,
		Chain:     req.Chain,
	})

	if err != nil {
		counterRelayRequest.WithLabelValues("false", "true", reasonRelayFailedSessionErr, req.Chain, "").Inc()
		return nil, err
	}

	randomNode, ok := common.GetRandomElement(sessionResp.Nodes)
	if !ok {
		return nil, errors.New("random node in session cannot be found")
	}

	requestTimeout := r.getPocketRequestTimeout(req.Chain)
	// populate request with session metadata
	req.Session = randomNode.MorseSession
	req.Signer = appStake.Signer
	req.Timeout = &requestTimeout
	req.SelectedNodePubKey = randomNode.GetPublicKey()
	rsp, err := r.pocketClient.SendRelay(req)

	// record if relay was successful
	counterRelayRequest.WithLabelValues(strconv.FormatBool(err == nil), "false", "", req.Chain, r.extractHostFromServiceUrl(randomNode.MorseNode.ServiceUrl)).Inc()

	return rsp, err
}

func (r *Relayer) altruistRelay(req *models.SendRelayRequest) (*models.SendRelayResponse, error) {

	chainConfig, ok := r.chainConfigurationRegistry.GetChainConfiguration(req.Chain)

	if !ok {
		return nil, errAltruistNotFound
	}

	// Send to altruist
	request := fasthttp.AcquireRequest()
	response := fasthttp.AcquireResponse()

	defer func() {
		fasthttp.ReleaseRequest(request)
		fasthttp.ReleaseResponse(response)
	}()

	checkHeaders := checks.GetFixedHeaders(r.chainConfigurationRegistry, req.Chain, map[string]string{"content-type": "application/json"})
	for key, value := range checkHeaders {
		request.Header.Set(key, value)
	}

	requestTimeout := r.getAltruistRequestTimeout(req.Chain)
	request.Header.SetUserAgent(r.userAgent)
	request.SetRequestURI(chainConfig.AltruistUrl.String)

	if req.Payload.Method == "POST" {
		request.SetBody([]byte(req.Payload.Data))
	}

	err := r.httpRequester.DoTimeout(request, response, requestTimeout)

	success := err == nil
	counterRelayRequest.WithLabelValues(strconv.FormatBool(success), "true", "", req.Chain, "").Inc()

	if !success {
		return nil, err
	}

	str := string(response.Body())
	return &models.SendRelayResponse{Response: str}, nil
}

func (r *Relayer) getAltruistRequestTimeout(chainId string) time.Duration {
	chainConfig, ok := r.chainConfigurationRegistry.GetChainConfiguration(chainId)
	if !ok {
		return r.globalConfigProvider.GetAltruistRequestTimeout()
	}
	configTime, err := time.ParseDuration(chainConfig.AltruistRequestTimeoutDuration.String)
	if err != nil {
		return r.globalConfigProvider.GetAltruistRequestTimeout()
	}
	return configTime
}

func (r *Relayer) getPocketRequestTimeout(chainId string) time.Duration {
	chainConfig, ok := r.chainConfigurationRegistry.GetChainConfiguration(chainId)
	if !ok {
		return r.globalConfigProvider.GetPoktRPCRequestTimeout()
	}
	configTime, err := time.ParseDuration(chainConfig.PocketRequestTimeoutDuration.String)
	if err != nil {
		return r.globalConfigProvider.GetPoktRPCRequestTimeout()
	}
	return configTime
}

func (r *Relayer) extractHostFromServiceUrl(urlStr string) string {
	if !r.globalConfigProvider.ShouldEmitServiceUrlPromMetrics() {
		return ""
	}
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return "" // return empty string or handle error
	}

	// Get the hostname from the parsed URL
	hostname := parsedURL.Hostname()

	// Find the last occurrence of "." in the hostname
	index := strings.LastIndex(hostname, ".")

	// If there is no "." or it's the first character, return the hostname itself
	if index == -1 || index == 0 {
		return hostname
	}

	// Find the index of the second-to-last occurrence of "." (root domain separator)
	index = strings.LastIndex(hostname[:index-1], ".")
	if index == -1 {
		// If there is only one ".", return the hostname itself
		return hostname
	}

	// Extract and return the root domain
	return hostname[index+1:]
}
