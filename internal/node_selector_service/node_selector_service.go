package node_selector_service

import (
	"go.uber.org/zap"
	"pokt_gateway_server/internal/node_selector_service/checks"
	"pokt_gateway_server/internal/node_selector_service/models"
	"pokt_gateway_server/internal/session_registry"
	"pokt_gateway_server/pkg/common"
	"pokt_gateway_server/pkg/pokt/pokt_v0"
	"time"
)

const (
	jobCheckInterval = time.Second
)

type NodeSelectorService struct {
	sessionRegistry session_registry.SessionRegistryService
	pocketRelayer   pokt_v0.PocketRelayer
	logger          *zap.Logger
	checkJobs       []checks.CheckJob
}

func NewNodeSelectorService(sessionRegistry session_registry.SessionRegistryService, pocketRelayer pokt_v0.PocketRelayer, logger *zap.Logger) *NodeSelectorService {

	// base checks will share same node list and pocket relayer
	baseCheck := checks.NewCheck(pocketRelayer)

	// enabled checks
	enabledChecks := []checks.CheckJob{
		checks.NewEvmHeightCheck(baseCheck),
		checks.NewEvmDataIntegrityCheck(baseCheck),
	}

	selectorService := &NodeSelectorService{
		sessionRegistry: sessionRegistry,
		logger:          logger,
		checkJobs:       enabledChecks,
	}
	selectorService.startJobChecker()
	return selectorService
}

func (q NodeSelectorService) FindNode(chainId string) (*models.QosNode, bool) {
	var healthyNodes []*models.QosNode
	nodes, found := q.sessionRegistry.GetNodesByChain(chainId)
	if !found {
		return nil, false
	}
	for _, r := range nodes {
		if r.IsHealthy() {
			healthyNodes = append(healthyNodes, r)
		}
	}
	return common.GetRandomElement(healthyNodes), true
}

func (q NodeSelectorService) startJobChecker() {
	ticker := time.Tick(jobCheckInterval)
	go func() {
		for {
			select {
			case <-ticker:
				nodes := q.sessionRegistry.GetNodes()
				for _, job := range q.checkJobs {
					if job.ShouldRun() {
						q.logger.Sugar().Infow("running job", "job", job.Name())
						job.SetNodes(nodes)
						job.Perform()
					}
				}
			}
		}
	}()
}
