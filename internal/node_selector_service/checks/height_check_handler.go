package checks

import (
	"fmt"
	"github.com/pokt-network/gateway-server/internal/node_selector_service/models"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
	"gonum.org/v1/gonum/stat"
	"math"
	"time"
)

const (
	defaultNodeHeightCheckInterval     = time.Minute * 5
	defaultZScoreHeightThreshold       = 3
	defaultHeightTolerance         int = 100
	defaultCheckPenalty                = time.Minute * 5
)

var (
	defaultHeaders = map[string]string{"content-type": "application/json"}
)

type HeightJsonParser func(response string) (uint64, error)

// PerformDefaultHeightCheck is the default implementation of a height check by:
// 0. Filtering out nodes that have not been checked since defaultNodeHeightCheckInterval
// 1. Sending height request via payload to all the nodes
// 2. Punishing all nodes that return an error
// 3. Filtering out nodes that are returning a height out of the zScore threshold
// 4. Punishing the nodes with defaultCheckPenalty that exceed the height tolerance.
func PerformDefaultHeightCheck(check *Check, payload string, path string, parseHeight HeightJsonParser, logger *zap.Logger) {
	chainId := check.NodeList[0].GetChain()
	logger.Sugar().Infow("running default height check", "chain", chainId)
	checkHeaders := GetFixedHeaders(check.ChainConfiguration, chainId, defaultHeaders)

	var nodesResponded []*models.QosNode
	// Send request to all nodes
	relayResponses := SendRelaysAsync(check.PocketRelayer, getEligibleHeightCheckNodes(check.NodeList), payload, "POST", path, checkHeaders)

	// Process relay responses
	for resp := range relayResponses {
		err := resp.Error
		if err != nil {
			DefaultPunishNode(err, resp.Node, logger)
			continue
		}

		height, err := parseHeight(resp.Relay.Response)
		if err != nil {
			logger.Sugar().Warnw("failed to unmarshal response", "err", err)
			// Treat an invalid response as a timeout error
			DefaultPunishNode(fasthttp.ErrTimeout, resp.Node, logger)
			continue
		}

		resp.Node.SetLastHeightCheckTime(time.Now())
		resp.Node.SetLastKnownHeight(height)
		nodesResponded = append(nodesResponded, resp.Node)
	}

	highestNodeHeight := getHighestNodeHeight(nodesResponded, defaultZScoreHeightThreshold)
	// Compare each node's reported height against the highest reported height
	for _, node := range nodesResponded {
		heightDifference := int(highestNodeHeight - node.GetLastKnownHeight())
		// Penalize nodes whose reported height is significantly lower than the highest reported height
		if heightDifference > GetBlockHeightTolerance(check.ChainConfiguration, node.GetChain(), defaultHeightTolerance) {
			logger.Sugar().Infow("node is out of sync", "node", node.MorseNode.ServiceUrl, "heightDifference", heightDifference, "nodeSyncedHeight", node.GetLastKnownHeight(), "highestNodeHeight", highestNodeHeight, "chain", node.GetChain())
			// Punish Node specifically due to timeout.
			node.SetSynced(false)
			node.SetTimeoutUntil(time.Now().Add(defaultCheckPenalty), models.OutOfSyncTimeout, fmt.Errorf("heightDifference: %d, nodeSyncedHeight: %d, highestNodeHeight: %d", heightDifference, node.GetLastKnownHeight(), highestNodeHeight))
		} else {
			node.SetSynced(true)
		}
	}
}

// getHighestHeight returns the highest height reported from a slice of nodes
// by using z-score threshhold to prevent any misconfigured or malicious node
func getHighestNodeHeight(nodes []*models.QosNode, zScoreHeightThreshhold float64) uint64 {

	var nodeHeights []float64
	for _, node := range nodes {
		nodeHeights = append(nodeHeights, float64(node.GetLastKnownHeight()))
	}

	// Calculate mean and standard deviation
	meanValue := stat.Mean(nodeHeights, nil)
	stdDevValue := stat.StdDev(nodeHeights, nil)

	var highestNodeHeight float64
	for _, nodeHeight := range nodeHeights {

		zScore := stat.StdScore(nodeHeight, meanValue, stdDevValue)

		// height is an outlier according to zScore threshold
		if math.Abs(zScore) > zScoreHeightThreshhold {
			continue
		}
		// Height is higher than last recorded height
		if nodeHeight > highestNodeHeight {
			highestNodeHeight = nodeHeight
		}
	}
	return uint64(highestNodeHeight)
}

func getEligibleHeightCheckNodes(nodes []*models.QosNode) []*models.QosNode {
	// Filter nodes based on last checked time
	var eligibleNodes []*models.QosNode
	for _, node := range nodes {
		if node.GetLastHeightCheckTime().IsZero() || time.Since(node.GetLastHeightCheckTime()) >= defaultNodeHeightCheckInterval {
			eligibleNodes = append(eligibleNodes, node)
		}
	}
	return eligibleNodes
}
