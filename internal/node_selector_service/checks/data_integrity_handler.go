package checks

import (
	"fmt"
	"github.com/pokt-network/gateway-server/internal/node_selector_service/models"
	"github.com/pokt-network/gateway-server/pkg/common"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
	"time"
)

const (
	// how often to check a node's data integrity
	dataIntegrityNodeCheckInterval = time.Minute * 10

	// penalty whenever a pocket node doesn't match other node providers responses
	dataIntegrityTimePenalty = time.Minute * 15

	// the look back we will use to determine which block number to do a data integrity against (latestBlockHeight - lookBack)
	dataIntegrityHeightLookbackDefault = 25
)

var (
	dataIntegrityHeadersDefault = map[string]string{"content-type": "application/json"}
)

type nodeHashRspPair struct {
	node            *models.QosNode
	blockIdentifier string
}

type BlockHashParser func(response string) (string, error)

type GetBlockByNumberPayloadFmter func(blockToFind uint64) string

// PerformDataIntegrityCheck: is the default implementation of a data integrity check by:
func PerformDataIntegrityCheck(check *Check, calculatePayload GetBlockByNumberPayloadFmter, path string, retrieveBlockIdentifier BlockHashParser, logger *zap.Logger) {
	// Find a node that has been reported as healthy to use as source of truth
	sourceOfTruth := findRandomHealthyNode(check.NodeList)

	// Node that is synced cannot be found, so we cannot run data integrity checks since we need a trusted source
	if sourceOfTruth == nil {
		logger.Sugar().Warnw("cannot find source of truth for data integrity check", "chain", check.NodeList[0].GetChain())
		return
	}

	logger.Sugar().Infow("running default data integrity check", "chain", check.NodeList[0].GetChain())

	// Map to count number of nodes that return blockHash -> counter
	nodeResponseCounts := make(map[string]int)

	var nodeResponsePairs []*nodeHashRspPair

	chainId := sourceOfTruth.GetChain()
	checkHeaders := GetFixedHeaders(check.ChainConfiguration, chainId, dataIntegrityHeadersDefault)

	// find a random block to search that nodes should have access too
	blockNumberToSearch := sourceOfTruth.GetLastKnownHeight() - uint64(GetDataIntegrityHeightLookback(check.ChainConfiguration, chainId, dataIntegrityHeightLookbackDefault))
	attestationResponses := SendRelaysAsync(check.PocketRelayer, getEligibleDataIntegrityCheckNodes(check.NodeList), calculatePayload(blockNumberToSearch), "POST", path, checkHeaders)
	for rsp := range attestationResponses {

		if rsp.Error != nil {
			DefaultPunishNode(rsp.Error, rsp.Node, logger)
			continue
		}

		blockIdentifier, err := retrieveBlockIdentifier(rsp.Relay.Response)
		if err != nil {
			logger.Sugar().Warnw("failed to unmarshal response", "err", err)
			DefaultPunishNode(fasthttp.ErrTimeout, rsp.Node, logger)
			continue
		}

		rsp.Node.SetLastDataIntegrityCheckTime(time.Now())
		nodeResponsePairs = append(nodeResponsePairs, &nodeHashRspPair{
			node:            rsp.Node,
			blockIdentifier: blockIdentifier,
		})
		nodeResponseCounts[blockIdentifier]++
	}

	majorityBlockIdentifier := findMajorityBlockIdentifier(nodeResponseCounts)

	// Blcok blockIdentifier must not be empty
	if majorityBlockIdentifier == "" {
		return
	}

	// Penalize other node operators with a timeout if they don't attest with same block blockIdentifier.
	for _, nodeResp := range nodeResponsePairs {
		if nodeResp.blockIdentifier != majorityBlockIdentifier {
			logger.Sugar().Errorw("punishing node for failed data integrity check", "node", nodeResp.node.MorseNode.ServiceUrl, "nodeBlockHash", nodeResp.blockIdentifier, "trustedSourceBlockHash", majorityBlockIdentifier)
			nodeResp.node.SetTimeoutUntil(time.Now().Add(dataIntegrityTimePenalty), models.DataIntegrityTimeout, fmt.Errorf("nodeBlockHash %s, trustedSourceBlockHash %s", nodeResp.blockIdentifier, majorityBlockIdentifier))
		}
	}

}

// findRandomHealthyNode - returns a healthy node that is synced so we can use it as a source of truth for data integrity checks
func findRandomHealthyNode(nodes []*models.QosNode) *models.QosNode {
	var healthyNodes []*models.QosNode
	for _, node := range nodes {
		if node.IsHealthy() {
			healthyNodes = append(healthyNodes, node)
		}
	}
	healthyNode, ok := common.GetRandomElement(healthyNodes)
	if !ok {
		return nil
	}
	return healthyNode
}

func getEligibleDataIntegrityCheckNodes(nodes []*models.QosNode) []*models.QosNode {
	// Filter nodes based on last checked time
	var eligibleNodes []*models.QosNode
	for _, node := range nodes {
		if (node.GetLastDataIntegrityCheckTime().IsZero() || time.Since(node.GetLastDataIntegrityCheckTime()) >= dataIntegrityNodeCheckInterval) && node.IsHealthy() {
			eligibleNodes = append(eligibleNodes, node)
		}
	}
	return eligibleNodes
}

// findMajorityBlockIdentifier finds the blockIdentifier with the highest response count
func findMajorityBlockIdentifier(responseCounts map[string]int) string {
	var highestResponseIdentifier string
	var highestResponseCount int
	for rsp, count := range responseCounts {
		if count > highestResponseCount {
			highestResponseIdentifier = rsp
			highestResponseCount = count
		}
	}
	return highestResponseIdentifier
}
