package checks

import (
	"encoding/json"
	"pokt_gateway_server/internal/qos_node_registry/models"
	"pokt_gateway_server/pkg/pokt/pokt_v0"
	relayer_models "pokt_gateway_server/pkg/pokt/pokt_v0/models"
	"sync"
	"time"
)

const (
	evmHeightCheckInterval = time.Minute * 1
	heightJsonPayload      = `{"jsonrpc":"2.0","method":"eth_blockNumber","params": [],"id":1}`
	defaultHeightThreshold = 100
)

type evmHeightResponse struct {
	Height uint64 `json:"result"`
}

type EvmHeightCheck struct {
	Check
	relayer pokt_v0.PocketRelayer
}

type nodeRelayResponse struct {
	Node    *models.QosNode
	Relay   *relayer_models.SendRelayResponse
	Success bool
}

func (c *EvmHeightCheck) PerformJob() {

	var highestHeight uint64
	var wg sync.WaitGroup

	// Define a channel to receive relay responses
	relayResponses := make(chan *nodeRelayResponse)
	defer close(relayResponses)

	// Define a function to handle sending relay requests concurrently
	sendRelayAsync := func(node *models.QosNode) {
		defer wg.Done()
		relay, err := c.relayer.SendRelay(&relayer_models.SendRelayRequest{
			Payload:            &relayer_models.Payload{Data: heightJsonPayload, Method: "POST"},
			Chain:              c.ChainId,
			SelectedNodePubKey: node.MorseNode.PublicKey,
		})
		relayResponses <- &nodeRelayResponse{
			Node:    node,
			Relay:   relay,
			Success: err == nil,
		}
	}

	// Start a goroutine for each node to send relay requests concurrently
	for _, node := range c.NodeList {
		wg.Add(1)
		go sendRelayAsync(node)
	}

	wg.Wait()
	// Process relay responses
	for resp := range relayResponses {
		if resp.Success {
			var evmHeightResp evmHeightResponse
			err := json.Unmarshal([]byte(resp.Relay.Response), &evmHeightResp)
			if err != nil {
				continue
			}
			resp.Node.SetLastHeightCheckTime(time.Now())
			resp.Node.SetLastKnownHeight(evmHeightResp.Height)
			// We track the session's highest height to make a majority decision
			reportedHeight := evmHeightResp.Height
			if reportedHeight > highestHeight {
				highestHeight = reportedHeight
			}
		}
	}

	// Compare each node's reported height against the highest reported height
	for _, node := range c.NodeList {
		heightDifference := int64(highestHeight) - int64(node.GetLastKnownHeight())
		// Penalize nodes whose reported height is significantly lower than the highest reported height
		if heightDifference > defaultHeightThreshold {
			node.SetSynced(false)
			node.SetTimeoutUntil(time.Now().Add(timeoutPenalty), models.OutOfSyncTimeout)
		} else {
			node.SetSynced(true)
		}
	}
	c.LastChecked = time.Now()
}

func (c *EvmHeightCheck) ShouldRun() bool {
	return time.Now().Sub(c.LastChecked) > evmHeightCheckInterval
}

func (c *EvmHeightCheck) getEligibleNodes() []*models.QosNode {
	// Filter nodes based on last checked time
	var eligibleNodes []*models.QosNode
	for _, node := range c.NodeList {
		if time.Since(node.GetLastHeightCheckTime()) >= minLastCheckedNodeTime {
			eligibleNodes = append(eligibleNodes, node)
		}
	}
	return eligibleNodes
}
