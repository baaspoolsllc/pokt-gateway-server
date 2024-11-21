package controllers

import (
	"fmt"
	"github.com/pokt-network/gateway-server/cmd/gateway_server/internal/common"
	"github.com/pokt-network/gateway-server/pkg/pokt/pokt_v0"
	"github.com/pokt-network/gateway-server/pkg/pokt/pokt_v0/models"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
	"strings"
)

// RelayController handles relay requests for a specific chain.
type RelayController struct {
	logger  *zap.Logger
	relayer pokt_v0.PocketRelayer
}

// NewRelayController creates a new instance of RelayController.
func NewRelayController(relayer pokt_v0.PocketRelayer, logger *zap.Logger) *RelayController {
	return &RelayController{relayer: relayer, logger: logger}
}

// chainIdLength represents the expected length of chain IDs.
const chainIdLength = 4

// HandleRelay handles incoming relay requests.
func (c *RelayController) HandleRelay(ctx *fasthttp.RequestCtx) {

	chainID, path := getPathSegmented(ctx.Path())

	// Check if the chain ID is empty or has an incorrect length.
	if chainID == "" || len(chainID) != chainIdLength {
		common.JSONError(ctx, "Incorrect chain id", fasthttp.StatusBadRequest, nil)
		return
	}

	contentType := string(ctx.Request.Header.Peek("content-type"))
	if contentType == "" {
		contentType = "application/json"
	}

	relay, err := c.relayer.SendRelay(&models.SendRelayRequest{
		Payload: &models.Payload{
			// TODO: the best here will been able to get the chain configuration to use the configure headers.
			Headers: map[string]string{"content-type": contentType},
			Data:    string(ctx.PostBody()),
			Method:  string(ctx.Method()),
			Path:    path,
		},
		Chain: chainID,
	})

	if err != nil {
		c.logger.Error("Error relaying", zap.Error(err))
		common.JSONError(ctx, fmt.Sprintf("Something went wrong %v", err), fasthttp.StatusInternalServerError, err)
		return
	}

	// Send a successful response back to the client.
	ctx.Response.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.Set("Content-Type", "application/json")
	ctx.Response.SetBodyString(relay.Response)
	return
}

// getPathSegmented: returns the chain being requested and other parts to be proxied to pokt nodes
// Example: /relay/0001/v1/client, returns 0001, /v1/client
func getPathSegmented(path []byte) (chain, otherParts string) {
	paths := strings.Split(string(path), "/")

	if len(paths) >= 3 {
		chain = paths[2]
	}

	if len(paths) > 3 {
		otherParts = "/" + strings.Join(paths[3:], "/")
	}

	return chain, otherParts
}
