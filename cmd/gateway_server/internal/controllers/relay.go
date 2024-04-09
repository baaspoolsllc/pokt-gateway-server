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

	relay, err := c.relayer.SendRelay(&models.SendRelayRequest{
		Payload: &models.Payload{
			Data:   string(ctx.PostBody()),
			Method: string(ctx.Method()),
			Path:   path,
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
