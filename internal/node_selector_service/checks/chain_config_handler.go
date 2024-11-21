package checks

import (
	"github.com/pokt-network/gateway-server/internal/chain_configurations_registry"
	"github.com/pokt-network/gateway-server/pkg/common"
)

// GetBlockHeightTolerance - helper function to retrieve block height tolerance across checks
func GetBlockHeightTolerance(chainConfiguration chain_configurations_registry.ChainConfigurationsService, chainId string, defaultValue int) int {
	chainConfig, ok := chainConfiguration.GetChainConfiguration(chainId)
	if !ok {
		return defaultValue
	}
	return int(*chainConfig.HeightCheckBlockTolerance)
}

// GetDataIntegrityHeightLookback - helper function ro retrieve data integrity lookback across checks
func GetDataIntegrityHeightLookback(chainConfiguration chain_configurations_registry.ChainConfigurationsService, chainId string, defaultValue int) int {
	chainConfig, ok := chainConfiguration.GetChainConfiguration(chainId)
	if !ok {
		return defaultValue
	}
	return int(*chainConfig.DataIntegrityCheckLookbackHeight)
}

// GetFixedHeaders returns the fixed headers for a specific chain configuration.
// It takes a ChainConfigurationsService to retrieve the chain configuration,
// the chainId string to identify the specific chain,
// and a defaultValue map[string]string to return in case the chain configuration is not found.
// The function first retrieves the chain configuration using the chainConfiguration.GetChainConfiguration method.
// If the chain configuration is not found, it returns the defaultValue.
// If the chain configuration is found, it retrieves the fixed headers as a map[string]string from the chain configuration.
// If the fixed headers cannot be cast into a map[string]string, it returns the defaultValue.
// Otherwise, it returns the retrieved fixed headers.
func GetFixedHeaders(chainConfiguration chain_configurations_registry.ChainConfigurationsService, chainId string, defaultValue map[string]string) map[string]string {
	chainConfig, ok := chainConfiguration.GetChainConfiguration(chainId)
	value := defaultValue

	if ok && chainConfig.FixedHeaders != nil {
		if headers, castOk := chainConfig.FixedHeaders.Get().(map[string]string); castOk {
			// apply the specific headers override coming from chain configuration over the defaults one.
			// in this way, the chain configuration on db only needs to hold the overrides or additions that are may
			// not add to base code.
			value = common.MergeStringMaps(value, headers)
		}
	}

	return value
}
