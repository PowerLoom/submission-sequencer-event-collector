package config

import (
	"log"
	"os"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
)

var SettingsObj *Settings

type Settings struct {
	ClientUrl                 string
	ContractAddress           string
	RedisHost                 string
	RedisPort                 string
	RedisDB                   string
	SlackReportingUrl         string
	DataMarketAddress         string
	DataMarketContractAddress common.Address
	BlockTime                 int
	HttpTimeout               int
}

func LoadConfig() {
	config := Settings{
		ClientUrl:                 getEnv("PROST_RPC_URL", ""),
		ContractAddress:           getEnv("PROTOCOL_STATE_CONTRACT", ""),
		RedisHost:                 getEnv("REDIS_HOST", ""),
		RedisPort:                 getEnv("REDIS_PORT", ""),
		RedisDB:                   getEnv("REDIS_DB", ""),
		SlackReportingUrl:         getEnv("SLACK_REPORTING_URL", ""),
		DataMarketAddress:         getEnv("DATA_MARKET_CONTRACT", ""),
		DataMarketContractAddress: common.HexToAddress(getEnv("DATA_MARKET_CONTRACT", "")),
	}

	blockTime, blockTimeParseErr := strconv.Atoi(getEnv("BLOCK_TIME", ""))
	if blockTimeParseErr != nil {
		log.Fatalf("Failed to parse BLOCK_TIME environment variable: %v", blockTimeParseErr)
	}
	config.BlockTime = blockTime

	httpTimeout, timeoutParseErr := strconv.Atoi(getEnv("HTTP_TIMEOUT", ""))
	if timeoutParseErr != nil {
		log.Fatalf("Failed to parse HTTP_TIMEOUT environment variable: %v", timeoutParseErr)
	}
	config.HttpTimeout = httpTimeout

	SettingsObj = &config
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
