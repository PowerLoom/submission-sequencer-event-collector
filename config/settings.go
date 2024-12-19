package config

import (
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
)

var SettingsObj *Settings

type Settings struct {
	ClientUrl                   string
	ContractAddress             string
	RedisHost                   string
	RedisPort                   string
	RedisDB                     string
	AuthReadToken               string
	SlackReportingUrl           string
	TxRelayerUrl                string
	TxRelayerAuthWriteToken     string
	APIHost                     string
	DataMarketAddresses         []string
	DataMarketContractAddresses []common.Address
	BatchSize                   int
	BlockTime                   int
	HttpTimeout                 int
	PeriodicEligibleCountAlerts bool
	PastDaysBuffer              int
	RetryLimits                 int
	RewardsUpdateBatchSize      int
	RewardsUpdateEpochInterval  int64
}

func LoadConfig() {
	dataMarketAddresses := getEnv("DATA_MARKET_ADDRESSES", "[]")
	dataMarketAddressesList := []string{}

	err := json.Unmarshal([]byte(dataMarketAddresses), &dataMarketAddressesList)
	if err != nil {
		log.Fatalf("Failed to parse DATA_MARKET_ADDRESSES environment variable: %v", err)
	}
	if len(dataMarketAddressesList) == 0 {
		log.Fatalf("DATA_MARKET_ADDRESSES environment variable has an empty array")
	}

	periodicEligibleCountAlerts, periodicEligibleCountAlertsErr := strconv.ParseBool(getEnv("PERIODIC_ELIGIBLE_COUNT_ALERTS", "true"))
	if periodicEligibleCountAlertsErr != nil {
		log.Fatalf("Failed to parse PERIODIC_ELIGIBLE_COUNT_ALERTS environment variable: %v", periodicEligibleCountAlertsErr)
	}

	config := Settings{
		ClientUrl:                   getEnv("PROST_RPC_URL", ""),
		ContractAddress:             getEnv("PROTOCOL_STATE_CONTRACT", ""),
		RedisHost:                   getEnv("REDIS_HOST", ""),
		RedisPort:                   getEnv("REDIS_PORT", ""),
		RedisDB:                     getEnv("REDIS_DB", ""),
		AuthReadToken:               getEnv("AUTH_READ_TOKEN", ""),
		SlackReportingUrl:           getEnv("SLACK_REPORTING_URL", ""),
		TxRelayerUrl:                getEnv("TX_RELAYER_URL", ""),
		TxRelayerAuthWriteToken:     getEnv("TX_RELAYER_AUTH_WRITE_TOKEN", ""),
		DataMarketAddresses:         dataMarketAddressesList,
		PeriodicEligibleCountAlerts: periodicEligibleCountAlerts,
		APIHost:                     getEnv("API_HOST", ""),
	}

	if config.AuthReadToken == "" {
		log.Fatalf("AUTH_READ_TOKEN environment variable is not set")
	}

	for _, addr := range config.DataMarketAddresses {
		config.DataMarketContractAddresses = append(config.DataMarketContractAddresses, common.HexToAddress(addr))
	}

	batchSize, batchSizeParseErr := strconv.Atoi(getEnv("BATCH_SIZE", ""))
	if batchSizeParseErr != nil {
		log.Fatalf("Failed to parse BATCH_SIZE environment variable: %v", batchSizeParseErr)
	}
	config.BatchSize = batchSize

	rewardsUpdateBatchSize, rewardsUpdateBatchSizeParseErr := strconv.Atoi(getEnv("REWARDS_UPDATE_BATCH_SIZE", ""))
	if rewardsUpdateBatchSizeParseErr != nil {
		log.Fatalf("Failed to parse REWARDS_UPDATE_BATCH_SIZE environment variable: %v", rewardsUpdateBatchSizeParseErr)
	}
	config.RewardsUpdateBatchSize = rewardsUpdateBatchSize

	rewardsUpdateEpochInterval, rewardsUpdateEpochIntervalParseErr := strconv.Atoi(getEnv("REWARDS_UPDATE_EPOCH_INTERVAL", ""))
	if rewardsUpdateEpochIntervalParseErr != nil {
		log.Fatalf("Failed to parse REWARDS_UPDATE_EPOCH_INTERVAL environment variable: %v", rewardsUpdateEpochIntervalParseErr)
	}
	config.RewardsUpdateEpochInterval = int64(rewardsUpdateEpochInterval)

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

	pastDaysBuffer, pastDaysBufferParseErr := strconv.Atoi(getEnv("PAST_DAYS_BUFFER", "5"))
	if pastDaysBufferParseErr != nil {
		log.Fatalf("Failed to parse PAST_DAYS_BUFFER environment variable: %v", pastDaysBufferParseErr)
	}
	config.PastDaysBuffer = pastDaysBuffer

	retryLimits, retryLimitsParseErr := strconv.Atoi(getEnv("RETRY_LIMITS", "3"))
	if retryLimitsParseErr != nil {
		log.Fatalf("Failed to parse RETRY_LIMITS environment variable: %v", retryLimitsParseErr)
	}
	config.RetryLimits = retryLimits

	SettingsObj = &config
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
