package prost

import (
	"context"
	"encoding/json"
	"math/big"
	"submission-sequencer-collector/config"
	"submission-sequencer-collector/pkgs/clients"
	"submission-sequencer-collector/pkgs/redis"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
)

func setup(t *testing.T) *miniredis.Miniredis {
	// Initialize a miniredis instance
	mockRedis, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}

	// Configure the application to use miniredis
	config.SettingsObj = &config.Settings{
		RedisHost: mockRedis.Host(),
		RedisPort: mockRedis.Port(),
		RedisDB:   "0",
	}

	clients.InitializeReportingClient(config.SettingsObj.SlackReportingUrl, time.Duration(config.SettingsObj.HttpTimeout)*time.Second)

	// Initialize the Redis client
	redis.RedisClient = redis.NewRedisClient()

	// Use t.Cleanup to ensure the miniredis instance is closed after the test
	t.Cleanup(func() {
		redis.RedisClient.FlushAll(context.Background())
		mockRedis.Close()
	})

	return mockRedis
}

// TestCheckAndTriggerBatchPreparation tests the checkAndTriggerBatchPreparation function
func TestCheckAndTriggerBatchPreparation(t *testing.T) {
	setup(t)

	// Define the epoch marker key and its details
	epochKey := "1"
	epochDetails := EpochMarkerDetails{
		SubmissionLimitBlockNumber: 10,
		EpochReleaseBlockNumber:    5,
	}

	// Convert epoch details to JSON
	epochDetailsJSON, err := json.Marshal(epochDetails)
	if err != nil {
		t.Fatalf("Failed to marshal epoch details: %v", err)
	}

	// Store the epoch details in Redis
	if err := redis.StoreEpochDetails(context.Background(), epochKey, string(epochDetailsJSON), 0); err != nil {
		t.Fatalf("Failed to store epoch details in Redis: %v", err)
	}

	// Create a sample block with a matching block number
	currentBlock := types.NewBlock(&types.Header{Number: big.NewInt(10)}, nil, nil, nil, nil)

	// Call the function under test
	checkAndTriggerBatchPreparation(currentBlock)

	// Ensure the epoch key is removed from the Redis set
	members, err := redis.RedisClient.SMembers(context.Background(), redis.EpochMarkerSet()).Result()
	assert.NoError(t, err)
	assert.Empty(t, members)

	// Ensure the details for this epoch key have also been removed
	details, err := redis.RedisClient.Get(context.Background(), epochKey).Result()
	assert.EqualError(t, err, "redis: nil")
	assert.Empty(t, details)
}
