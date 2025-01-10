package prost

import (
	"context"
	"fmt"
	"math/big"
	"submission-sequencer-collector/config"
	"submission-sequencer-collector/pkgs"
	"submission-sequencer-collector/pkgs/clients"
	"submission-sequencer-collector/pkgs/redis"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/ethereum/go-ethereum/core/types"
	log "github.com/sirupsen/logrus"
)

var lastProcessedBlock int64

// StartFetchingBlocks continuously fetches blocks and processes events
func StartFetchingBlocks() {
	log.Println("Submission Event Collector started")

	for {
		// Fetch the latest block available on the chain
		latestBlock, err := fetchBlock(nil)
		if err != nil {
			log.Errorf("Error fetching latest block: %s", err)
			time.Sleep(100 * time.Millisecond) // Sleep briefly before retrying to prevent spamming
			continue
		}

		latestBlockNumber := latestBlock.Number().Int64()

		// Check if lastProcessedBlock is set, if not set it to the latest block
		if lastProcessedBlock == 0 {
			lastProcessedBlock = latestBlockNumber
		}

		// Process any missing blocks between the last processed block and the latest block
		for blockNum := lastProcessedBlock + 1; blockNum <= latestBlockNumber; blockNum++ {
			// Fetch the block by its number by passing the block number
			block, err := fetchBlock(big.NewInt(blockNum))
			if err != nil {
				log.Errorf("Error fetching block %d: %s", blockNum, err)
				continue // Skip this block and continue with the next
			}

			if block == nil {
				log.Errorf("Received nil block for number: %d", blockNum)
				continue
			}

			log.Debugf("Processing block: %d", blockNum)

			// Check and trigger batch preparation if submission limit is reached for any epoch
			go checkAndTriggerBatchPreparation(block)

			// Process the events in the block
			go ProcessEvents(block)

			// Start periodic cleanup of stale epoch marker keys
			go startPeriodicCleanupRoutine(context.Background(), block)

			// Add block number and its hash to Redis
			if err = redis.SetWithExpiration(context.Background(), redis.BlockHashByNumber(blockNum), block.Hash().Hex(), 30*time.Minute); err != nil {
				log.Errorf("Failed to set block hash for block number %d in Redis: %s", blockNum, err)
			}

			// Update last processed block
			lastProcessedBlock = blockNum
		}

		// Sleep for approximately half the expected block time to balance load and responsiveness.
		time.Sleep(time.Duration(config.SettingsObj.BlockTime*500) * time.Millisecond)
	}
}

// fetchBlock retrieves a block from the client using retry logic
func fetchBlock(blockNum *big.Int) (*types.Block, error) {
	var block *types.Block
	operation := func() error {
		var err error
		block, err = Client.BlockByNumber(context.Background(), blockNum) // Pass blockNum (nil for the latest block)
		if err != nil {
			log.Errorf("Failed to fetch block %v: %s", blockNum, err)
			return err // Return the error to trigger a retry
		}
		return nil // Block successfully fetched, return nil to stop retries
	}

	// Retry fetching the block with a backoff strategy
	if err := backoff.Retry(operation, backoff.WithMaxRetries(backoff.NewConstantBackOff(200*time.Millisecond), 3)); err != nil {
		errMsg := fmt.Sprintf("Failed to fetch block %v after retries: %s", blockNum, err.Error())
		clients.SendFailureNotification(pkgs.StartFetchingBlocks, errMsg, time.Now().String(), "High")
		log.Error(errMsg)
		return nil, err
	}

	return block, nil
}
