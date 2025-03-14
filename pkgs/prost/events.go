package prost

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"submission-sequencer-collector/config"
	"submission-sequencer-collector/pkgs"
	"submission-sequencer-collector/pkgs/clients"
	"submission-sequencer-collector/pkgs/redis"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	log "github.com/sirupsen/logrus"
)

// ProcessEvents processes all events from a given block with proper context management
func ProcessEvents(ctx context.Context, block *types.Block) error {
	if block == nil {
		return fmt.Errorf("received nil block")
	}

	hash := block.Hash()

	// Create a filter query to fetch logs for the block
	filterQuery := ethereum.FilterQuery{
		BlockHash: &hash,
		Addresses: []common.Address{common.HexToAddress(config.SettingsObj.ContractAddress)},
	}

	var logs []types.Log
	var err error

	operation := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			logs, err = Client.FilterLogs(ctx, filterQuery)
			return err
		}
	}

	if err = backoff.Retry(operation, backoff.WithMaxRetries(backoff.NewConstantBackOff(200*time.Millisecond), 3)); err != nil {
		errorMsg := fmt.Sprintf("Error fetching logs for block number %d: %s", block.Number().Int64(), err.Error())
		clients.SendFailureNotification(pkgs.ProcessEvents, errorMsg, time.Now().String(), "High")
		log.Error(errorMsg)
		return fmt.Errorf("failed to fetch logs: %w", err)
	}

	log.Infof("Processing %d logs for block number %d", len(logs), block.Number().Int64())

	var wg sync.WaitGroup
	errChan := make(chan error, len(logs)) // Buffered channel for errors

	for _, vLog := range logs {
		vLog := vLog
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Check the event signature and handle the events
			switch vLog.Topics[0].Hex() {
			case ContractABI.Events["EpochReleased"].ID.Hex():
				if err := handleEpochReleasedEvent(ctx, block, vLog); err != nil {
					select {
					case errChan <- fmt.Errorf("epoch released event: %w", err):
					default:
						log.Errorf("Error channel full: %v", err)
					}
				}
			case ContractABI.Events["SnapshotBatchSubmitted"].ID.Hex():
				if err := handleSnapshotBatchSubmittedEvent(ctx, block, vLog); err != nil {
					select {
					case errChan <- fmt.Errorf("snapshot batch event: %w", err):
					default:
						log.Errorf("Error channel full: %v", err)
					}
				}
			}
		}()
	}

	// Close error channel after all goroutines complete
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Collect errors
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("multiple event processing errors: %v", errs)
	}

	return nil
}

// processEpochDeadlinesForDataMarkets checks conditions and triggers batch preparation if needed
func processEpochDeadlinesForDataMarkets(ctx context.Context, block *types.Block) error {
	if block == nil {
		return fmt.Errorf("received nil block")
	}

	currentBlockNum := block.Number().Int64()
	log.Infof("🔍 Starting batch preparation check for block number: %d", currentBlockNum)

	var wg sync.WaitGroup
	errChan := make(chan error, len(config.SettingsObj.DataMarketAddresses))

	for _, dataMarketAddress := range config.SettingsObj.DataMarketAddresses {
		dataMarketAddress := dataMarketAddress
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Create timeout context for market processing, use batchProcessingTimeout for batch preparation for a datamarket
			marketCtx, cancel := context.WithTimeout(ctx, batchProcessingTimeout)
			defer cancel()

			log.Infof("Processing started for data market %s at block number: %d", dataMarketAddress, currentBlockNum)
			if err := checkAndTriggerBatchPreparation(marketCtx, dataMarketAddress, currentBlockNum); err != nil {
				select {
				case errChan <- fmt.Errorf("market %s: %w", dataMarketAddress, err):
				default:
					log.Errorf("error channel full: market %s failed: %v", dataMarketAddress, err)
				}
			}
		}()
	}

	// Close error channel after all goroutines complete
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Collect errors
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("multiple market processing errors: %v", errs)
	}

	return nil
}

// handleEpochReleasedEvent processes epoch released events
func handleEpochReleasedEvent(ctx context.Context, block *types.Block, vLog types.Log) error {
	log.Debugf("EpochReleased event detected in block %d", block.Number().Int64())

	// Parse the `EpochReleased` event from the log
	releasedEvent, err := Instance.ParseEpochReleased(vLog)
	if err != nil {
		errorMsg := fmt.Sprintf("Epoch release parse error for block %d: %s", block.Number().Int64(), err.Error())
		clients.SendFailureNotification(pkgs.ProcessEvents, errorMsg, time.Now().String(), "High")
		log.Error(errorMsg)
		return fmt.Errorf("failed to parse epoch released event: %w", err)
	}

	// Check if the DataMarketAddress in the event matches any address in the DataMarketAddress array
	if isValidDataMarketAddress(releasedEvent.DataMarketAddress.Hex()) {
		// Extract the epoch ID and the data market address from the event
		newEpochID := releasedEvent.EpochId
		dataMarketAddress := releasedEvent.DataMarketAddress.Hex()

		log.Debugf("Detected epoch released event at block %d for data market %s with epochID %s", block.Header().Number, dataMarketAddress, newEpochID.String())

		// Calculate the submission limit block based on the epoch release block number (current block number)
		submissionLimitBlockNumber, err := calculateSubmissionLimitBlock(ctx, dataMarketAddress, new(big.Int).Set(block.Number()))
		if err != nil {
			log.Errorf("Failed to calculate submission limit block number for data market %s, epoch ID %s, block %d: %s", dataMarketAddress, newEpochID.String(), block.Number().Int64(), err.Error())
			return fmt.Errorf("failed to calculate submission limit block: %w", err)
		}

		log.Infof("Calculated Submission Limit Block Number for epochID %s, data market %s: %d", newEpochID.String(), dataMarketAddress, submissionLimitBlockNumber.Int64())

		// Send updateRewards to relayer when current epoch is a multiple of epoch interval (config param)
		if newEpochID.Int64()%config.SettingsObj.RewardsUpdateEpochInterval == 0 {
			// Send periodic updateRewards to relayer throughout the day
			if err := sendRewardUpdates(ctx, dataMarketAddress, newEpochID.String()); err != nil {
				errMsg := fmt.Sprintf("Failed to send reward updates for epoch %s within data market %s: %v", newEpochID.String(), dataMarketAddress, err)
				clients.SendFailureNotification(pkgs.SendUpdateRewardsToRelayer, errMsg, time.Now().String(), "High")
				log.Error(errMsg)
				return fmt.Errorf("failed to send reward updates: %w", err)
			}
		}

		// Prepare the epoch marker details
		epochMarkerDetails := EpochMarkerDetails{
			EpochReleaseBlockNumber:    block.Number().Int64(),
			SubmissionLimitBlockNumber: submissionLimitBlockNumber.Int64(),
		}

		epochMarkerDetailsJSON, err := json.Marshal(epochMarkerDetails)
		if err != nil {
			errorMsg := fmt.Sprintf("Failed to marshal epoch marker details for data market %s, epochID %s: %s", dataMarketAddress, newEpochID.String(), err.Error())
			clients.SendFailureNotification(pkgs.ProcessEvents, errorMsg, time.Now().String(), "High")
			log.Error(errorMsg)
			return fmt.Errorf("failed to marshal epoch marker details: %w", err)
		}

		// Store the details associated with the new epoch in Redis
		if err := redis.StoreEpochDetails(ctx, dataMarketAddress, newEpochID.String(), string(epochMarkerDetailsJSON)); err != nil {
			errorMsg := fmt.Sprintf("Failed to store epoch marker details for epochID %s, data market %s in Redis: %s", newEpochID.String(), dataMarketAddress, err.Error())
			clients.SendFailureNotification(pkgs.ProcessEvents, errorMsg, time.Now().String(), "High")
			log.Error(errorMsg)
			return fmt.Errorf("failed to store epoch marker details: %w", err)
		}

		log.Infof("✅ Successfully stored epoch marker details for epochID %s, data market %s in Redis", newEpochID.String(), dataMarketAddress)
	}

	return nil
}

// handleSnapshotBatchSubmittedEvent processes snapshot batch submitted events
func handleSnapshotBatchSubmittedEvent(ctx context.Context, block *types.Block, vLog types.Log) error {
	log.Debugf("SnapshotBatchSubmitted event detected in block %d", block.Number().Int64())

	// Parse the `SnapshotBatchSubmitted` event from the log
	releasedEvent, err := Instance.ParseSnapshotBatchSubmitted(vLog)
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to parse SnapshotBatchSubmitted event for block %d: %s", block.Number().Int64(), err.Error())
		clients.SendFailureNotification(pkgs.ProcessEvents, errorMsg, time.Now().String(), "High")
		log.Error(errorMsg)
		return fmt.Errorf("failed to parse snapshot batch submitted event: %w", err)
	}

	// Check if the DataMarketAddress in the event matches any address in the DataMarketAddress array
	if isValidDataMarketAddress(releasedEvent.DataMarketAddress.Hex()) {
		// Extract the epoch ID and the data market address from the event
		epochID := releasedEvent.EpochId
		dataMarketAddress := releasedEvent.DataMarketAddress.Hex()

		// Create an instance of batch details
		batch := BatchDetails{
			EpochID:           epochID,
			DataMarketAddress: dataMarketAddress,
			BatchCID:          releasedEvent.BatchCid,
		}

		// Serialize the struct to JSON
		jsonData, err := json.Marshal(batch)
		if err != nil {
			errMsg := fmt.Sprintf("Serialization failed for batch details of epoch %s, data market %s: %v", epochID.String(), dataMarketAddress, err)
			clients.SendFailureNotification(pkgs.ProcessEvents, err.Error(), time.Now().String(), "High")
			log.Error(errMsg)
			return fmt.Errorf("failed to marshal batch details: %w", err)
		}

		if config.SettingsObj.AttestorQueuePushEnabled {
			// Push the serialized data to Redis
			if err = redis.LPush(ctx, "attestorQueue", jsonData).Err(); err != nil {
				errMsg := fmt.Sprintf("Error pushing batch details to attestor queue in Redis for epoch %s, data market %s: %v",
					epochID.String(), dataMarketAddress, err)
				clients.SendFailureNotification(pkgs.ProcessEvents, errMsg, time.Now().String(), "High")
				log.Error(errMsg)
				return fmt.Errorf("failed to push batch details to attestor queue: %w", err)
			}
		}

		log.Infof("✅ Batch details successfully pushed to Redis for epoch %s in data market %s", epochID.String(), dataMarketAddress)
	}

	return nil
}
