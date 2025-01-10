package prost

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"submission-sequencer-collector/config"
	"submission-sequencer-collector/pkgs"
	"submission-sequencer-collector/pkgs/clients"
	"submission-sequencer-collector/pkgs/redis"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	log "github.com/sirupsen/logrus"
)

type EpochMarkerDetails struct {
	EpochReleaseBlockNumber    int64
	SubmissionLimitBlockNumber int64
}

type SubmissionDetails struct {
	DataMarketAddress string
	EpochID           *big.Int
	BatchID           int
	Batch             map[string][]string // ProjectID -> SubmissionKeys
}

type DayTransitionEpochInfo struct {
	LastKnownDay string
	CurrentEpoch int64
	BufferEpoch  int64
}

type BatchDetails struct {
	DataMarketAddress string
	BatchCID          string
	EpochID           *big.Int
}

// ProcessEvents processes logs for the given block and handles the EpochReleased event
func ProcessEvents(block *types.Block) {
	var logs []types.Log
	var err error

	hash := block.Hash()

	// Create a filter query to fetch logs for the block
	filterQuery := ethereum.FilterQuery{
		BlockHash: &hash,
		Addresses: []common.Address{common.HexToAddress(config.SettingsObj.ContractAddress)},
	}

	operation := func() error {
		logs, err = Client.FilterLogs(context.Background(), filterQuery)
		return err
	}

	if err = backoff.Retry(operation, backoff.WithMaxRetries(backoff.NewConstantBackOff(200*time.Millisecond), 3)); err != nil {
		errorMsg := fmt.Sprintf("Error fetching logs for block number %d: %s", block.Number().Int64(), err.Error())
		clients.SendFailureNotification(pkgs.ProcessEvents, errorMsg, time.Now().String(), "High")
		log.Error(errorMsg)
		return
	}

	log.Infof("Processing %d logs for block number %d", len(logs), block.Number().Int64())

	// Process the logs for the current block
	for _, vLog := range logs {
		// Check the event signature and handle the `EpochReleased` event
		switch vLog.Topics[0].Hex() {
		case ContractABI.Events["EpochReleased"].ID.Hex():
			log.Debugf("EpochReleased event detected in block %d", block.Number().Int64())

			// Parse the `EpochReleased` event from the log
			releasedEvent, err := Instance.ParseEpochReleased(vLog)
			if err != nil {
				errorMsg := fmt.Sprintf("Epoch release parse error for block %d: %s", block.Number().Int64(), err.Error())
				clients.SendFailureNotification(pkgs.ProcessEvents, errorMsg, time.Now().String(), "High")
				log.Error(errorMsg)
				continue
			}

			// Check if the DataMarketAddress in the event matches any address in the DataMarketAddress array
			if isValidDataMarketAddress(releasedEvent.DataMarketAddress.Hex()) {
				// Extract the epoch ID and the data market address from the event
				newEpochID := releasedEvent.EpochId
				dataMarketAddress := releasedEvent.DataMarketAddress.Hex()

				log.Debugf("Detected epoch released event at block %d for data market %s with epochID %s", block.Header().Number, dataMarketAddress, newEpochID.String())

				// Calculate the submission limit block based on the epoch release block number (current block number)
				submissionLimitBlockNumber, err := calculateSubmissionLimitBlock(dataMarketAddress, new(big.Int).Set(block.Number()))
				if err != nil {
					log.Errorf("Failed to calculate submission limit block number for data market %s, epoch ID %s, block %d: %s", dataMarketAddress, newEpochID.String(), block.Number().Int64(), err.Error())
					continue
				}

				log.Infof("Calculated Submission Limit Block Number for epochID %s, data market %s: %d", newEpochID.String(), dataMarketAddress, submissionLimitBlockNumber.Int64())

				// Send updateRewards to relayer when current epoch is a multiple of epoch interval (config param)
				if newEpochID.Int64()%config.SettingsObj.RewardsUpdateEpochInterval == 0 {
					// Send periodic updateRewards to relayer throughtout the day
					if err := sendRewardUpdates(dataMarketAddress, newEpochID.String()); err != nil {
						errMsg := fmt.Sprintf("Failed to send reward updates for epoch %s within data market %s: %v", newEpochID.String(), dataMarketAddress, err)
						clients.SendFailureNotification(pkgs.SendUpdateRewardsToRelayer, errMsg, time.Now().String(), "High")
						log.Error(errMsg)
						continue
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
					continue
				}

				// Store the details associated with the new epoch in Redis
				if err := redis.StoreEpochDetails(context.Background(), dataMarketAddress, newEpochID.String(), string(epochMarkerDetailsJSON)); err != nil {
					errorMsg := fmt.Sprintf("Failed to store epoch marker details for epochID %s, data market %s in Redis: %s", newEpochID.String(), dataMarketAddress, err.Error())
					clients.SendFailureNotification(pkgs.ProcessEvents, errorMsg, time.Now().String(), "High")
					log.Error(errorMsg)
				}

				log.Infof("✅ Successfully stored epoch marker details for epochID %s, data market %s in Redis", newEpochID.String(), dataMarketAddress)
			}

		case ContractABI.Events["SnapshotBatchSubmitted"].ID.Hex():
			log.Debugf("SnapshotBatchSubmitted event detected in block %d", block.Number().Int64())

			// Parse the `SnapshotBatchSubmitted` event from the log
			releasedEvent, err := Instance.ParseSnapshotBatchSubmitted(vLog)
			if err != nil {
				errorMsg := fmt.Sprintf("Failed to parse SnapshotBatchSubmitted event for block %d: %s", block.Number().Int64(), err.Error())
				clients.SendFailureNotification(pkgs.ProcessEvents, errorMsg, time.Now().String(), "High")
				log.Error(errorMsg)
				continue
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
					continue
				}

				// Push the serialized data to Redis
				if err = redis.RedisClient.LPush(context.Background(), "attestorQueue", jsonData).Err(); err != nil {
					errMsg := fmt.Sprintf("Error pushing batch details to attestor queue in Redis for epoch %s, data market %s: %v", epochID.String(), dataMarketAddress, err)
					clients.SendFailureNotification(pkgs.ProcessEvents, err.Error(), time.Now().String(), "High")
					log.Error(errMsg)
					continue
				}

				log.Infof("✅ Batch details successfully pushed to Redis for epoch %s in data market %s", epochID.String(), dataMarketAddress)
			}
		}
	}
}

func checkAndTriggerBatchPreparation(currentBlock *types.Block) {
	// Get the current block number
	currentBlockNum := currentBlock.Number().Int64()
	log.Infof("🔍 Starting batch preparation check for block number: %d", currentBlockNum)

	var wg sync.WaitGroup

	// Fetch and process epoch markers set concurrently for each data market address
	for _, dataMarketAddress := range config.SettingsObj.DataMarketAddresses {
		wg.Add(1)

		go func(dataMarketAddress string) {
			defer wg.Done()

			log.Infof("Processing started for data market %s at block number: %d", dataMarketAddress, currentBlockNum)

			// Fetch all the epoch marker keys from Redis for this data market address
			epochMarkerKeys, err := redis.RedisClient.SMembers(context.Background(), redis.EpochMarkerSet(dataMarketAddress)).Result()
			if err != nil {
				errMsg := fmt.Sprintf("Failed to fetch epoch markers from Redis for data market %s: %s", dataMarketAddress, err)
				clients.SendFailureNotification(pkgs.CheckAndTriggerBatchPreparation, errMsg, time.Now().String(), "High")
				log.Error(errMsg)
				return
			}

			log.Infof("Fetched %d epoch marker keys for data market %s: %v", len(epochMarkerKeys), dataMarketAddress, epochMarkerKeys)

			// Process each epoch marker key for this data market address
			for _, epochMarkerKey := range epochMarkerKeys {
				// Retrieve the epoch marker details from Redis
				epochMarkerDetailsJSON, err := redis.RedisClient.Get(context.Background(), redis.EpochMarkerDetails(dataMarketAddress, epochMarkerKey)).Result()
				if err != nil {
					errMsg := fmt.Sprintf("Failed to fetch epoch marker details from Redis for key %s: %s", epochMarkerKey, err)
					clients.SendFailureNotification(pkgs.CheckAndTriggerBatchPreparation, errMsg, time.Now().String(), "High")
					log.Error(errMsg)
					continue
				}

				var epochMarkerDetails EpochMarkerDetails
				if err := json.Unmarshal([]byte(epochMarkerDetailsJSON), &epochMarkerDetails); err != nil {
					errMsg := fmt.Sprintf("Failed to unmarshal epoch marker details for key %s: %s", epochMarkerKey, err)
					clients.SendFailureNotification(pkgs.CheckAndTriggerBatchPreparation, errMsg, time.Now().String(), "High")
					log.Error(errMsg)
					continue
				}

				log.Debugf("📊 Epoch marker details for epoch marker key %s: %+v", epochMarkerKey, epochMarkerDetails)

				// Check if the current block number matches the submission limit block number for this epoch
				if currentBlockNum == epochMarkerDetails.SubmissionLimitBlockNumber {
					log.Infof("🔄 Initiating batch preparation for epoch %s, data market %s at submission limit block number: %d", epochMarkerKey, dataMarketAddress, currentBlockNum)

					// Convert the epoch ID string to big.Int for further processing
					epochID, ok := big.NewInt(0).SetString(epochMarkerKey, 10)
					if !ok {
						log.Errorf("Failed to convert epochID %s to big.Int for data market %s", epochMarkerKey, dataMarketAddress)
						continue
					}

					// Trigger batch preparation logic for the current epoch
					go triggerBatchPreparation(dataMarketAddress, epochID, epochMarkerDetails.EpochReleaseBlockNumber, currentBlockNum)
				}
			}
			log.Infof("Completed processing for data market %s at block number: %d", dataMarketAddress, currentBlockNum)
		}(dataMarketAddress) // Pass data market address to avoid closure issues
	}

	// Wait for all data market goroutines to finish
	wg.Wait()
}

func triggerBatchPreparation(dataMarketAddress string, epochID *big.Int, startBlockNum, endBlockNum int64) {
	// Initialize a slice to store block headers (block hashes)
	headers := make([]string, 0)

	// Calculate the total number of blocks in the range
	blockCount := endBlockNum - startBlockNum + 1
	log.Infof("🚀 Starting batch preparation for epoch %s, data market %s, processing %d blocks from %d to %d", epochID.String(), dataMarketAddress, blockCount, startBlockNum, endBlockNum)

	// Iterate through the block numbers and fetch the block headers (hashes)
	for blockNum := startBlockNum; blockNum <= endBlockNum; blockNum++ {
		// Generate the Redis key for the current block number
		blockKey := redis.BlockHashByNumber(blockNum)

		// Fetch the block hash from Redis using the generated key
		blockHashValue, err := redis.Get(context.Background(), blockKey)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to fetch block hash for block %d: %s", blockNum, err.Error())
			clients.SendFailureNotification(pkgs.TriggerBatchPreparation, errMsg, time.Now().String(), "High")
			log.Error(errMsg)
			continue // Skip this block and move to the next
		}

		// Convert the block hash from string to common.Hash type
		blockHash := common.HexToHash(blockHashValue)

		// Add the block hash to the headers slice
		headers = append(headers, blockHash.Hex())
	}

	log.Infof("📦 Collected %d headers for epoch %s in data market %s", len(headers), epochID.String(), dataMarketAddress)

	// Fetch valid submission keys for the epoch
	submissionKeys, err := getValidSubmissionKeys(context.Background(), epochID.Uint64(), headers, dataMarketAddress)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to fetch submission keys for epoch %s in data market %s: %s", epochID.String(), dataMarketAddress, err.Error())
		clients.SendFailureNotification(pkgs.TriggerBatchPreparation, errMsg, time.Now().String(), "High")
		log.Error(errMsg)
	}

	log.Infof("🔑 Retrieved %d valid submission keys for epoch %s in data market %s", len(submissionKeys), epochID.String(), dataMarketAddress)

	// Update total submission count for the specified data market address
	if err := UpdateSlotSubmissionCount(context.Background(), epochID, dataMarketAddress, submissionKeys); err != nil {
		log.Errorf("Failed to update slot submission counts for epoch %s in data market %s: %s", epochID, dataMarketAddress, err.Error())
	}

	// Construct the project map [ProjectID -> SubmissionKeys]
	projectMap := constructProjectMap(submissionKeys)
	log.Infof("📊 Project map created with %d projects for epoch %s in data market %s", len(projectMap), epochID.String(), dataMarketAddress)

	// Arrange the projectMap into batches of submission keys
	batches := arrangeSubmissionKeysInBatches(projectMap)
	log.Infof("🔄 Arranged %d batches of submission keys for epoch %s in data market %s", len(batches), epochID.String(), dataMarketAddress)

	// Store the batch count for the specified data market address in Redis
	if err := redis.Set(context.Background(), redis.GetBatchCountKey(dataMarketAddress, epochID.String()), strconv.Itoa(len(batches))); err != nil {
		log.Errorf("Failed to set batch count for epoch %s, data market %s in Redis: %s", epochID.String(), dataMarketAddress, err.Error())
	}

	// Send the size of the batches to the external tx relayer service
	if err = SendBatchSizeToRelayer(dataMarketAddress, epochID, len(batches)); err != nil {
		errMsg := fmt.Sprintf("🚨 Failed to send submission batch size for epoch %s in data market %s to relayer: %s", epochID.String(), dataMarketAddress, err.Error())
		clients.SendFailureNotification(pkgs.TriggerBatchPreparation, errMsg, time.Now().String(), "High")
		log.Error(errMsg)
	}

	log.Infof("📨 Batch size %d sent successfully for epoch %s in data market %s", len(batches), epochID.String(), dataMarketAddress)

	// Iterate over all the batches, construct submission details for each batch,
	// serialize them to JSON, and push the serialized data to Redis for further processing
	for i, batch := range batches {
		// Create an instance of submission details
		submissionDetails := SubmissionDetails{
			EpochID:           epochID,
			BatchID:           i + 1,
			Batch:             batch,
			DataMarketAddress: dataMarketAddress,
		}

		// Serialize the struct to JSON
		jsonData, err := json.Marshal(submissionDetails)
		if err != nil {
			errMsg := fmt.Sprintf("Serialization failed for submission details of batch %d, epoch %s in data market %s: %v", i+1, epochID.String(), dataMarketAddress, err)
			clients.SendFailureNotification(pkgs.TriggerBatchPreparation, errMsg, time.Now().String(), "High")
			log.Error(errMsg)
		}

		// Push the serialized data to Redis
		err = redis.RedisClient.LPush(context.Background(), "finalizerQueue", jsonData).Err()
		if err != nil {
			errMsg := fmt.Sprintf("Error pushing submission details of batch %d to Redis for epoch %s in data market %s to finalizer queue in Redis: %v", i+1, epochID.String(), dataMarketAddress, err)
			clients.SendFailureNotification(pkgs.TriggerBatchPreparation, errMsg, time.Now().String(), "High")
			log.Error(errMsg)
		}

		// Serialize the batch details to JSON
		batchJSONData, err := json.Marshal(batch)
		if err != nil {
			errMsg := fmt.Sprintf("Serialization failed for batch details of batch %d, epoch %s in data market %s: %v", i+1, epochID.String(), dataMarketAddress, err)
			clients.SendFailureNotification(pkgs.TriggerBatchPreparation, errMsg, time.Now().String(), "High")
			log.Error(errMsg)
		}

		// Convert the batch ID to a big integer
		batchID := big.NewInt(int64(i + 1))

		// Store the batch details with a key generated from dataMarketAddress, epochID, and batchID
		if err := redis.StoreBatchDetails(context.Background(), dataMarketAddress, epochID.String(), batchID.String(), string(batchJSONData)); err != nil {
			log.Errorf("Failed to store details for batch %d of epoch %s in data market %s: %v", batchID.Int64(), epochID.String(), dataMarketAddress, err)
			continue
		}

		log.Infof("✅ Batch %d successfully pushed to Redis and stored for epoch %s in data market %s", batchID.Int64(), epochID.String(), dataMarketAddress)
	}
}

// Calculate and update total submission count for a data market address
func UpdateSlotSubmissionCount(ctx context.Context, epochID *big.Int, dataMarketAddress string, submissionKeys []string) error {
	// Fetch the current day
	currentDay, err := FetchCurrentDay(common.HexToAddress(dataMarketAddress))
	if err != nil {
		log.Errorf("Failed to fetch current day for data market %s: %v", dataMarketAddress, err)
		return err
	}

	if epochID.Int64()%config.SettingsObj.RewardsUpdateEpochInterval == 0 {
		// Send eligible nodes count to the relayer if the periodic eligible count alerts are set to true
		if config.SettingsObj.PeriodicEligibleCountAlerts {
			// Fetch the slotIDs whose eligible submissions are recorded for the current day
			eligibleNodesByDayKeys := redis.EligibleNodesByDayKey(dataMarketAddress, currentDay.String())
			slotIDs := redis.GetSetKeys(context.Background(), eligibleNodesByDayKeys)

			// Construct the message with eligible node count and slot IDs
			alertMsg := fmt.Sprintf("🔔 Epoch %d: Eligible node count for data market %s on day %s: %d\nSlot IDs: %v", epochID, dataMarketAddress, currentDay.String(), len(slotIDs), slotIDs)

			// Send the alert
			clients.SendFailureNotification(pkgs.SendEligibleNodesCount, alertMsg, time.Now().String(), "High")
			log.Info(alertMsg)
		}

		// Send the updateRewards request to the relayer, including the count of eligible nodes for the specified buffer days period
		for day := 1; day <= int(math.Min(float64(config.SettingsObj.PastDaysBuffer), float64(currentDay.Int64()))); day++ {
			// Calculate the day to check
			dayToCheck := new(big.Int).Sub(currentDay, big.NewInt(int64(day)))

			// Skip processing if day is less than equal to zero
			if dayToCheck.Int64() <= 0 {
				continue
			}

			// Initialize retry attempts
			retryCount := 0
			for retryCount < config.SettingsObj.RetryLimits {
				// Fetch the contract instance for the data market address
				instance := DataMarketInstances[dataMarketAddress]

				// Fetch eligible node count from data market contract
				var count *big.Int
				if output, err := MustQuery(context.Background(), func() (*big.Int, error) {
					return instance.EligibleNodesForDay(&bind.CallOpts{}, dayToCheck)
				}); err == nil {
					count = output
				}

				// If count is non-zero, break the retry loop
				if count != nil && count.Uint64() > 0 {
					log.Infof("✅ Contract Query successful: Eligible node count for data market %s on day %s: %d", dataMarketAddress, dayToCheck.String(), count.Uint64())
					break
				}

				// Calculate the difference between currentDay and dayToCheck
				dayDifference := new(big.Int).Sub(currentDay, dayToCheck)

				// Skip cached count and recalculation when the day has rolled over and epochID is within the buffer range
				epochsInADay, err := redis.GetEpochsInADay(context.Background(), dataMarketAddress)
				if err != nil {
					log.Errorf("Failed to fetch epochs in a day for data market %s: %v", dataMarketAddress, err)
					return err
				}

				if dayDifference.Int64() == 1 && int(epochID.Int64())%int(epochsInADay.Int64()) <= BufferEpochs {
					log.Infof("Skipping cached count and recalculation for data market %s on day %s due to epochID %s being in buffer range", dataMarketAddress, dayToCheck.String(), epochID.String())
					break
				}

				cachedCount, err := redis.GetSetCardinality(context.Background(), redis.EligibleNodesByDayKey(dataMarketAddress, dayToCheck.String()))
				if err != nil {
					errorMsg := fmt.Sprintf("❌ Error fetching cached eligible node count for data market %s on day %s: %v", dataMarketAddress, dayToCheck.String(), err)
					clients.SendFailureNotification(pkgs.SendEligibleNodesCount, errorMsg, time.Now().String(), "Medium")
					log.Error(errorMsg)
					return err
				}

				if cachedCount > 0 {
					log.Infof("Cached eligible node count found for data market %s on day %s: %d", dataMarketAddress, dayToCheck.String(), cachedCount)

					// Attempt to update using cached value
					if err = SendUpdateRewardsToRelayer(dataMarketAddress, []*big.Int{}, []*big.Int{}, dayToCheck.String(), cachedCount); err != nil {
						errorMsg := fmt.Sprintf("🚨 Failed to send rewards update for data market %s on day %s using cached count: %v", dataMarketAddress, dayToCheck.String(), err)
						clients.SendFailureNotification(pkgs.SendUpdateRewardsToRelayer, errorMsg, time.Now().String(), "High")
						log.Error(errorMsg)
						return err
					}

					successMsg := fmt.Sprintf("✅ Successfully updated rewards using cached count: Eligible node count for data market %s on day %s: %d", dataMarketAddress, dayToCheck.String(), cachedCount)
					clients.SendFailureNotification(pkgs.SendEligibleNodesCount, successMsg, time.Now().String(), "High")
					log.Info(successMsg)

					break
				} else {
					// Check if a zero count update has already been sent
					sent, err := redis.GetBooleanValue(context.Background(), redis.ZeroCountUpdateKey(dataMarketAddress, dayToCheck.String()))
					if err != nil {
						log.Errorf("Error checking zero count update status for data market %s on day %s: %v", dataMarketAddress, dayToCheck.String(), err)
						return err
					}
					if sent {
						log.Infof("Skipping zero count update for data market %s on day %s as it has already been sent", dataMarketAddress, dayToCheck.String())
						break
					}

					// Fallback to recalculation if cached value is not found
					eligibleNodes := 0

					// Fetch daily snapshot quota for the specified data market address from Redis
					dailySnapshotQuota, err := redis.GetDailySnapshotQuota(context.Background(), dataMarketAddress)
					if err != nil {
						log.Errorf("❌ Failed to fetch daily snapshot quota for data market %s: %v", dataMarketAddress, err)
						return err
					}

					for slotID := int64(1); slotID <= NodeCount.Int64(); slotID++ {
						var slotSubmissionCount *big.Int
						if output, err := MustQuery(context.Background(), func() (*big.Int, error) {
							return Instance.SlotSubmissionCount(&bind.CallOpts{}, common.HexToAddress(dataMarketAddress), big.NewInt(int64(slotID)), dayToCheck)
						}); err == nil {
							slotSubmissionCount = output
						}

						if slotSubmissionCount != nil && slotSubmissionCount.Uint64() >= dailySnapshotQuota.Uint64() {
							eligibleNodes++
						}
					}

					log.Infof("Recalculated eligible nodes count for data market %s on day %s: %d", dataMarketAddress, dayToCheck.String(), eligibleNodes)

					// Check if recalculated eligible nodes count is zero
					if eligibleNodes == 0 {
						log.Infof("Eligible node count is zero for data market %s on day %s", dataMarketAddress, dayToCheck.String())

						if err := redis.SetBooleanValue(context.Background(), redis.ZeroCountUpdateKey(dataMarketAddress, dayToCheck.String()), true, 24*time.Hour); err != nil {
							log.Errorf("Error marking zero count update as sent for data market %s on day %s: %v", dataMarketAddress, dayToCheck.String(), err)
							return err
						}

						log.Infof("Marked zero count update as sent for data market %s on day %s. Skipping update to relayer.", dataMarketAddress, dayToCheck.String())
						break
					}

					// Attempt to update using recalculated value
					if err = SendUpdateRewardsToRelayer(dataMarketAddress, []*big.Int{}, []*big.Int{}, dayToCheck.String(), eligibleNodes); err != nil {
						errorMsg := fmt.Sprintf("🚨 Failed to send rewards update for data market %s on day %s using recalculated count: %v", dataMarketAddress, dayToCheck.String(), err)
						clients.SendFailureNotification(pkgs.SendUpdateRewardsToRelayer, errorMsg, time.Now().String(), "High")
						log.Error(errorMsg)
						retryCount++
						continue
					}

					successMsg := fmt.Sprintf("✅ Successfully updated rewards using recalculated count: Eligible node count for data market %s on day %s: %d", dataMarketAddress, dayToCheck.String(), eligibleNodes)
					clients.SendFailureNotification(pkgs.SendEligibleNodesCount, successMsg, time.Now().String(), "High")
					log.Info(successMsg)

					break
				}
			}
		}
	}

	// Process day transitions and store corresponding epoch marker details
	if err := handleDayTransition(dataMarketAddress, currentDay, epochID); err != nil {
		log.Errorf("Error handling day transition for data market %s: %v", dataMarketAddress, err)
		return err
	}

	// Verify and trigger updateRewards to relayer when the current epoch matches the buffer epoch for any data market
	go sendFinalRewards(epochID)

	// Fetch day size for the specified data market address from Redis
	daySize, err := redis.GetDaySize(ctx, dataMarketAddress)
	if err != nil {
		log.Errorf("Failed to fetch day size for data market %s: %v", dataMarketAddress, err)
		return err
	}

	// Fetch epochs in a day for the specified data market address from Redis
	epochsInADay, err := redis.GetEpochsInADay(context.Background(), dataMarketAddress)
	if err != nil {
		log.Errorf("Failed to fetch epochs in a day for data market %s: %v", dataMarketAddress, err)
		return err
	}

	// Calculate expiration time
	expirationTime := getExpirationTime(epochID.Int64(), daySize.Int64(), epochsInADay.Int64())

	// Set the current day in Redis with the calculated expiration duration
	if err := redis.SetWithExpiration(context.Background(), redis.GetCurrentDayKey(dataMarketAddress), currentDay.String(), time.Until(expirationTime)); err != nil {
		return fmt.Errorf("failed to cache day value for data market %s in Redis: %v", dataMarketAddress, err)
	}

	// Set the last known day in Redis (for detecting day transition)
	if err := redis.Set(context.Background(), redis.LastKnownDay(dataMarketAddress), currentDay.String()); err != nil {
		return fmt.Errorf("failed to cache last known day value for data market %s in Redis: %v", dataMarketAddress, err)
	}

	// Increment the slot submissions count for a data market in Redis
	for _, submissionKey := range submissionKeys {
		parts := strings.Split(submissionKey, ".")
		slotID := parts[3]

		count, err := redis.Incr(context.Background(), redis.SlotSubmissionKey(dataMarketAddress, slotID, currentDay.String()))
		if err != nil {
			errorMsg := fmt.Sprintf("Failed to increment submission count for slotID %s, epoch %s in data market %s: %s", slotID, epochID, dataMarketAddress, err.Error())
			clients.SendFailureNotification(pkgs.UpdateSlotSubmissionCount, errorMsg, time.Now().String(), "High")
			log.Error(errorMsg)
			return err
		}

		log.Infof("📈 Slot submission count updated successfully for slotID %s, epoch %s in data market %s: %d", slotID, epochID, dataMarketAddress, count)
	}

	return nil
}

func handleDayTransition(dataMarketAddress string, currentDay, epochID *big.Int) error {
	// Fetch the last known day value from Redis
	lastKnownDay, err := redis.Get(context.Background(), redis.LastKnownDay(dataMarketAddress))
	if err != nil {
		log.Errorf("Error fetching last known day value for data market %s from Redis: %v", dataMarketAddress, err)
		return err
	}

	// Check for day transition
	if lastKnownDay != "" && lastKnownDay != currentDay.String() {
		log.Infof("Day transition detected for data market %s: %s -> %s", dataMarketAddress, lastKnownDay, currentDay.String())

		// Prepare the day transition epoch marker details
		dayTransitionEpochDetails := DayTransitionEpochInfo{
			LastKnownDay: lastKnownDay,
			CurrentEpoch: epochID.Int64(),
			BufferEpoch:  epochID.Int64() + int64(BufferEpochs),
		}

		dayTransitionEpochDetailsJSON, err := json.Marshal(dayTransitionEpochDetails)
		if err != nil {
			errorMsg := fmt.Sprintf("Failed to marshal day transtion epoch details for epochID %s, data market %s: %s", epochID.String(), dataMarketAddress, err.Error())
			clients.SendFailureNotification(pkgs.HandleDayTransition, errorMsg, time.Now().String(), "High")
			log.Error(errorMsg)
			return err
		}

		// Store the day transition epoch details in Redis
		if err := redis.StoreDayTransitionEpochDetails(context.Background(), dataMarketAddress, epochID.String(), string(dayTransitionEpochDetailsJSON)); err != nil {
			errorMsg := fmt.Sprintf("Failed to store day transition epoch marker details for epochID %s, data market %s in Redis: %s", epochID.String(), dataMarketAddress, err.Error())
			clients.SendFailureNotification(pkgs.HandleDayTransition, errorMsg, time.Now().String(), "High")
			log.Error(errorMsg)
		}

		log.Infof("✅ Successfully stored day transition epoch marker details for epochID %s, data market %s in Redis", epochID.String(), dataMarketAddress)
	}

	return nil
}

func sendRewardUpdates(dataMarketAddress, epochID string) error {
	// Fetch the current day
	currentDay, err := FetchCurrentDay(common.HexToAddress(dataMarketAddress))
	if err != nil {
		log.Errorf("Failed to fetch current day for data market %s: %v", dataMarketAddress, err)
		return err
	}

	// Prepare data for relayer
	slotIDs := make([]*big.Int, 0)
	submissionsList := make([]*big.Int, 0)

	for slotID := int64(1); slotID <= NodeCount.Int64(); slotID++ {
		// Get the eligible submission count from Redis
		key := redis.EligibleSlotSubmissionKey(dataMarketAddress, big.NewInt(slotID).String(), currentDay.String())
		slotSubmissionCount, err := redis.Get(context.Background(), key)
		if err != nil {
			log.Errorf("Failed to fetch eligible submission count for slotID %d, epoch %s within data market %s: %v", slotID, epochID, dataMarketAddress, err)
			continue
		}

		// Check that submission count value is not empty
		if slotSubmissionCount != "" {
			submissionCountBigInt, ok := new(big.Int).SetString(slotSubmissionCount, 10)
			if !ok {
				log.Errorf("Failed to convert slot submission count %s to big.Int", slotSubmissionCount)
				continue
			}

			// Add the slotID and submission count to the respective arrays
			slotIDs = append(slotIDs, big.NewInt(slotID))
			submissionsList = append(submissionsList, submissionCountBigInt)
		}
	}

	batchArrays(dataMarketAddress, currentDay.String(), slotIDs, submissionsList, 0)

	return nil
}

func sendFinalRewards(currentEpoch *big.Int) {
	log.Infof("🔍 Initiating day transition check for current epoch: %s", currentEpoch.String())

	var wg sync.WaitGroup

	// Fetch and process day transition epoch markers set concurrently for each data market address
	for _, dataMarketAddress := range config.SettingsObj.DataMarketAddresses {
		wg.Add(1)

		go func(dataMarketAddress string) {
			defer wg.Done()

			log.Infof("Processing started for data market %s at epoch number: %d", dataMarketAddress, currentEpoch.Uint64())

			// Fetch all the day transition epoch marker keys from Redis for this data market address
			epochMarkerKeys, err := redis.RedisClient.SMembers(context.Background(), redis.DayRolloverEpochMarkerSet(dataMarketAddress)).Result()
			if err != nil {
				log.Errorf("Failed to fetch day transition epoch markers from Redis for data market %s: %s", dataMarketAddress, err)
				return
			}

			log.Infof("✅ Fetched %d day transition epoch marker keys for data market %s: %v", len(epochMarkerKeys), dataMarketAddress, epochMarkerKeys)

			// Process each day transition epoch marker key for this data market address
			for _, epochMarkerKey := range epochMarkerKeys {
				// Retrieve the day transition epoch marker details from Redis
				epochMarkerDetailsJSON, err := redis.RedisClient.Get(context.Background(), redis.DayRolloverEpochMarkerDetails(dataMarketAddress, epochMarkerKey)).Result()
				if err != nil {
					log.Errorf("Failed to fetch day transition epoch marker details from Redis for key %s: %s", epochMarkerKey, err)
					continue
				}

				var epochMarkerDetails DayTransitionEpochInfo
				if err := json.Unmarshal([]byte(epochMarkerDetailsJSON), &epochMarkerDetails); err != nil {
					log.Errorf("Failed to unmarshal day transition epoch marker details for key %s: %s", epochMarkerKey, err)
					continue
				}

				log.Debugf("📊 Day transition epoch marker details for key %s: %+v", epochMarkerKey, epochMarkerDetails)

				// Check if the current epoch matches the buffer epoch
				if currentEpoch.Int64() == epochMarkerDetails.BufferEpoch {
					lastKnownDay := epochMarkerDetails.LastKnownDay

					// Fetch the eligible nodes count and slotIDs for the last known day (prev day)
					eligibleNodesCount, eligibleSlotIDs := fetchEligibleSlotIDs(dataMarketAddress, lastKnownDay)

					log.Infof("✅ Successfully fetched eligible nodes count for data market %s on day %s: %d", dataMarketAddress, lastKnownDay, eligibleNodesCount)

					// Prepare data for relayer
					slotIDs := make([]*big.Int, 0)
					submissionsList := make([]*big.Int, 0)

					for _, slotID := range eligibleSlotIDs {
						slotIDBigInt, _ := new(big.Int).SetString(slotID, 10)
						lastKnownDayBigInt, _ := new(big.Int).SetString(lastKnownDay, 10)

						var submissionCount *big.Int
						if output, err := MustQuery(context.Background(), func() (*big.Int, error) {
							return Instance.SlotSubmissionCount(&bind.CallOpts{}, common.HexToAddress(dataMarketAddress), slotIDBigInt, lastKnownDayBigInt)
						}); err == nil {
							submissionCount = output
						}

						slotIDs = append(slotIDs, slotIDBigInt)
						submissionsList = append(submissionsList, submissionCount)

					}

					go batchArrays(dataMarketAddress, lastKnownDay, slotIDs, submissionsList, eligibleNodesCount)

					// Remove the epochID and its day transition details from Redis
					epochID := new(big.Int).Sub(currentEpoch, big.NewInt(int64(BufferEpochs)))
					if err := redis.RemoveDayTransitionEpochFromRedis(context.Background(), dataMarketAddress, epochID.String()); err != nil {
						log.Errorf("Error removing day transition epoch %s data from Redis for data market %s: %v", epochID.String(), dataMarketAddress, err)
					}

					log.Infof("🧹 Successfully removed day transition epoch %s data from Redis for data market %s", epochID.String(), dataMarketAddress)

					// Send alert message about eligible nodes count update
					alertMsg := fmt.Sprintf("🔔 Day Transition Update: Eligible nodes count for data market %s has been updated for day %s: %d", dataMarketAddress, lastKnownDay, eligibleNodesCount)
					clients.SendFailureNotification(pkgs.SendEligibleNodesCount, alertMsg, time.Now().String(), "High")
					log.Info(alertMsg)
				}
			}
			log.Infof("Completed processing for data market %s at epoch: %d", dataMarketAddress, currentEpoch.Uint64())
		}(dataMarketAddress) // Pass data market address to avoid closure issues
	}

	// Wait for all data market goroutines to finish
	wg.Wait()
}

func batchArrays(dataMarketAddress, currentDay string, slotIDs, submissionsList []*big.Int, eligibleNodesCount int) {
	// Fetch the batch size from config
	batchSize := config.SettingsObj.RewardsUpdateBatchSize

	var wg sync.WaitGroup
	// Process the data in batches
	for start := 0; start < len(slotIDs); start += batchSize {
		end := start + batchSize
		if end > len(slotIDs) {
			end = len(slotIDs)
		}

		slotIDsBatch := slotIDs[start:end]
		submissionsBatch := submissionsList[start:end]

		wg.Add(1)
		go func(start, end int, slotIDsBatch, submissionsBatch []*big.Int) {
			defer wg.Done()

			// Send the updateRewards request to the relayer
			if err := SendUpdateRewardsToRelayer(dataMarketAddress, slotIDsBatch, submissionsBatch, currentDay, eligibleNodesCount); err != nil {
				errorMsg := fmt.Sprintf("🚨 Relayer batch error in batch %d-%d for data market %s on day %s: %v", start, end, dataMarketAddress, currentDay, err)
				clients.SendFailureNotification(pkgs.SendUpdateRewardsToRelayer, errorMsg, time.Now().String(), "High")
				log.Error(errorMsg)
				return
			}
		}(start, end, slotIDsBatch, submissionsBatch)
	}

	// Wait for all batches to complete
	wg.Wait()

	log.Infof("✅ Successfully sent %d batches to relayer for data market %s on day %s", len(slotIDs)/batchSize, dataMarketAddress, currentDay)
}
