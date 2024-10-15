package prost

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"submission-sequencer-collector/config"
	"submission-sequencer-collector/pkgs"
	"submission-sequencer-collector/pkgs/clients"
	"submission-sequencer-collector/pkgs/redis"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	log "github.com/sirupsen/logrus"
)

type EpochMarkerDetails struct {
	EpochReleaseBlockNumber    int64
	SubmissionLimitBlockNumber int64
}

type SubmissionDetails struct {
	EpochID    *big.Int
	ProjectMap map[string][]string // ProjectID -> SubmissionKeys
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
		log.Errorln("Error fetching logs: ", err.Error())
		clients.SendFailureNotification(pkgs.ProcessEvents, fmt.Sprintf("Error fetching logs: %s", err.Error()), time.Now().String(), "High")
		return
	}

	// Process the logs for the current block
	for _, vLog := range logs {
		// Check the event signature and handle the `EpochReleased` event
		switch vLog.Topics[0].Hex() {
		case ContractABI.Events["EpochReleased"].ID.Hex():
			// Parse the `EpochReleased` event from the log
			releasedEvent, err := Instance.ParseEpochReleased(vLog)
			if err != nil {
				clients.SendFailureNotification("Epoch release parse error: ", err.Error(), time.Now().String(), "High")
				log.Errorln("Error unpacking epoch released event: ", err)
				continue
			}

			// Ensure the DataMarketAddress in the event matches the configured DataMarketAddress
			if releasedEvent.DataMarketAddress.Hex() == config.SettingsObj.DataMarketAddress {
				log.Debugf("Epoch Released at block %d: %s\n", block.Header().Number, releasedEvent.EpochId.String())

				// Fetch the current epoch ID from Redis
				// NOTE: why is this important?
				currentEpochID, err := redis.Get(context.Background(), pkgs.CurrentEpoch)
				// NOTE: in the first run, how will this be set?
				// because it will always return a null and hit the condition below
				if err != nil {
					clients.SendFailureNotification(pkgs.ProcessEvents, fmt.Sprintf("Failed to fetch current epoch from Redis: %s", err.Error()), time.Now().String(), "High")
					log.Errorf("Failed to fetch the current epoch from Redis: %s", err.Error())
					continue
				}

				// If the new epoch is greater than the current epoch, update Redis
				if currentEpochID < releasedEvent.EpochId.String() {
					newEpochID := releasedEvent.EpochId

					// Update Redis with the new current epoch ID
					if err := redis.Set(context.Background(), pkgs.CurrentEpoch, newEpochID.String(), 0); err != nil {
						log.Errorf("Failed to update current epoch in Redis: %s", err.Error())
						continue
					}

					// Calculate the submission limit block based on the epoch release block number (current block number)
					submissionLimitBlockNumber, err := calculateSubmissionLimitBlock(new(big.Int).Set(block.Number()))
					if err != nil {
						log.Errorf("Failed to fetch submission limit block number: %s", err.Error())
						continue
					}

					// Prepare the epoch marker details
					epochMarkerDetails := EpochMarkerDetails{
						EpochReleaseBlockNumber:    block.Number().Int64(),
						SubmissionLimitBlockNumber: submissionLimitBlockNumber.Int64(),
					}

					epochMarkerDetailsJSON, err := json.Marshal(epochMarkerDetails)
					if err != nil {
						clients.SendFailureNotification(pkgs.ProcessEvents, fmt.Sprintf("Failed to marshal epoch marker details: %s", err.Error()), time.Now().String(), "High")
						log.Errorf("Failed to marshal epoch marker details: %s", err)
						continue
					}
					// Save the epoch details in Redis using the epoch marker key
					// NOTE: keep a master set of all epoch marker keys created so far
					// this will be used to iterate over available epoch markers
					// once done, remove the epoch marker key from this set
					// ALSO NOTE: create distinct names when referring to keys in different package contexts
					err = redis.Set(context.Background(), redis.EpochMarkerKey(newEpochID.String()), string(epochMarkerDetailsJSON), 0)
					if err != nil {
						log.Errorf("Failed to store epoch marker in Redis: %s", err)
					}
				}
			}
		}
	}
}

func checkAndTriggerBatchPreparation(currentBlock *types.Block) {
	// Get the current block number
	currentBlockNum := currentBlock.Number().Int64()

	// Fetch all epoch marker keys from Redis
	// maintain a set if needed and use that to iterate over available feature keys
	redisKeys, err := redis.RedisClient.Keys(context.Background(), fmt.Sprintf("%s.*", pkgs.EpochMarkerKey)).Result()
	if err != nil {
		log.Errorf("Failed to fetch epoch markers from Redis: %s", err)
		return
	}

	for _, key := range redisKeys {
		// Retrieve the epoch marker details from Redis
		epochMarkerDetailsJSON, err := redis.Get(context.Background(), key)
		if err != nil {
			log.Errorf("Failed to fetch epoch details from Redis for key %s: %s", key, err.Error())
			continue
		}

		var epochMarkerDetails EpochMarkerDetails
		if err := json.Unmarshal([]byte(epochMarkerDetailsJSON), &epochMarkerDetails); err != nil {
			log.Errorf("Failed to unmarshal epoch details for key %s: %s", key, err.Error())
			continue
		}

		// Check if the current block number matches the end block number for this epoch
		if currentBlockNum == epochMarkerDetails.SubmissionLimitBlockNumber {
			log.Infof("Triggering batch preparation for epoch %s", key)

			// Extract epoch ID by trimming the prefix from the Redis key
			epochIDStr := strings.TrimPrefix(key, fmt.Sprintf("%s.", pkgs.EpochMarkerKey))

			// Convert the epoch ID string to big.Int for further processing
			epochID, ok := big.NewInt(0).SetString(epochIDStr, 10)
			if !ok {
				log.Errorf("Failed to convert epochID %s to big.Int", epochIDStr)
				return
			}

			// Trigger batch preparation logic for the current epoch
			go triggerBatchPreparation(epochID, epochMarkerDetails.EpochReleaseBlockNumber, currentBlockNum)

			// Remove epoch marker from Redis after processing
			if err := redis.Delete(context.Background(), key); err != nil {
				log.Errorf("Failed to delete epoch marker from Redis: %s", err.Error())
			}
		}
	}
}

func triggerBatchPreparation(epochID *big.Int, startBlockNum, endBlockNum int64) {
	// Initialize a slice to store block headers (block hashes)
	headers := make([]string, 0)

	// Iterate through the block numbers and fetch the block headers (hashes)
	for blockNum := startBlockNum; blockNum <= endBlockNum; blockNum++ {
		// Generate the Redis key for the current block number
		blockKey := redis.BlockNumber(blockNum)

		// Fetch the block hash from Redis using the generated key
		blockHashValue, err := redis.Get(context.Background(), blockKey)
		if err != nil {
			log.Errorf("Failed to fetch block hash for block number %d: %s", blockNum, err.Error())
			continue // Skip this block and move to the next
		}

		// Convert the block hash from string to common.Hash type
		blockHash := common.HexToHash(blockHashValue)

		// Add the block hash to the headers slice
		headers = append(headers, blockHash.Hex())
	}

	log.Debugf("Collected headers for epoch %s: %v", epochID.String(), headers)

	// Fetch valid submission keys for the epoch
	submissionkeys, err := getValidSubmissionKeys(epochID.Uint64(), headers)
	if err != nil {
		log.Errorln("Failed to fetch submission keys: ", submissionkeys)
	}

	// Construct the project map [ProjectID -> SubmissionKeys]
	projectMap := constructProjectMap(submissionkeys)

	// Create an instance of submission details
	submissionDetails := SubmissionDetails{
		EpochID:    epochID,
		ProjectMap: projectMap,
	}

	// Serialize the struct to JSON
	jsonData, err := json.Marshal(submissionDetails)
	if err != nil {
		log.Fatalf("Error serializing submission details: %s", err)
	}

	// Push the serialized data to Redis
	err = redis.RedisClient.LPush(context.Background(), "batchQueue", jsonData).Err()
	if err != nil {
		log.Fatalf("Error pushing data to Redis: %s", err)
	}
}

func getValidSubmissionKeys(epochID uint64, headers []string) ([]string, error) {
	// Initialize an empty slice to store valid submission keys
	submissionKeys := make([]string, 0)

	// Iterate through the list of headers
	for _, header := range headers {
		keys := redis.RedisClient.SMembers(context.Background(), redis.SubmissionSetByHeaderKey(epochID, header)).Val()
		if len(keys) > 0 {
			submissionKeys = append(submissionKeys, keys...)
		}
	}

	return submissionKeys, nil
}

func constructProjectMap(submissionKeys []string) map[string][]string {
	// Initialize an empty map to store the projectID and the submission keys
	projectMap := make(map[string][]string)

	for _, submissionKey := range submissionKeys {
		parts := strings.Split(submissionKey, ".")
		if len(parts) != 3 {
			log.Errorln("Improper key stored in redis: ", submissionKey)
			clients.SendFailureNotification(pkgs.ConstructProjectMap, fmt.Sprintf("Improper key stored in redis: %s", submissionKey), time.Now().String(), "High")
			continue // skip malformed entries
		}

		projectID := parts[1]
		projectMap[projectID] = append(projectMap[projectID], submissionKey)
	}

	return projectMap
}
