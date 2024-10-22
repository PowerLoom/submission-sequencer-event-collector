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
	"sync"
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
	DataMarketAddress string
	EpochID           *big.Int
	ProjectMap        map[string][]string // ProjectID -> SubmissionKeys
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

			// Check if the DataMarketAddress in the event matches any address in the DataMarketAddress array
			if isValidDataMarketAddress(releasedEvent.DataMarketAddress.Hex()) {
				// Extract the epoch ID and the data market address from the event
				newEpochID := releasedEvent.EpochId
				dataMarketAddress := releasedEvent.DataMarketAddress.Hex()

				log.Debugf("Epoch Released at block %d: %s\n", block.Header().Number, newEpochID.String())

				// Calculate the submission limit block based on the epoch release block number (current block number)
				submissionLimitBlockNumber, err := calculateSubmissionLimitBlock(dataMarketAddress, new(big.Int).Set(block.Number()))
				if err != nil {
					log.Errorf("Failed to fetch submission limit block number: %s", err.Error())
					continue
				}

				log.Debugln("Snapshot Submission Limit Block Number: ", submissionLimitBlockNumber)

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

				// Store the details associated with the new epoch in Redis
				if err := redis.StoreEpochDetails(context.Background(), dataMarketAddress, newEpochID.String(), string(epochMarkerDetailsJSON)); err != nil {
					errorMessage := fmt.Sprintf("Failed to store epoch marker details in Redis for epoch ID %s in data market %s: %s", newEpochID.String(), dataMarketAddress, err.Error())
					clients.SendFailureNotification(pkgs.ProcessEvents, errorMessage, time.Now().String(), "High")
					log.Errorf("Error occurred: %s", errorMessage)
				}
			}
		}
	}
}

func checkAndTriggerBatchPreparation(currentBlock *types.Block) {
	// Get the current block number
	currentBlockNum := currentBlock.Number().Int64()

	var wg sync.WaitGroup

	// Fetch and process epoch markers set concurrently for each data market address
	for _, dataMarketAddress := range config.SettingsObj.DataMarketAddresses {
		wg.Add(1)

		go func(dataMarketAddress string) {
			defer wg.Done()

			// Fetch all the epoch marker keys from Redis for this data market address
			epochMarkerKeys, err := redis.RedisClient.SMembers(context.Background(), redis.EpochMarkerSet(dataMarketAddress)).Result()
			if err != nil {
				log.Errorf("Failed to fetch epoch markers from Redis for data market %s: %s", dataMarketAddress, err)
				return
			}

			// Process each epoch marker key for this data market address
			for _, epochKey := range epochMarkerKeys {
				// Retrieve the epoch marker details from Redis
				epochMarkerDetailsJSON, err := redis.RedisClient.Get(context.Background(), redis.EpochMarkerDetails(dataMarketAddress, epochKey)).Result()
				if err != nil {
					log.Errorf("Failed to fetch epoch marker details from Redis for key %s: %s", epochKey, err)
					continue
				}

				var epochMarkerDetails EpochMarkerDetails
				if err := json.Unmarshal([]byte(epochMarkerDetailsJSON), &epochMarkerDetails); err != nil {
					log.Errorf("Failed to unmarshal epoch marker details for key %s: %s", epochKey, err)
					continue
				}

				// Check if the current block number matches the submission limit block number for this epoch
				if currentBlockNum == epochMarkerDetails.SubmissionLimitBlockNumber {
					log.Infof("Triggering batch preparation for epoch %s in data market %s", epochKey, dataMarketAddress)

					// Convert the epoch ID string to big.Int for further processing
					epochID, ok := big.NewInt(0).SetString(epochKey, 10)
					if !ok {
						log.Errorf("Failed to convert epoch ID %s to big.Int", epochKey)
						continue
					}

					// Trigger batch preparation logic for the current epoch
					go triggerBatchPreparation(dataMarketAddress, epochID, epochMarkerDetails.EpochReleaseBlockNumber, currentBlockNum)
				}
			}
		}(dataMarketAddress) // Pass data market address to avoid closure issues
	}

	// Wait for all data market goroutines to finish
	wg.Wait()
}

func triggerBatchPreparation(dataMarketAddress string, epochID *big.Int, startBlockNum, endBlockNum int64) {
	// Initialize a slice to store block headers (block hashes)
	headers := make([]string, 0)

	// Iterate through the block numbers and fetch the block headers (hashes)
	for blockNum := startBlockNum; blockNum <= endBlockNum; blockNum++ {
		// Generate the Redis key for the current block number
		blockKey := redis.BlockHashByNumber(blockNum)

		// Fetch the block hash from Redis using the generated key
		blockHashValue, err := redis.Get(context.Background(), blockKey)
		if err != nil {
			log.Errorf("Failed to fetch block hash for block %d: %s", blockNum, err.Error())
			continue // Skip this block and move to the next
		}

		// Convert the block hash from string to common.Hash type
		blockHash := common.HexToHash(blockHashValue)

		// Add the block hash to the headers slice
		headers = append(headers, blockHash.Hex())
	}

	log.Debugf("Collected headers for epoch %s in data market %s: %v", epochID.String(), dataMarketAddress, headers)

	// Fetch valid submission keys for the epoch
	submissionkeys, err := getValidSubmissionKeys(context.Background(), epochID.Uint64(), headers, dataMarketAddress)
	if err != nil {
		log.Errorf("Failed to fetch submission keys for epoch %s in data market %s: %s", epochID.String(), dataMarketAddress, err.Error())
	}

	// Construct the project map [ProjectID -> SubmissionKeys]
	projectMap := constructProjectMap(submissionkeys)

	// Create an instance of submission details
	submissionDetails := SubmissionDetails{
		EpochID:           epochID,
		ProjectMap:        projectMap,
		DataMarketAddress: dataMarketAddress,
	}

	// Serialize the struct to JSON
	jsonData, err := json.Marshal(submissionDetails)
	if err != nil {
		log.Fatalf("Failed to serialize submission details for epoch %s in data market %s: %s", epochID.String(), dataMarketAddress, err)
	}

	// Push the serialized data to Redis
	err = redis.RedisClient.LPush(context.Background(), "finalizerQueue", jsonData).Err()
	if err != nil {
		log.Fatalf("Error pushing data to Redis for epoch %s in data market %s: %s", epochID.String(), dataMarketAddress, err)
	}

	// Remove the epochID and its details from Redis after processing
	if err := redis.RemoveEpochFromRedis(context.Background(), dataMarketAddress, epochID.String()); err != nil {
		log.Errorf("Error removing epoch %s from Redis for data market %s: %s", epochID.String(), dataMarketAddress, err)
	}
}

func getValidSubmissionKeys(ctx context.Context, epochID uint64, headers []string, dataMarketAddress string) ([]string, error) {
	// Initialize an empty slice to store valid submission keys
	submissionKeys := make([]string, 0)

	// Iterate through the list of headers
	for _, header := range headers {
		keys := redis.RedisClient.SMembers(ctx, redis.SubmissionSetByHeaderKey(epochID, header, dataMarketAddress)).Val()
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
