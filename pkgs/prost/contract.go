package prost

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"submission-sequencer-collector/config"
	pkgs "submission-sequencer-collector/pkgs"
	"submission-sequencer-collector/pkgs/clients"
	"submission-sequencer-collector/pkgs/contract"
	"submission-sequencer-collector/pkgs/dataMarketContract"
	"submission-sequencer-collector/pkgs/redis"
	"time"

	"github.com/cenkalti/backoff"
	log "github.com/sirupsen/logrus"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

var (
	Client              *ethclient.Client
	Instance            *contract.Contract
	ContractABI         abi.ABI
	DataMarketInstances = make(map[string]*dataMarketContract.DataMarketContract)
	BufferEpochs        = 5
	operationTimeout    = time.Duration(config.SettingsObj.ContractQueryTimeout) * time.Second
)

func ConfigureClient(ctx context.Context) error {
	rpcClient, err := rpc.DialOptions(ctx, config.SettingsObj.ClientUrl, rpc.WithHTTPClient(&http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}))
	if err != nil {
		log.Errorf("Failed to connect to client: %s", err)
		log.Fatal(err)
	}

	Client = ethclient.NewClient(rpcClient)
	return nil
}

func ConfigureContractInstance(ctx context.Context) error {
	var err error
	Instance, err = contract.NewContract(common.HexToAddress(config.SettingsObj.ContractAddress), Client)
	if err != nil {
		return err
	}

	for _, dataMarketContractAddr := range config.SettingsObj.DataMarketContractAddresses {
		DataMarketInstance, err := dataMarketContract.NewDataMarketContract(dataMarketContractAddr, Client)
		if err != nil {
			return err
		}
		DataMarketInstances[dataMarketContractAddr.Hex()] = DataMarketInstance
	}

	return nil
}

func ConfigureABI() {
	contractABI, err := abi.JSON(strings.NewReader(contract.ContractMetaData.ABI))
	if err != nil {
		log.Errorf("Failed to configure contract ABI: %s", err)
		log.Fatal(err)
	}

	ContractABI = contractABI
}

func MustQuery[T any](parentCtx context.Context, queryFn func() (T, error)) (T, error) {
	var result T
	var err error

	// Simple, independent timeout
	opCtx, opCancel := context.WithTimeout(context.Background(), contractQueryTimeout)
	defer opCancel()

	operation := func() error {
		localResult, err := queryFn()
		if err != nil {
			return err
		}
		result = localResult
		return nil
	}

	// Use our independent context for retries
	err = backoff.Retry(operation, backoff.WithContext(backoff.NewExponentialBackOff(), opCtx))
	if err != nil {
		errMsg := fmt.Sprintf("Contract query error [MustQuery]: %s", err)
		clients.SendFailureNotification(pkgs.MustQuery, errMsg, time.Now().String(), "High")
		log.Error(errMsg)
		return result, fmt.Errorf("contract query error: %w", err)
	}

	return result, nil
}

func LoadContractStateVariables(ctx context.Context) error {
	// Iterate over each data market contract address in the config
	for _, dataMarketAddress := range config.SettingsObj.DataMarketContractAddresses {
		// Fetch snapshot submission limit for the current data market address
		if output, err := MustQuery(ctx, func() (*big.Int, error) {
			return Instance.SnapshotSubmissionWindow(&bind.CallOpts{}, dataMarketAddress)
		}); err == nil {
			// Convert the submission limit to a string for storage in Redis
			submissionLimit := output.String()

			// Store the submission limit in the Redis hash table
			err := redis.RedisClient.HSet(ctx, redis.GetSubmissionLimitTableKey(), dataMarketAddress.Hex(), submissionLimit).Err()
			if err != nil {
				log.Errorf("Failed to set submission limit for data market %s in Redis: %v", dataMarketAddress.Hex(), err)
			}
		}

		// Fetch the day size for the specified data market address from contract
		if output, err := MustQuery(ctx, func() (*big.Int, error) {
			return Instance.DAYSIZE(&bind.CallOpts{}, dataMarketAddress)
		}); err == nil {
			// Convert the day size to a string for storage in Redis
			daySize := output.String()

			// Store the day size in the Redis hash table
			err := redis.RedisClient.HSet(ctx, redis.GetDaySizeTableKey(), dataMarketAddress.Hex(), daySize).Err()
			if err != nil {
				log.Errorf("Failed to set day size for data market %s in Redis: %v", dataMarketAddress.Hex(), err)
			}
		}

		// Fetch the daily snapshot quota for the specified data market address from contract
		if output, err := MustQuery(ctx, func() (*big.Int, error) {
			return Instance.DailySnapshotQuota(&bind.CallOpts{}, dataMarketAddress)
		}); err == nil {
			// Convert the daily snapshot quota to a string for storage in Redis
			dailySnapshotQuota := output.String()

			// Store the daily snapshot quota in the Redis hash table
			err := redis.RedisClient.HSet(ctx, redis.GetDailySnapshotQuotaTableKey(), dataMarketAddress.Hex(), dailySnapshotQuota).Err()
			if err != nil {
				log.Errorf("Failed to set daily snapshot quota for data market %s in Redis: %v", dataMarketAddress.Hex(), err)
			}
		}
	}

	return nil
}

func getExpirationTime(epochID, daySize, epochsInADay int64) time.Time {
	// DAY_SIZE in microseconds
	updatedDaySize := time.Duration(daySize) * time.Microsecond

	// Calculate the duration of each epoch
	epochDuration := updatedDaySize / time.Duration(epochsInADay)

	// Calculate the number of epochs left for the day
	remainingEpochs := epochID % epochsInADay

	// Calculate the expiration duration
	expirationDuration := epochDuration * time.Duration(remainingEpochs)

	// Set a buffer of 10 seconds to expire slightly earlier
	bufferDuration := 10 * time.Second

	// Calculate the expiration time by subtracting the buffer duration
	expirationTime := time.Now().Add(expirationDuration - bufferDuration)

	return expirationTime
}

func FetchCurrentDay(ctx context.Context, dataMarketAddress common.Address) (*big.Int, error) {
	// Fetch the current day for the given data market address from Redis
	value, err := redis.Get(ctx, redis.GetCurrentDayKey(dataMarketAddress.Hex()))
	if err != nil {
		log.Errorf("Error fetching day value for data market %s from Redis: %v", dataMarketAddress.Hex(), err)
		return nil, err
	}

	if value != "" {
		// Cache hit: return the current day value
		currentDay := new(big.Int)
		currentDay.SetString(value, 10)
		return currentDay, nil
	}

	// Cache miss: fetch the current day for the specified data market address from contract
	var currentDay *big.Int
	if output, err := MustQuery(ctx, func() (*big.Int, error) {
		return Instance.DayCounter(&bind.CallOpts{}, dataMarketAddress)
	}); err == nil {
		currentDay = output
	}

	return currentDay, nil
}

// isValidDataMarketAddress checks if the given address is in the DataMarketAddress list
func isValidDataMarketAddress(dataMarketAddress string) bool {
	for _, addr := range config.SettingsObj.DataMarketAddresses {
		if dataMarketAddress == addr {
			return true
		}
	}

	return false
}

// getSubmissionLimitTimeDuration returns the time duration when the submission window ends as configured on contract
func getSubmissionLimitTimeDuration(ctx context.Context, dataMarketAddress string) (time.Duration, error) {
	submissionLimitStr, err := redis.RedisClient.HGet(ctx, redis.GetSubmissionLimitTableKey(), dataMarketAddress).Result()
	if err != nil {
		log.Errorf("Error fetching submission limit for data market %s: %s", dataMarketAddress, err)
		return 0, err
	}

	submissionLimit, ok := new(big.Int).SetString(submissionLimitStr, 10)
	if !ok {
		log.Errorf("Invalid submission limit value for data market %s: %s", dataMarketAddress, submissionLimitStr)
		return 0, err
	}

	return time.Duration(submissionLimit.Int64()) * time.Second, nil
}
