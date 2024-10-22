package prost

import (
	"context"
	"crypto/tls"
	"math/big"
	"net/http"
	"strings"
	"submission-sequencer-collector/config"
	"submission-sequencer-collector/pkgs/clients"
	"submission-sequencer-collector/pkgs/contract"
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
	Client      *ethclient.Client
	Instance    *contract.Contract
	ContractABI abi.ABI
)

func ConfigureClient() {
	rpcClient, err := rpc.DialOptions(context.Background(), config.SettingsObj.ClientUrl, rpc.WithHTTPClient(&http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}))
	if err != nil {
		log.Errorf("Failed to connect to client: %s", err)
		log.Fatal(err)
	}

	Client = ethclient.NewClient(rpcClient)
}

func ConfigureContractInstance() {
	Instance, _ = contract.NewContract(common.HexToAddress(config.SettingsObj.ContractAddress), Client)
}

func ConfigureABI() {
	contractABI, err := abi.JSON(strings.NewReader(contract.ContractMetaData.ABI))
	if err != nil {
		log.Errorf("Failed to configure contract ABI: %s", err)
		log.Fatal(err)
	}

	ContractABI = contractABI
}

func MustQuery[K any](ctx context.Context, call func() (val K, err error)) (K, error) {
	expBackOff := backoff.NewConstantBackOff(1 * time.Second)

	var val K
	operation := func() error {
		var err error
		val, err = call()
		return err
	}
	// Use the retry package to execute the operation with backoff
	err := backoff.Retry(operation, backoff.WithMaxRetries(expBackOff, 3))
	if err != nil {
		clients.SendFailureNotification("Contract query error [MustQuery]", err.Error(), time.Now().String(), "High")
		return *new(K), err
	}
	return val, err
}

func LoadContractStateVariables() {
	// Iterate over each data market contract address in the config
	for _, dataMarketAddress := range config.SettingsObj.DataMarketContractAddresses {
		// Fetch snapshot submission limit for the current data market address
		if output, err := MustQuery[*big.Int](context.Background(), func() (*big.Int, error) {
			return Instance.SnapshotSubmissionWindow(&bind.CallOpts{}, dataMarketAddress)
		}); err == nil {
			// Convert the submission limit to a string for storage in Redis
			submissionLimit := output.String()

			// Store the submission limit in the Redis hash table
			err := redis.RedisClient.HSet(context.Background(), redis.GetSubmissionLimitTableKey(), dataMarketAddress.Hex(), submissionLimit).Err()
			if err != nil {
				log.Errorf("Failed to set submission limit for %s in Redis", dataMarketAddress.Hex())
			}
		}
	}
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

// calculateSubmissionLimitBlock computes the block number when the submission window ends
func calculateSubmissionLimitBlock(dataMarketAddress string, epochReleaseBlock *big.Int) (*big.Int, error) {
	// Fetch the submission limit for the given data market address from Redis
	submissionLimitStr, err := redis.RedisClient.HGet(context.Background(), redis.GetSubmissionLimitTableKey(), dataMarketAddress).Result()
	if err != nil {
		log.Errorf("Error fetching submission limit for data market address %s: %s", dataMarketAddress, err)
		return nil, err
	}

	// Convert the submission limit from string to *big.Int
	submissionLimit, ok := new(big.Int).SetString(submissionLimitStr, 10)
	if !ok {
		log.Errorf("Invalid submission limit value for data market address %s: %s", dataMarketAddress, submissionLimitStr)
		return nil, err
	}

	// Calculate and return the submission limit block number
	return new(big.Int).Add(epochReleaseBlock, submissionLimit), nil
}
