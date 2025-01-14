package main

import (
	"submission-sequencer-collector/config"
	"submission-sequencer-collector/pkgs/clients"
	"submission-sequencer-collector/pkgs/prost"
	"submission-sequencer-collector/pkgs/redis"
	"submission-sequencer-collector/pkgs/service"
	"submission-sequencer-collector/pkgs/utils"
	"sync"
	"time"
)

func main() {
	// Initiate logger
	utils.InitLogger()

	// Load the config object
	config.LoadConfig()

	// Initialize reporting service
	clients.InitializeReportingClient(config.SettingsObj.SlackReportingUrl, 5*time.Second)

	// Initialize tx relayer service
	clients.InitializeTxClient(config.SettingsObj.TxRelayerUrl, time.Duration(config.SettingsObj.HttpTimeout)*time.Second)

	// Setup redis
	redis.RedisClient = redis.NewRedisClient()

	// Set up the RPC client, contract, and ABI instance
	prost.ConfigureClient()
	prost.ConfigureContractInstance()
	prost.ConfigureABI()

	// Load the state variables from the protocol state contract
	prost.LoadContractStateVariables()

	var wg sync.WaitGroup

	wg.Add(1)
	go service.StartApiServer()    // Start API Server
	go prost.StartFetchingBlocks() // Start detecting blocks
	wg.Wait()
}
