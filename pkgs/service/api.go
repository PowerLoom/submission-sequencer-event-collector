package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"submission-sequencer-collector/config"
	"submission-sequencer-collector/pkgs/prost"
	"submission-sequencer-collector/pkgs/redis"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/gorilla/handlers"
	httpSwagger "github.com/swaggo/http-swagger"
	"google.golang.org/protobuf/encoding/protojson"

	_ "submission-sequencer-collector/pkgs/service/docs"

	"submission-sequencer-collector/pkgs"

	log "github.com/sirupsen/logrus"
)

// @title My API Documentation
// @version 1.0
// @description This API handles submissions and provides Swagger documentation
// @termsOfService http://yourterms.com

// @contact.name API Support
// @contact.url http://www.yoursupport.com
// @contact.email support@example.com

// @host devnet-sequencer-collector.aws2.powerloom.io
// @schemes https
// @BasePath /

type SubmissionsRequest struct {
	Token             string `json:"token"`
	SlotID            int    `json:"slotID"`
	PastDays          int    `json:"pastDays"`
	DataMarketAddress string `json:"dataMarketAddress"`
}

type DailySubmissions struct {
	Day                 int   `json:"day"`
	EligibleSubmissions int64 `json:"eligibleSubmissions"`
	TotalSubmissions    int64 `json:"totalSubmissions"`
}

type EligibleNodesRequest struct {
	Token             string `json:"token"`
	EpochID           int    `json:"epochID"`
	PastDays          int    `json:"pastDays"`
	DataMarketAddress string `json:"dataMarketAddress"`
}

type EpochDataMarketRequest struct {
	Token             string `json:"token"`
	EpochID           int    `json:"epochID"`
	DataMarketAddress string `json:"dataMarketAddress"`
}

type EpochDataMarketDayRequest struct {
	Token             string `json:"token"`
	Day               int    `json:"day"`
	EpochID           int    `json:"epochID"`
	DataMarketAddress string `json:"dataMarketAddress"`
}

type EligibleNodes struct {
	Day     int      `json:"day"`
	Count   int      `json:"eligibleNodesCount"`
	SlotIDs []string `json:"slotIDs"`
}

type BatchCount struct {
	TotalBatches int `json:"totalBatches"`
}

// Swagger-compatible struct for Request
type RequestSwagger struct {
	SlotID      uint64 `json:"slotID,omitempty"`
	Deadline    uint64 `json:"deadline,omitempty"`
	SnapshotCID string `json:"snapshotCID,omitempty"`
	EpochID     uint64 `json:"epochID,omitempty"`
	ProjectID   string `json:"projectID,omitempty"`
}

// Swagger-compatible struct for SnapshotSubmission
type SnapshotSubmissionSwagger struct {
	Request   *RequestSwagger `json:"request,omitempty"`
	Signature string          `json:"signature,omitempty"`
	Header    string          `json:"header,omitempty"`
}

type SubmissionDetails struct {
	SubmissionID   string                     `json:"submissionID"`
	SubmissionData *SnapshotSubmissionSwagger `json:"submissionData"`
}

type EpochSubmissionSummary struct {
	SubmissionCount int                 `json:"epochSubmissionCount"`
	Submissions     []SubmissionDetails `json:"submissions"`
}

type EligibleSubmissionCounts struct {
	SlotID int `json:"slotID"`
	Count  int `json:"count"`
}

type EligibleSubmissionCountsResponse struct {
	SlotCounts []EligibleSubmissionCounts `json:"eligibleSubmissionCounts"`
}

type DiscardedSubmissionDetails struct {
	MostFrequentSnapshotCID  string              `json:"mostFrequentSnapshotCID"`
	DiscardedSubmissionCount int                 `json:"discardedSubmissionCount"`
	DiscardedSubmissions     map[string][]string `json:"discardedSubmissions"`
}

type DiscardedSubmissionDetailsResponse struct {
	ProjectID string                     `json:"projectID"`
	Details   DiscardedSubmissionDetails `json:"details"`
}

type DiscardedSubmissionsAPIResponse struct {
	Projects []DiscardedSubmissionDetailsResponse `json:"projects"`
}

type InfoType[K any] struct {
	Success  bool `json:"success"`
	Response K    `json:"response"`
}

type ResponseArray[K any] []K

type Response[K any] struct {
	Info      InfoType[K] `json:"info"`
	RequestID string      `json:"requestID"`
}

func getDailyTotalSubmission(dataMarketAddress string, slotID int, day *big.Int) int64 {
	// Construct the key for fetching total submissions from Redis
	key := redis.SlotSubmissionKey(dataMarketAddress, strconv.Itoa(slotID), day.String())

	// Attempt to get the value from Redis
	val, err := redis.Get(context.Background(), key)
	if err != nil {
		log.Errorf("Failed to fetch total submissions for key %s: %v", key, err)
		return 0
	}

	// If the value is found, parse it as a big integer
	if val != "" {
		totalSubmissions, ok := new(big.Int).SetString(val, 10)
		if !ok {
			log.Errorf("Failed to parse total submissions for key %s: invalid integer format", key)
			return 0
		}

		return totalSubmissions.Int64()
	}

	// Return 0 if no value is found in Redis
	return 0
}

func getDailyEligibleSubmission(dataMarketAddress string, slotID int, day *big.Int) int64 {
	if val, err := redis.Get(context.Background(), redis.EligibleSlotSubmissionKey(dataMarketAddress, strconv.Itoa(slotID), day.String())); err != nil || val == "" {
		subs, err := prost.MustQuery[*big.Int](context.Background(), func() (*big.Int, error) {
			subs, err := prost.Instance.SlotSubmissionCount(&bind.CallOpts{}, common.HexToAddress(dataMarketAddress), big.NewInt(int64(slotID)), day)
			return subs, err
		})
		if err != nil {
			log.Errorln("Could not fetch eligible submissions from contract: ", err.Error())
			return 0
		}

		return subs.Int64()
	} else {
		eligibleSubmissions, _ := new(big.Int).SetString(val, 10)
		return eligibleSubmissions.Int64()
	}
}

func getEligibleSlotIDs(dataMarketAddress string, day *big.Int) []string {
	// Construct the key for fetching eligible slotIDs from Redis
	key := redis.EligibleNodesByDayKey(dataMarketAddress, day.String())

	// Attempt to get the set values from Redis
	eligibleSlotIDs := redis.GetSetKeys(context.Background(), key)

	// Return the list of slotIDs
	return eligibleSlotIDs
}

func getEpochSubmissions(epochSubmissionKey string) (map[string]string, error) {
	// Use HGetAll to retrieve all key-value pairs in the hash
	submissions, err := redis.RedisClient.HGetAll(context.Background(), epochSubmissionKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch epoch submission details from Redis: %v", err)
	}

	return submissions, nil
}

// handleTotalSubmissions godoc
// @Summary Get eligible and total submissions count
// @Description Retrieves eligible and total submission counts for a specific data market address across a specified number of past days
// @Tags Submissions
// @Accept json
// @Produce json
// @Param request body SubmissionsRequest true "Submissions request payload"
// @Success 200 {object} Response[ResponseArray[DailySubmissions]]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., past days < 1, invalid slotID or invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Router /totalSubmissions [post]
func handleTotalSubmissions(w http.ResponseWriter, r *http.Request) {
	var request SubmissionsRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.Token != config.SettingsObj.AuthReadToken {
		http.Error(w, "Incorrect Token!", http.StatusUnauthorized)
		return
	}

	if request.PastDays < 1 {
		http.Error(w, "Past days should be at least 1", http.StatusBadRequest)
		return
	}

	slotID := request.SlotID
	if slotID < 1 || slotID > 10000 {
		http.Error(w, fmt.Sprintf("Invalid slotID: %d", slotID), http.StatusBadRequest)
		return
	}

	isValid := false
	for _, dataMarketAddress := range config.SettingsObj.DataMarketAddresses {
		if request.DataMarketAddress == dataMarketAddress {
			isValid = true
			break
		}
	}

	if !isValid {
		http.Error(w, "Invalid Data Market Address!", http.StatusBadRequest)
		return
	}

	day, err := prost.FetchCurrentDay(common.HexToAddress(request.DataMarketAddress))
	if err != nil {
		http.Error(w, "Failed to fetch current day", http.StatusBadRequest)
		return
	}

	currentDay := new(big.Int).Set(day)
	submissionsResponse := make([]DailySubmissions, request.PastDays)

	var wg sync.WaitGroup
	ch := make(chan DailySubmissions, request.PastDays)

	for i := 0; i < request.PastDays; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			day := new(big.Int).Sub(currentDay, big.NewInt(int64(i)))
			eligibleSubs := getDailyEligibleSubmission(request.DataMarketAddress, request.SlotID, day)
			totalSubs := getDailyTotalSubmission(request.DataMarketAddress, request.SlotID, day)
			ch <- DailySubmissions{Day: int(day.Int64()), EligibleSubmissions: eligibleSubs, TotalSubmissions: totalSubs}
		}(i)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for submission := range ch {
		submissionsResponse[int(currentDay.Int64())-submission.Day] = submission
	}

	info := InfoType[ResponseArray[DailySubmissions]]{
		Success:  true,
		Response: submissionsResponse,
	}

	response := Response[ResponseArray[DailySubmissions]]{
		Info:      info,
		RequestID: r.Context().Value("request_id").(string),
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

// handleEligibleNodesCount godoc
// @Summary Get eligible nodes count
// @Description Retrieves the total count of eligible nodes along with their corresponding slotIDs for a specified data market address and epochID across a specified number of past days
// @Tags Eligible Nodes Count
// @Accept json
// @Produce json
// @Param request body EligibleNodesRequest true "Eligible nodes count payload"
// @Success 200 {object} Response[ResponseArray[EligibleNodes]]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., past days < 1, missing or invalid epochID, or invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Router /eligibleNodesCount [post]
func handleEligibleNodesCount(w http.ResponseWriter, r *http.Request) {
	var request EligibleNodesRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.Token != config.SettingsObj.AuthReadToken {
		http.Error(w, "Incorrect Token!", http.StatusUnauthorized)
		return
	}

	if request.PastDays < 1 {
		http.Error(w, "Past days should be at least 1", http.StatusBadRequest)
		return
	}

	epochID := request.EpochID
	if epochID <= 0 {
		http.Error(w, "EpochID is missing or invalid", http.StatusBadRequest)
		return
	}

	isValid := false
	for _, dataMarketAddress := range config.SettingsObj.DataMarketAddresses {
		if request.DataMarketAddress == dataMarketAddress {
			isValid = true
			break
		}
	}

	if !isValid {
		http.Error(w, "Invalid Data Market Address!", http.StatusBadRequest)
		return
	}

	day, err := prost.FetchCurrentDay(common.HexToAddress(request.DataMarketAddress))
	if err != nil {
		http.Error(w, "Failed to fetch current day", http.StatusBadRequest)
		return
	}

	currentDay := new(big.Int).Set(day)
	eligibleNodesResponse := make([]EligibleNodes, request.PastDays)

	var wg sync.WaitGroup
	ch := make(chan EligibleNodes, request.PastDays)

	for i := 0; i < request.PastDays; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			day := new(big.Int).Sub(currentDay, big.NewInt(int64(i)))
			slotIDs := getEligibleSlotIDs(request.DataMarketAddress, day) // Fetch eligible slot IDs for the day
			ch <- EligibleNodes{
				Day:     int(day.Int64()),
				Count:   len(slotIDs),
				SlotIDs: slotIDs,
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for eligibleNode := range ch {
		eligibleNodesResponse[currentDay.Int64()-int64(eligibleNode.Day)] = eligibleNode
	}

	info := InfoType[ResponseArray[EligibleNodes]]{
		Success:  true,
		Response: eligibleNodesResponse,
	}

	response := Response[ResponseArray[EligibleNodes]]{
		Info:      info,
		RequestID: r.Context().Value("request_id").(string),
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

// handleBatchCount godoc
// @Summary Get total batch count
// @Description Retrieves the total number of batches created within a specific epoch for a given data market address
// @Tags Batch Count
// @Accept json
// @Produce json
// @Param request body EpochDataMarketRequest true "Epoch data market request payload"
// @Success 200 {object} Response[BatchCount]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., missing or invalid epochID, or invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Router /batchCount [post]
func handleBatchCount(w http.ResponseWriter, r *http.Request) {
	var request EpochDataMarketRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.Token != config.SettingsObj.AuthReadToken {
		http.Error(w, "Incorrect Token!", http.StatusUnauthorized)
		return
	}

	epochID := request.EpochID
	if epochID <= 0 {
		http.Error(w, "EpochID is missing or invalid", http.StatusBadRequest)
		return
	}

	isValid := false
	for _, dataMarketAddress := range config.SettingsObj.DataMarketAddresses {
		if request.DataMarketAddress == dataMarketAddress {
			isValid = true
			break
		}
	}

	if !isValid {
		http.Error(w, "Invalid Data Market Address!", http.StatusBadRequest)
		return
	}

	// Fetch the batch count from Redis
	batchCountKey := redis.GetBatchCountKey(request.DataMarketAddress, strconv.Itoa(request.EpochID))
	batchCountStr, err := redis.Get(context.Background(), batchCountKey)
	if err != nil {
		http.Error(w, "Internal Server Error: Failed to fetch batch count", http.StatusInternalServerError)
		return
	}

	// Convert batch count to integer
	batchCount, err := strconv.Atoi(batchCountStr)
	if err != nil {
		http.Error(w, "Internal Server Error: Invalid batch count format", http.StatusInternalServerError)
		return
	}

	info := InfoType[BatchCount]{
		Success: true,
		Response: BatchCount{
			TotalBatches: batchCount,
		},
	}

	response := Response[BatchCount]{
		Info:      info,
		RequestID: r.Context().Value("request_id").(string),
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

// handleEpochSubmissionDetails godoc
// @Summary Get epoch submission details
// @Description Retrieves the submission count and details of all submissions for a specific epoch and data market address
// @Tags Submissions
// @Accept json
// @Produce json
// @Param request body EpochDataMarketRequest true "Epoch data market request payload"
// @Success 200 {object} Response[EpochSubmissionSummary]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., missing or invalid epochID, or invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Router /epochSubmissionDetails [post]
func handleEpochSubmissionDetails(w http.ResponseWriter, r *http.Request) {
	var request EpochDataMarketRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.Token != config.SettingsObj.AuthReadToken {
		http.Error(w, "Incorrect Token!", http.StatusUnauthorized)
		return
	}

	epochID := request.EpochID
	if epochID <= 0 {
		http.Error(w, "EpochID is missing or invalid", http.StatusBadRequest)
		return
	}

	isValid := false
	for _, dataMarketAddress := range config.SettingsObj.DataMarketAddresses {
		if request.DataMarketAddress == dataMarketAddress {
			isValid = true
			break
		}
	}

	if !isValid {
		http.Error(w, "Invalid Data Market Address!", http.StatusBadRequest)
		return
	}

	// Fetch the epoch submission count from Redis
	submissionCountKey := redis.EpochSubmissionsCount(request.DataMarketAddress, uint64(request.EpochID))
	submissionCountStr, err := redis.Get(context.Background(), submissionCountKey)
	if err != nil {
		http.Error(w, "Internal Server Error: Failed to fetch epoch submission count", http.StatusInternalServerError)
		return
	}

	// Convert submission count to integer
	submissionCount, err := strconv.Atoi(submissionCountStr)
	if err != nil {
		http.Error(w, "Internal Server Error: Invalid epoch submission format", http.StatusInternalServerError)
		return
	}

	// Fetch the epoch submission details from Redis
	epochSubmissionsKey := redis.EpochSubmissionsKey(request.DataMarketAddress, uint64(request.EpochID))
	epochSubmissionDetails, err := getEpochSubmissions(epochSubmissionsKey)
	if err != nil {
		http.Error(w, "Internal Server Error: Failed to fetch epoch submission details", http.StatusInternalServerError)
		return
	}

	submissionDetailsList := make([]SubmissionDetails, 0)
	for submissionID, submissionJSON := range epochSubmissionDetails {
		// Unmarshal the JSON into the SnapshotSubmission struct
		submissionData := pkgs.SnapshotSubmission{}
		err = protojson.Unmarshal([]byte(submissionJSON), &submissionData)
		if err != nil {
			log.Errorf("Failed to unmarshal submission details for ID %s: %v", submissionID, err)
			continue // Skip this submission and move to the next
		}

		// Create a SubmissionDetails object
		details := SubmissionDetails{
			SubmissionID: submissionID,
			SubmissionData: &SnapshotSubmissionSwagger{
				Request: &RequestSwagger{
					SlotID:      submissionData.Request.SlotId,
					Deadline:    submissionData.Request.Deadline,
					EpochID:     submissionData.Request.EpochId,
					SnapshotCID: submissionData.Request.SnapshotCid,
					ProjectID:   submissionData.Request.ProjectId,
				},
				Signature: submissionData.Signature,
				Header:    submissionData.Header,
			},
		}

		// Append the details to the list
		submissionDetailsList = append(submissionDetailsList, details)
	}

	sort.Slice(submissionDetailsList, func(i, j int) bool {
		return submissionDetailsList[i].SubmissionID < submissionDetailsList[j].SubmissionID
	})

	info := InfoType[EpochSubmissionSummary]{
		Success: true,
		Response: EpochSubmissionSummary{
			SubmissionCount: submissionCount,
			Submissions:     submissionDetailsList,
		},
	}

	response := Response[EpochSubmissionSummary]{
		Info:      info,
		RequestID: r.Context().Value("request_id").(string),
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

// handleEligibleSlotSubmissionCount godoc
// @Summary Get the submission counts of all eligible slotIDs
// @Description Retrieves the submission counts of all eligible slotIDs within a specific epoch for a given data market address
// @Tags Eligible Submission Count
// @Accept json
// @Produce json
// @Param request body EpochDataMarketDayRequest true "Epoch data market day request payload"
// @Success 200 {object} Response[EligibleSubmissionCountsResponse]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., missing or invalid epochID, invalid day or invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Router /eligibleSlotSubmissionCount [post]
func handleEligibleSlotSubmissionCount(w http.ResponseWriter, r *http.Request) {
	var request EpochDataMarketDayRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.Token != config.SettingsObj.AuthReadToken {
		http.Error(w, "Incorrect Token!", http.StatusUnauthorized)
		return
	}

	day := request.Day
	if day <= 0 {
		http.Error(w, "Invalid day!", http.StatusBadRequest)
		return
	}

	epochID := request.EpochID
	if epochID <= 0 {
		http.Error(w, "EpochID is missing or invalid", http.StatusBadRequest)
		return
	}

	isValid := false
	for _, dataMarketAddress := range config.SettingsObj.DataMarketAddresses {
		if request.DataMarketAddress == dataMarketAddress {
			isValid = true
			break
		}
	}

	if !isValid {
		http.Error(w, "Invalid Data Market Address!", http.StatusBadRequest)
		return
	}

	// Retrieve all slotIDs and their counts for the given epochID
	eligibleSlotSubmissionByEpochKey := redis.EligibleSlotSubmissionsByEpochKey(request.DataMarketAddress, strconv.Itoa(day), strconv.Itoa(request.EpochID))
	slotCounts, err := redis.RedisClient.HGetAll(context.Background(), eligibleSlotSubmissionByEpochKey).Result()
	if err != nil {
		http.Error(w, fmt.Sprintf("Internal Server Error: Failed to fetch eligible submission counts for epoch %v", epochID), http.StatusInternalServerError)
		return
	}

	eligibleSubmissionCounts := make([]EligibleSubmissionCounts, 0)
	for slotID, count := range slotCounts {
		// Convert slotID from string to int
		slotIDInt, err := strconv.Atoi(slotID)
		if err != nil {
			log.Errorf("Failed to convert slotID %s to int: %v", slotID, err)
			continue // Skip this iteration if conversion fails
		}

		// Convert count from string to int
		countInt, err := strconv.Atoi(count)
		if err != nil {
			log.Errorf("Failed to convert count %s to int: %v", count, err)
			continue // Skip this iteration if conversion fails
		}

		// Create response object
		response := EligibleSubmissionCounts{
			SlotID: slotIDInt,
			Count:  countInt,
		}

		eligibleSubmissionCounts = append(eligibleSubmissionCounts, response)
	}

	info := InfoType[EligibleSubmissionCountsResponse]{
		Success: true,
		Response: EligibleSubmissionCountsResponse{
			SlotCounts: eligibleSubmissionCounts,
		},
	}

	response := Response[EligibleSubmissionCountsResponse]{
		Info:      info,
		RequestID: r.Context().Value("request_id").(string),
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

// handleDiscardedSubmissions godoc
// @Summary Get discarded submission details
// @Description Retrieves the discarded submissions details within a specific epoch for a given data market address
// @Tags Discarded Submissions
// @Accept json
// @Produce json
// @Param request body EpochDataMarketDayRequest true "Epoch data market day request payload"
// @Success 200 {object} Response[DiscardedSubmissionsAPIResponse]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., missing or invalid epochID, invalid day or invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Router /discardedSubmissions [post]
func handleDiscardedSubmissions(w http.ResponseWriter, r *http.Request) {
	var request EpochDataMarketDayRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.Token != config.SettingsObj.AuthReadToken {
		http.Error(w, "Incorrect Token!", http.StatusUnauthorized)
		return
	}

	day := request.Day
	if day <= 0 {
		http.Error(w, "Invalid day!", http.StatusBadRequest)
		return
	}

	epochID := request.EpochID
	if epochID <= 0 {
		http.Error(w, "EpochID is missing or invalid", http.StatusBadRequest)
		return
	}

	isValid := false
	for _, dataMarketAddress := range config.SettingsObj.DataMarketAddresses {
		if request.DataMarketAddress == dataMarketAddress {
			isValid = true
			break
		}
	}

	if !isValid {
		http.Error(w, "Invalid Data Market Address!", http.StatusBadRequest)
		return
	}

	// Construct the Redis key for the discarded submission details
	discardedKey := redis.DiscardedSubmissionsKey(request.DataMarketAddress, strconv.Itoa(day), strconv.Itoa(epochID))

	// Fetch all the project details from the Redis hash
	discardedDetailsMap, err := redis.RedisClient.HGetAll(context.Background(), discardedKey).Result()
	if err != nil {
		http.Error(w, fmt.Sprintf("Internal Server Error: Failed to fetch discarded submission details from Redis for epoch %v", epochID), http.StatusInternalServerError)
		return
	}

	// Prepare the response data
	var responseProjects []DiscardedSubmissionDetailsResponse
	for projectID, detailsJSON := range discardedDetailsMap {
		var details DiscardedSubmissionDetails

		// Deserialize the JSON string
		if err := json.Unmarshal([]byte(detailsJSON), &details); err != nil {
			log.Errorf("Failed to deserialize discarded submission details for project %s: %v", projectID, err)
			continue
		}

		// Append to the response list
		responseProjects = append(responseProjects, DiscardedSubmissionDetailsResponse{
			ProjectID: projectID,
			Details:   details,
		})

		// Break if we have 50 projects
		if len(responseProjects) == 50 {
			break
		}
	}

	// Construct the final API response
	apiResponse := DiscardedSubmissionsAPIResponse{
		Projects: responseProjects,
	}

	info := InfoType[DiscardedSubmissionsAPIResponse]{
		Success:  true,
		Response: apiResponse,
	}

	response := Response[DiscardedSubmissionsAPIResponse]{
		Info:      info,
		RequestID: r.Context().Value("request_id").(string),
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func RequestMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := uuid.New().String()
		ctx := context.WithValue(r.Context(), "request_id", requestID)
		r = r.WithContext(ctx)

		log.WithField("request_id", requestID).Infof("Request started for: %s", r.URL.Path)

		w.Header().Set("X-Request-ID", requestID)

		next.ServeHTTP(w, r)

		log.WithField("request_id", requestID).Infof("Request ended")
	})
}

func StartApiServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/totalSubmissions", handleTotalSubmissions)
	mux.HandleFunc("/eligibleNodesCount", handleEligibleNodesCount)
	mux.HandleFunc("/batchCount", handleBatchCount)
	mux.HandleFunc("/epochSubmissionDetails", handleEpochSubmissionDetails)
	mux.HandleFunc("/eligibleSlotSubmissionCount", handleEligibleSlotSubmissionCount)
	mux.HandleFunc("/discardedSubmissions", handleDiscardedSubmissions)

	// Enable CORS with specific settings
	corsHandler := handlers.CORS(
		handlers.AllowedOrigins([]string{"*"}), // Allow all origins or specify your frontend URL
		handlers.AllowedMethods([]string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}),
		handlers.AllowedHeaders([]string{"Content-Type", "Authorization", "X-Request-ID"}),
	)(mux)

	handler := RequestMiddleware(corsHandler)

	// Serve Swagger UI with the middleware
	swaggerHandler := httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
		httpSwagger.DeepLinking(true),
		httpSwagger.DocExpansion("none"),
		httpSwagger.DomID("swagger-ui"),
	)

	mux.Handle("/swagger/", RequestMiddleware(swaggerHandler))

	log.Println("Server is running on port 8080")
	log.Fatal(http.ListenAndServe(":8080", handler))
}
