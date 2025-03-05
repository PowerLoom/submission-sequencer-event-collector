package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"submission-sequencer-collector/config"
	"submission-sequencer-collector/pkgs/prost"
	"submission-sequencer-collector/pkgs/redis"
	"sync"
	"time"

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

// @title Sequencer API Documentation
// @version 1.0
// @description Offers comprehensive documentation of endpoints for seamless interaction with the sequencer, enabling efficient data retrieval.
// @termsOfService http://yourterms.com

// @contact.name API Support
// @contact.url http://www.yoursupport.com
// @contact.email support@example.com

// @host {{API_Host}}
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

type SlotIDInDataMarketRequest struct {
	Token             string `json:"token"`
	DataMarketAddress string `json:"dataMarketAddress"`
	SlotID            int    `json:"slotID"`
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

type EligibleNodesCountRequest struct {
	Day               int    `json:"day"`
	DataMarketAddress string `json:"dataMarketAddress"`
	Token             string `json:"token"`
}

type EligibleNodesPastDaysRequest struct {
	Token             string `json:"token"`
	PastDays          int    `json:"pastDays"`
	DataMarketAddress string `json:"dataMarketAddress"`
}

type DiscardedSubmissionsByDayRequest struct {
	Day               int    `json:"day"`
	SlotID            int    `json:"slotID"`
	DataMarketAddress string `json:"dataMarketAddress"`
	Token             string `json:"token"`
	Page              int    `json:"page"`
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

type DiscardedSubmissionByProjectDetails struct {
	FinalizedCID               string   `json:"finalizedCID"`
	DiscardedSubmissionCount   int      `json:"discardedSubmissionCount"`
	DiscardedSubmissionDetails []string `json:"discardedSubmissions"`
}

type DiscardedSubmissionByProject map[string]*DiscardedSubmissionByProjectDetails

type DiscardedSubmissionByDayResponse struct {
	ProjectID string                              `json:"projectID"`
	Details   DiscardedSubmissionByProjectDetails `json:"details"`
}

type DiscardedSubmissionDetailsByDayAPIResponse struct {
	SlotID               int                                `json:"slotID"`
	DiscardedSubmissions []DiscardedSubmissionByDayResponse `json:"discardedSubmissions"`
	TotalPages           int                                `json:"totalPages"`
	CurrentPage          int                                `json:"currentPage"`
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

func getDailyTotalSubmission(ctx context.Context, dataMarketAddress string, slotID int, day *big.Int) int64 {
	// Construct the key for fetching total submissions from Redis
	key := redis.SlotSubmissionKey(dataMarketAddress, strconv.Itoa(slotID), day.String())

	// Attempt to get the value from Redis
	val, err := redis.Get(ctx, key)
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

func getDailyEligibleSubmission(ctx context.Context, dataMarketAddress string, slotID int, day *big.Int) int64 {
	if val, err := redis.Get(ctx, redis.EligibleSlotSubmissionKey(dataMarketAddress, strconv.Itoa(slotID), day.String())); err != nil || val == "" {
		subs, err := prost.MustQuery[*big.Int](ctx, func() (*big.Int, error) {
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

func getEligibleSlotIDs(ctx context.Context, dataMarketAddress string, day *big.Int) []string {
	// Construct the key for fetching eligible slotIDs from Redis
	key := redis.EligibleNodesByDayKey(dataMarketAddress, day.String())

	// Attempt to get the set values from Redis
	eligibleSlotIDs := redis.GetSetKeys(ctx, key)

	// Return the list of slotIDs
	return eligibleSlotIDs
}

func getEpochSubmissions(ctx context.Context, epochSubmissionKey string) (map[string]string, error) {
	// Use HGetAll to retrieve all key-value pairs in the hash
	submissions, err := redis.RedisClient.HGetAll(ctx, epochSubmissionKey).Result()
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
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., past days < 1 or past days > current day, invalid slotID or invalid data market address)"
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

	day, err := prost.FetchCurrentDay(r.Context(), common.HexToAddress(request.DataMarketAddress))
	if err != nil {
		http.Error(w, "Failed to fetch current day", http.StatusBadRequest)
		return
	}

	currentDay := new(big.Int).Set(day)

	if request.PastDays > int(currentDay.Int64()) {
		http.Error(w, fmt.Sprintf("Past days cannot exceed the current day (%d)", currentDay.Int64()), http.StatusBadRequest)
		return
	}

	submissionsResponse := make([]DailySubmissions, request.PastDays)

	var wg sync.WaitGroup
	ch := make(chan DailySubmissions, request.PastDays)

	for i := 0; i < request.PastDays; i++ {
		wg.Add(1)
		go func(i int, ctx context.Context) {
			defer wg.Done()
			day := new(big.Int).Sub(currentDay, big.NewInt(int64(i)))
			eligibleSubs := getDailyEligibleSubmission(ctx, request.DataMarketAddress, request.SlotID, day)
			totalSubs := getDailyTotalSubmission(ctx, request.DataMarketAddress, request.SlotID, day)
			ch <- DailySubmissions{Day: int(day.Int64()), EligibleSubmissions: eligibleSubs, TotalSubmissions: totalSubs}
		}(i, r.Context())
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

// handleEligibleNodesCountPastDays godoc
// @Summary Get eligible nodes count for past days
// @Description Retrieves the total count of eligible nodes along with their corresponding slotIDs for a specified data market address across a specified number of past days
// @Tags Eligible Nodes
// @Accept json
// @Produce json
// @Param includeSlotDetails query bool false "Set to true to include slotIDs in the response"
// @Param request body EligibleNodesPastDaysRequest true "Eligible nodes count past days payload"
// @Success 200 {object} Response[ResponseArray[EligibleNodes]]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., past days < 1 or past days > current day, invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Router /eligibleNodesCountPastDays [post]
func handleEligibleNodesCountPastDays(w http.ResponseWriter, r *http.Request) {
	var request EligibleNodesPastDaysRequest
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

	day, err := prost.FetchCurrentDay(r.Context(), common.HexToAddress(request.DataMarketAddress))
	if err != nil {
		http.Error(w, "Failed to fetch current day", http.StatusBadRequest)
		return
	}

	currentDay := new(big.Int).Set(day)

	// Validate past days does not exceed the current day
	if request.PastDays > int(currentDay.Int64()) {
		http.Error(w, fmt.Sprintf("Past days cannot exceed the current day (%d)", currentDay.Int64()), http.StatusBadRequest)
		return
	}

	eligibleNodesResponse := make([]EligibleNodes, request.PastDays)

	// Get the includeSlotDetails query parameter value
	queryParams := r.URL.Query()
	includeSlotDetails := queryParams.Get("includeSlotDetails") == "true"

	var wg sync.WaitGroup
	ch := make(chan EligibleNodes, request.PastDays)

	for i := 0; i < request.PastDays; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			day := new(big.Int).Sub(currentDay, big.NewInt(int64(i)))
			slotIDs := getEligibleSlotIDs(r.Context(), request.DataMarketAddress, day) // Fetch eligible slot IDs for the day

			// Construct EligibleNodes object
			eligibleNode := EligibleNodes{
				Day:   int(day.Int64()),
				Count: len(slotIDs),
			}

			// Include slotIDs if query param is set
			if includeSlotDetails {
				eligibleNode.SlotIDs = slotIDs
			} else {
				eligibleNode.SlotIDs = []string{} // Set to an empty array if includeSlotDetails is false
			}

			ch <- eligibleNode
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

// handleEligibleNodesCount godoc
// @Summary Get eligible nodes count for a specific day
// @Description Retrieves the total count of eligible nodes and optionally their corresponding slotIDs (controlled by the includeSlotDetails query param) for a specified data market address and day
// @Tags Eligible Nodes
// @Accept json
// @Produce json
// @Param includeSlotDetails query bool false "Set to true to include slotIDs in the response"
// @Param request body EligibleNodesCountRequest true "Eligible nodes count payload"
// @Success 200 {object} Response[EligibleNodes]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., day < 1 or day > current day, invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Router /eligibleNodesCount [post]
func handleEligibleNodesCount(w http.ResponseWriter, r *http.Request) {
	var request EligibleNodesCountRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.Token != config.SettingsObj.AuthReadToken {
		http.Error(w, "Incorrect Token!", http.StatusUnauthorized)
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

	day, err := prost.FetchCurrentDay(r.Context(), common.HexToAddress(request.DataMarketAddress))
	if err != nil {
		http.Error(w, "Failed to fetch current day", http.StatusBadRequest)
		return
	}

	if request.Day < 1 {
		http.Error(w, "Day must be greater than or equal to 1", http.StatusBadRequest)
		return
	}

	if int64(request.Day) > day.Int64() {
		http.Error(w, fmt.Sprintf("Day must not exceed the current day (%d)", day.Int64()), http.StatusBadRequest)
		return
	}

	// Fetch eligible slot IDs for the specified day
	slotIDs := getEligibleSlotIDs(r.Context(), request.DataMarketAddress, new(big.Int).SetInt64(int64(request.Day)))

	// Construct the response object
	eligibleNodesResponse := EligibleNodes{
		Day:   request.Day,
		Count: len(slotIDs),
	}

	// Optionally retrieve slot IDs if query param is set
	queryParams := r.URL.Query()
	includeSlotDetails := queryParams.Get("includeSlotDetails") == "true"
	if includeSlotDetails {
		eligibleNodesResponse.SlotIDs = slotIDs
	} else {
		eligibleNodesResponse.SlotIDs = []string{} // Set to an empty array if includeSlotDetails is false
	}

	info := InfoType[EligibleNodes]{
		Success:  true,
		Response: eligibleNodesResponse,
	}

	response := Response[EligibleNodes]{
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
	batchCountStr, err := redis.Get(r.Context(), batchCountKey)
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
	submissionCountStr, err := redis.Get(r.Context(), submissionCountKey)
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
	epochSubmissionDetails, err := getEpochSubmissions(r.Context(), epochSubmissionsKey)
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
	slotCounts, err := redis.RedisClient.HGetAll(r.Context(), eligibleSlotSubmissionByEpochKey).Result()
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

// handleDiscardedSubmissionsByEpoch godoc
// @Summary Get discarded submission details by epoch
// @Description Retrieves the discarded submissions details within a specific epoch for a given data market address
// @Tags Discarded Submissions
// @Accept json
// @Produce json
// @Param request body EpochDataMarketDayRequest true "Epoch data market day request payload"
// @Success 200 {object} Response[DiscardedSubmissionsAPIResponse]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., missing or invalid epochID, invalid day or invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Router /discardedSubmissionsByEpoch [post]
func handleDiscardedSubmissionsByEpoch(w http.ResponseWriter, r *http.Request) {
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

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	discardedDetailsMap, err := redis.RedisClient.HGetAll(ctx, discardedKey).Result()
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

// handleDiscardedSubmissionsByDay godoc
// @Summary Get discarded submission details by day
// @Description Retrieves the details of discarded submissions for a specified day and slotID associated with a given data market address
// @Tags Discarded Submissions
// @Accept json
// @Produce json
// @Param request body DiscardedSubmissionsByDayRequest true "Data market slotID day request payload"
// @Success 200 {object} Response[DiscardedSubmissionDetailsByDayAPIResponse]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., invalid slotID, invalid day or invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Router /discardedSubmissionsByDay [post]
func handleDiscardedSubmissionsByDay(w http.ResponseWriter, r *http.Request) {
	var request DiscardedSubmissionsByDayRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.Token != config.SettingsObj.AuthReadToken {
		http.Error(w, "Incorrect Token!", http.StatusUnauthorized)
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

	day, err := prost.FetchCurrentDay(r.Context(), common.HexToAddress(request.DataMarketAddress))
	if err != nil {
		http.Error(w, "Failed to fetch current day", http.StatusBadRequest)
		return
	}

	if request.Day < 1 {
		http.Error(w, "Day must be greater than or equal to 1", http.StatusBadRequest)
		return
	}

	if int64(request.Day) > day.Int64() {
		http.Error(w, fmt.Sprintf("Day must not exceed the current day (%d)", day.Int64()), http.StatusBadRequest)
		return
	}

	slotID := request.SlotID
	if slotID < 1 || slotID > 10000 {
		http.Error(w, fmt.Sprintf("Invalid slotID: %d", slotID), http.StatusBadRequest)
		return
	}

	// Construct the Redis key for the discarded submission details
	discardedKey := redis.DiscardedSubmissionsByDayKey(request.DataMarketAddress, strconv.Itoa(request.Day))

	// Fetch all entries from the hashtable
	results, err := redis.RedisClient.HGetAll(r.Context(), discardedKey).Result()
	if err != nil {
		log.Errorf("Failed to fetch discarded submission details from Redis for day %s: %v", strconv.Itoa(request.Day), http.StatusInternalServerError)
		return
	}

	if len(results) == 0 {
		info := InfoType[DiscardedSubmissionDetailsByDayAPIResponse]{
			Success: true,
			Response: DiscardedSubmissionDetailsByDayAPIResponse{
				SlotID:               request.SlotID,
				DiscardedSubmissions: []DiscardedSubmissionByDayResponse{},
				TotalPages:           0,
				CurrentPage:          request.Page,
			},
		}

		response := Response[DiscardedSubmissionDetailsByDayAPIResponse]{
			Info:      info,
			RequestID: r.Context().Value("request_id").(string),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)

		return
	}

	// Get the slotID map
	slotIDMap, exists := results[strconv.Itoa(request.SlotID)]
	if !exists || slotIDMap == "" {
		http.Error(w, fmt.Sprintf("No discarded submissions found for slotID %d", request.SlotID), http.StatusNotFound)
		return
	}

	// Deserialize the JSON string into DiscardedSubmissionByProject
	var details DiscardedSubmissionByProject
	if err := json.Unmarshal([]byte(slotIDMap), &details); err != nil {
		log.Errorf("Failed to deserialize discarded submission details for slot %d: %v", slotID, err)
		return
	}

	// Prepare the response data
	var responseProjects []DiscardedSubmissionByDayResponse
	for projectID, projectDetails := range details {
		// Append to the response list
		responseProjects = append(responseProjects, DiscardedSubmissionByDayResponse{
			ProjectID: projectID,
			Details:   *projectDetails,
		})
	}

	// Pagination logic
	page := request.Page
	if page < 1 {
		page = 1
	}

	pageSize := 50 // Fixed page size
	totalItems := len(responseProjects)
	totalPages := (totalItems + pageSize - 1) / pageSize
	start := (page - 1) * pageSize
	end := start + pageSize
	if start > totalItems {
		start, end = 0, 0 // No items for this page
	} else if end > totalItems {
		end = totalItems
	}

	paginatedProjects := responseProjects[start:end]

	// Construct the final API response
	apiResponse := DiscardedSubmissionDetailsByDayAPIResponse{
		SlotID:               request.SlotID,
		DiscardedSubmissions: paginatedProjects,
		TotalPages:           totalPages,
		CurrentPage:          page,
	}

	info := InfoType[DiscardedSubmissionDetailsByDayAPIResponse]{
		Success:  true,
		Response: apiResponse,
	}

	response := Response[DiscardedSubmissionDetailsByDayAPIResponse]{
		Info:      info,
		RequestID: r.Context().Value("request_id").(string),
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

// handleLastSimulatedSubmission godoc
// @Summary Get the last time a simulation submission was received
// @Description Retrieves the last time a simulation submission was received for a given data market address and slotID
// @Tags Submissions
// @Accept json
// @Produce json
// @Param request body SlotIDInDataMarketRequest true "Data market address and slotID request payload"
// @Success 200 {object} Response[string]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., invalid slotID or invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Failure 500 {string} string "Internal Server Error: Failed to fetch last simulated submission"
// @Router /lastSimulatedSubmission [post]
func handleLastSimulatedSubmission(w http.ResponseWriter, r *http.Request) {
	var request SlotIDInDataMarketRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.Token != config.SettingsObj.AuthReadToken {
		http.Error(w, "Incorrect Token!", http.StatusUnauthorized)
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

	slotID := request.SlotID
	if slotID < 1 || slotID > 10000 {
		http.Error(w, fmt.Sprintf("Invalid slotID: %d", slotID), http.StatusBadRequest)
		return
	}

	// Fetch the last simulated submission from Redis
	lastSimulatedSubmissionKey := redis.LastSimulatedSubmission(request.DataMarketAddress, uint64(request.SlotID))
	lastSimulatedSubmission, err := redis.Get(r.Context(), lastSimulatedSubmissionKey)
	if err != nil || lastSimulatedSubmission == "" {
		http.Error(w, "Failed to fetch last simulated submission", http.StatusInternalServerError)
		return
	}

	// Convert the timestamp to a human-readable format
	timestamp, err := strconv.ParseInt(lastSimulatedSubmission, 10, 64)
	if err != nil {
		http.Error(w, "Failed to parse timestamp", http.StatusInternalServerError)
		return
	}
	lastSimulatedSubmissionTime := time.Unix(timestamp, 0).Format(time.RFC3339)

	info := InfoType[string]{
		Success:  true,
		Response: lastSimulatedSubmissionTime,
	}

	response := Response[string]{
		Info:      info,
		RequestID: r.Context().Value("request_id").(string),
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

// handleLastSnapshotSubmission godoc
// @Summary Get the last time a snapshot submission against a released epoch was received
// @Description Retrieves the last time a snapshot submission against a released epoch was received for a given data market address and slotID
// @Tags Submissions
// @Accept json
// @Produce json
// @Param request body SlotIDInDataMarketRequest true "Data market address and slotID request payload"
// @Success 200 {object} Response[string]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., invalid slotID or invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Failure 500 {string} string "Internal Server Error: Failed to fetch last snapshot submission"
// @Router /lastSnapshotSubmission [post]
func handleLastSnapshotSubmission(w http.ResponseWriter, r *http.Request) {
	var request SlotIDInDataMarketRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.Token != config.SettingsObj.AuthReadToken {
		http.Error(w, "Incorrect Token!", http.StatusUnauthorized)
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

	slotID := request.SlotID
	if slotID < 1 || slotID > 10000 {
		http.Error(w, fmt.Sprintf("Invalid slotID: %d", slotID), http.StatusBadRequest)
		return
	}

	// Fetch the last snapshot submission from Redis
	lastSnapshotSubmissionKey := redis.LastSnapshotSubmission(request.DataMarketAddress, uint64(request.SlotID))
	lastSnapshotSubmission, err := redis.Get(r.Context(), lastSnapshotSubmissionKey)
	if err != nil || lastSnapshotSubmission == "" {
		http.Error(w, "Failed to fetch last snapshot submission", http.StatusInternalServerError)
		return
	}

	// Convert the timestamp to a human-readable format
	timestamp, err := strconv.ParseInt(lastSnapshotSubmission, 10, 64)
	if err != nil {
		http.Error(w, "Failed to parse timestamp", http.StatusInternalServerError)
		return
	}
	lastSnapshotSubmissionTime := time.Unix(timestamp, 0).Format(time.RFC3339)

	info := InfoType[string]{
		Success:  true,
		Response: lastSnapshotSubmissionTime,
	}

	response := Response[string]{
		Info:      info,
		RequestID: r.Context().Value("request_id").(string),
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

// handleActiveNodesCountByEpoch godoc
// @Summary Get active nodes count for an epoch
// @Description Retrieves the count of active nodes that submitted snapshots for a specific epoch in a given data market
// @Tags Active Nodes
// @Accept json
// @Produce json
// @Param request body EpochDataMarketRequest true "Epoch data market request payload"
// @Success 200 {object} Response[int64]
// @Failure 400 {string} string "Bad Request: Invalid input parameters (e.g., missing or invalid epochID, or invalid data market address)"
// @Failure 401 {string} string "Unauthorized: Incorrect token"
// @Failure 500 {string} string "Internal Server Error: Failed to fetch epoch active nodes count"
// @Router /activeNodesCountByEpoch [post]
func handleActiveNodesCountByEpoch(w http.ResponseWriter, r *http.Request) {
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

	// Fetch the active nodes count from Redis
	activeNodesCountKey := redis.ActiveSnapshottersForEpoch(request.DataMarketAddress, uint64(request.EpochID))
	activeNodesCountInt, err := redis.RedisClient.SCard(r.Context(), activeNodesCountKey).Result()
	if err != nil {
		http.Error(w, "Internal Server Error: Failed to fetch epoch active nodes count", http.StatusInternalServerError)
		return
	}

	info := InfoType[int64]{
		Success:  true,
		Response: activeNodesCountInt,
	}

	response := Response[int64]{
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
	// Get the API host from the environment variable
	host := config.SettingsObj.APIHost

	// Read the generated Swagger JSON file
	swaggerFile, err := os.ReadFile("../pkgs/service/docs/swagger.json")
	if err != nil {
		log.Fatalf("Error reading swagger.json file: %v", err)
	}

	// Replace the placeholder {{API_HOST}} with the actual host value
	swaggerData := string(swaggerFile)
	swaggerData = replaceHost(swaggerData, host)

	// Serve the modified Swagger UI
	mux := http.NewServeMux()
	mux.HandleFunc("/swagger/doc.json", func(w http.ResponseWriter, r *http.Request) {
		// Serve the modified swagger.json file
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(swaggerData))
	})

	// Define your route handlers
	mux.HandleFunc("/totalSubmissions", handleTotalSubmissions)
	mux.HandleFunc("/eligibleNodesCount", handleEligibleNodesCount)
	mux.HandleFunc("/eligibleNodesCountPastDays", handleEligibleNodesCountPastDays)
	mux.HandleFunc("/batchCount", handleBatchCount)
	mux.HandleFunc("/epochSubmissionDetails", handleEpochSubmissionDetails)
	mux.HandleFunc("/eligibleSlotSubmissionCount", handleEligibleSlotSubmissionCount)
	mux.HandleFunc("/discardedSubmissionsByEpoch", handleDiscardedSubmissionsByEpoch)
	mux.HandleFunc("/discardedSubmissionsByDay", handleDiscardedSubmissionsByDay)
	mux.HandleFunc("/lastSimulatedSubmission", handleLastSimulatedSubmission)
	mux.HandleFunc("/lastSnapshotSubmission", handleLastSnapshotSubmission)
	mux.HandleFunc("/activeNodesCountByEpoch", handleActiveNodesCountByEpoch)

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

	log.Println("Server is running on " + host)
	log.Fatal(http.ListenAndServe(":8080", handler))
}

// Replace the placeholder {{API_HOST}} in the Swagger JSON with the actual host
func replaceHost(swaggerData, host string) string {
	return strings.ReplaceAll(swaggerData, "{{API_Host}}", host)
}
