//nolint:errcheck
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"

	"submission-sequencer-collector/config"
	"submission-sequencer-collector/pkgs"
	"submission-sequencer-collector/pkgs/prost"
	"submission-sequencer-collector/pkgs/redis"
	"submission-sequencer-collector/pkgs/utils"

	"github.com/alicebob/miniredis"
	"github.com/stretchr/testify/assert"
)

var mr *miniredis.Miniredis

func TestMain(m *testing.M) {
	var err error
	mr, err = miniredis.Run()
	if err != nil {
		log.Fatalf("could not start miniredis: %v", err)
	}

	// Initialize the config settings
	config.SettingsObj = &config.Settings{
		ContractAddress:     "0xE88E5f64AEB483d7057645326AdDFA24A3B312DF",
		ClientUrl:           "https://rpc-prost1m.powerloom.io",
		DataMarketAddresses: []string{"0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"},
		RedisHost:           mr.Host(),
		RedisPort:           mr.Port(),
		RedisDB:             "0",
	}

	utils.InitLogger()
	redis.RedisClient = redis.NewRedisClient()

	prost.ConfigureClient()
	prost.ConfigureContractInstance()

	m.Run()

	mr.Close()
}

func TestHandleTotalSubmissions(t *testing.T) {
	// Set the authentication read token
	config.SettingsObj.AuthReadToken = "valid-token"

	// Set the current day
	redis.Set(context.Background(), redis.GetCurrentDayKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"), "5")

	// Set eligible submission count for each day
	redis.Set(context.Background(), redis.EligibleSlotSubmissionKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "1", "5"), "80")
	redis.Set(context.Background(), redis.EligibleSlotSubmissionKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "1", "4"), "60")
	redis.Set(context.Background(), redis.EligibleSlotSubmissionKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "1", "3"), "140")
	redis.Set(context.Background(), redis.EligibleSlotSubmissionKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "1", "2"), "50")
	redis.Set(context.Background(), redis.EligibleSlotSubmissionKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "1", "1"), "30")

	// Set total submission count for each day
	redis.Set(context.Background(), redis.SlotSubmissionKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "1", "5"), "120")
	redis.Set(context.Background(), redis.SlotSubmissionKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "1", "4"), "200")
	redis.Set(context.Background(), redis.SlotSubmissionKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "1", "3"), "150")
	redis.Set(context.Background(), redis.SlotSubmissionKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "1", "2"), "100")
	redis.Set(context.Background(), redis.SlotSubmissionKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "1", "1"), "60")

	tests := []struct {
		name       string
		body       string
		statusCode int
		response   []DailySubmissions
	}{
		{
			name:       "Valid token, past days 1",
			body:       `{"slotID": 1, "token": "valid-token", "pastDays": 1, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusOK,
			response: []DailySubmissions{
				{Day: 5, EligibleSubmissions: 80, TotalSubmissions: 120},
			},
		},
		{
			name:       "Valid token, past days 3",
			body:       `{"slotID": 1, "token": "valid-token", "pastDays": 3, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusOK,
			response: []DailySubmissions{
				{Day: 5, EligibleSubmissions: 80, TotalSubmissions: 120},
				{Day: 4, EligibleSubmissions: 60, TotalSubmissions: 200},
				{Day: 3, EligibleSubmissions: 140, TotalSubmissions: 150},
			},
		},
		{
			name:       "Valid token, all submissions till date",
			body:       `{"slotID": 1, "token": "valid-token", "pastDays": 5, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusOK,
			response: []DailySubmissions{
				{Day: 5, EligibleSubmissions: 80, TotalSubmissions: 120},
				{Day: 4, EligibleSubmissions: 60, TotalSubmissions: 200},
				{Day: 3, EligibleSubmissions: 140, TotalSubmissions: 150},
				{Day: 2, EligibleSubmissions: 50, TotalSubmissions: 100},
				{Day: 1, EligibleSubmissions: 30, TotalSubmissions: 60},
			},
		},
		{
			name:       "Valid token, negative past days",
			body:       `{"slotID": 1, "token": "valid-token", "pastDays": -1, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusBadRequest,
			response:   nil,
		},
		{
			name:       "Invalid token",
			body:       `{"slotID": 1, "token": "invalid-token", "pastDays": 1, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusUnauthorized,
			response:   nil,
		},
		{
			name:       "Invalid Data Market Address",
			body:       `{"slotID": 1, "token": "valid-token", "pastDays": 1, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200d"}`,
			statusCode: http.StatusBadRequest,
			response:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("POST", "/totalSubmissions", strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(handleTotalSubmissions)
			testHandler := RequestMiddleware(handler)
			testHandler.ServeHTTP(rr, req)

			responseBody := rr.Body.String()
			t.Log("Response Body:", responseBody)

			assert.Equal(t, tt.statusCode, rr.Code)

			if tt.statusCode == http.StatusOK {
				var response struct {
					Info struct {
						Success  bool               `json:"success"`
						Response []DailySubmissions `json:"response"`
					} `json:"info"`
					RequestID string `json:"requestID"`
				}

				err := json.NewDecoder(rr.Body).Decode(&response)
				assert.NoError(t, err)

				err = json.Unmarshal([]byte(responseBody), &response)
				assert.NoError(t, err)
				assert.Equal(t, tt.response, response.Info.Response)
			}
		})
	}
}

func TestHandleEligibleNodeCount(t *testing.T) {
	// Set the authentication read token
	config.SettingsObj.AuthReadToken = "valid-token"

	// Set the current day
	redis.Set(context.Background(), redis.GetCurrentDayKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"), "3")

	// Set eligible slotIDs for each day
	slotIDsForDay3 := []string{"slot1", "slot2", "slot3"}
	redis.AddToSet(context.Background(), redis.EligibleNodesByDayKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "3"), slotIDsForDay3...)

	slotIDsForDay2 := []string{"slot4", "slot5", "slot6"}
	redis.AddToSet(context.Background(), redis.EligibleNodesByDayKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "2"), slotIDsForDay2...)

	slotIDsForDay1 := []string{"slot7", "slot8", "slot9"}
	redis.AddToSet(context.Background(), redis.EligibleNodesByDayKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "1"), slotIDsForDay1...)

	tests := []struct {
		name       string
		body       string
		statusCode int
		response   []EligibleNodes
	}{
		{
			name:       "Valid token, past days 1",
			body:       `{"epochID": 100, "token": "valid-token", "pastDays": 1, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusOK,
			response: []EligibleNodes{
				{Day: 3, Count: 3, SlotIDs: slotIDsForDay3},
			},
		},
		{
			name:       "Valid token, past days 3",
			body:       `{"epochID": 100, "token": "valid-token", "pastDays": 3, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusOK,
			response: []EligibleNodes{
				{Day: 3, Count: 3, SlotIDs: slotIDsForDay3},
				{Day: 2, Count: 3, SlotIDs: slotIDsForDay2},
				{Day: 1, Count: 3, SlotIDs: slotIDsForDay1},
			},
		},
		{
			name:       "Valid token, negative past days",
			body:       `{"epochID": 100, "token": "valid-token", "pastDays": -1, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusBadRequest,
			response:   nil,
		},
		{
			name:       "Invalid token",
			body:       `{"epochID": 100, "token": "invalid-token", "pastDays": 1, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusUnauthorized,
			response:   nil,
		},
		{
			name:       "Invalid EpochID",
			body:       `{"epochID": -1, "token": "valid-token", "pastDays": 1, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusBadRequest,
			response:   nil,
		},
		{
			name:       "Invalid Data Market Address",
			body:       `{"epochID": 100, "token": "valid-token", "pastDays": 1, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200d"}`,
			statusCode: http.StatusBadRequest,
			response:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("POST", "/eligibleNodesCount", strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(handleEligibleNodesCount)
			testHandler := RequestMiddleware(handler)
			testHandler.ServeHTTP(rr, req)

			responseBody := rr.Body.String()
			t.Log("Response Body:", responseBody)

			assert.Equal(t, tt.statusCode, rr.Code)

			if tt.statusCode == http.StatusOK {
				var response struct {
					Info struct {
						Success  bool            `json:"success"`
						Response []EligibleNodes `json:"response"`
					} `json:"info"`
					RequestID string `json:"requestID"`
				}

				err := json.NewDecoder(rr.Body).Decode(&response)
				assert.NoError(t, err)

				err = json.Unmarshal([]byte(responseBody), &response)
				assert.NoError(t, err)
				assert.Equal(t, tt.response, response.Info.Response)
			}
		})
	}
}

func TestHandleBatchCount(t *testing.T) {
	// Set the authentication read token
	config.SettingsObj.AuthReadToken = "valid-token"

	// Set the batch count
	redis.Set(context.Background(), redis.GetBatchCountKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", "123"), "10")

	tests := []struct {
		name       string
		body       string
		statusCode int
		response   BatchCount
	}{
		{
			name:       "Valid token, batch count fetched",
			body:       `{"epochID": 123, "token": "valid-token", "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusOK,
			response: BatchCount{
				TotalBatches: 10,
			},
		},
		{
			name:       "Invalid token",
			body:       `{"epochID": 123, "token": "invalid-token", "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusUnauthorized,
			response:   BatchCount{},
		},
		{
			name:       "Invalid EpochID",
			body:       `{"epochID": -1, "token": "valid-token", "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusBadRequest,
			response:   BatchCount{},
		},
		{
			name:       "Invalid Data Market Address",
			body:       `{"epochID": 123, "token": "valid-token", "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200d"}`,
			statusCode: http.StatusBadRequest,
			response:   BatchCount{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("POST", "/batchCount", strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(handleBatchCount)
			testHandler := RequestMiddleware(handler)
			testHandler.ServeHTTP(rr, req)

			responseBody := rr.Body.String()
			t.Log("Response Body:", responseBody)

			assert.Equal(t, tt.statusCode, rr.Code)

			if tt.statusCode == http.StatusOK {
				var response struct {
					Info struct {
						Success  bool       `json:"success"`
						Response BatchCount `json:"response"`
					} `json:"info"`
					RequestID string `json:"requestID"`
				}

				err := json.NewDecoder(rr.Body).Decode(&response)
				assert.NoError(t, err)

				err = json.Unmarshal([]byte(responseBody), &response)
				assert.NoError(t, err)
				assert.Equal(t, tt.response, response.Info.Response)
			}
		})
	}
}

func TestHandleEpochSubmissionDetails(t *testing.T) {
	// Set the authentication read token
	config.SettingsObj.AuthReadToken = "valid-token"

	// Set the epoch submission count
	redis.Set(context.Background(), redis.EpochSubmissionsCount("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", 123), "10")

	// Set the epoch submission details
	epochSubmissionKey := redis.EpochSubmissionsKey("0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c", 123)
	epochSubmissionsMap := getEpochSubmissionDetails(10)
	epochSubmissionsList := refactorEpochSubmissions(epochSubmissionsMap)

	for submissionID, submissionData := range epochSubmissionsMap {
		// Marshal the SnapshotSubmission into JSON
		submissionJSON, err := json.Marshal(submissionData)
		if err != nil {
			log.Fatalf("Failed to marshal SnapshotSubmission: %v", err)
		}

		// Add to Redis hash set
		if err := redis.RedisClient.HSet(context.Background(), epochSubmissionKey, submissionID, submissionJSON).Err(); err != nil {
			log.Fatalf("Failed to write submission details to Redis: %v", err)
		}
	}

	tests := []struct {
		name       string
		body       string
		statusCode int
		response   EpochSubmissionSummary
	}{
		{
			name:       "Valid token, epoch submission details fetched",
			body:       `{"epochID": 123, "token": "valid-token", "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusOK,
			response: EpochSubmissionSummary{
				SubmissionCount: 10,
				Submissions:     epochSubmissionsList,
			},
		},
		{
			name:       "Invalid token",
			body:       `{"epochID": 123, "token": "invalid-token", "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusUnauthorized,
			response:   EpochSubmissionSummary{},
		},
		{
			name:       "Invalid EpochID",
			body:       `{"epochID": -1, "token": "valid-token", "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusBadRequest,
			response:   EpochSubmissionSummary{},
		},
		{
			name:       "Invalid Data Market Address",
			body:       `{"epochID": 123, "token": "valid-token", "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200d"}`,
			statusCode: http.StatusBadRequest,
			response:   EpochSubmissionSummary{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("POST", "/epochSubmissionDetails", strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(handleEpochSubmissionDetails)
			testHandler := RequestMiddleware(handler)
			testHandler.ServeHTTP(rr, req)

			responseBody := rr.Body.String()
			t.Log("Response Body:", responseBody)

			assert.Equal(t, tt.statusCode, rr.Code)

			if tt.statusCode == http.StatusOK {
				var response struct {
					Info struct {
						Success  bool                   `json:"success"`
						Response EpochSubmissionSummary `json:"response"`
					} `json:"info"`
					RequestID string `json:"requestID"`
				}

				err := json.NewDecoder(rr.Body).Decode(&response)
				assert.NoError(t, err)

				err = json.Unmarshal([]byte(responseBody), &response)
				assert.NoError(t, err)
				assert.Equal(t, tt.response, response.Info.Response)
			}
		})
	}
}

func TestHandleEligibleSubmissionCount(t *testing.T) {
	// Set the authentication read token
	config.SettingsObj.AuthReadToken = "valid-token"

	// Set the params required
	dataMarketAddr := "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"
	currentDay := "5"
	epochID := "123"

	// Initialize an array to store the eligible slot submissions
	eligibleSlotSubmissions := make([]EligibleSubmissionCounts, 0)

	// Set the eligible slot submission count values in hashtable
	eligibleSlotSubmissionByEpochKey := redis.EligibleSlotSubmissionsByEpochKey(dataMarketAddr, currentDay, epochID)
	for slotID := 1; slotID <= 5; slotID++ {
		// Set the eligible slot submission count in Redis
		err := redis.RedisClient.HSet(context.Background(), eligibleSlotSubmissionByEpochKey, strconv.Itoa(slotID), 10).Err()
		if err != nil {
			log.Fatalf("Failed to add eligible slot submission count to hashtable for slotID %d: %v", slotID, err)
			continue
		}

		// Append to the eligible submissions list
		eligibleSlotSubmissions = append(eligibleSlotSubmissions, EligibleSubmissionCounts{SlotID: slotID, Count: 10})
	}

	tests := []struct {
		name       string
		body       string
		statusCode int
		response   EligibleSubmissionCountsResponse
	}{
		{
			name:       "Valid token, epoch submission details fetched",
			body:       `{"epochID": 123, "token": "valid-token", "day": 5, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusOK,
			response: EligibleSubmissionCountsResponse{
				SlotCounts: eligibleSlotSubmissions,
			},
		},
		{
			name:       "Invalid token",
			body:       `{"epochID": 123, "token": "invalid-token", "day": 5, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusUnauthorized,
			response:   EligibleSubmissionCountsResponse{},
		},
		{
			name:       "Invalid EpochID",
			body:       `{"epochID": -1, "token": "valid-token", "day": 5, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusBadRequest,
			response:   EligibleSubmissionCountsResponse{},
		},
		{
			name:       "Invalid Data Market Address",
			body:       `{"epochID": 123, "token": "valid-token", "day": 5, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200d"}`,
			statusCode: http.StatusBadRequest,
			response:   EligibleSubmissionCountsResponse{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("POST", "/eligibleSlotSubmissionCount", strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(handleEligibleSlotSubmissionCount)
			testHandler := RequestMiddleware(handler)
			testHandler.ServeHTTP(rr, req)

			responseBody := rr.Body.String()
			t.Log("Response Body:", responseBody)

			assert.Equal(t, tt.statusCode, rr.Code)

			if tt.statusCode == http.StatusOK {
				var response struct {
					Info struct {
						Success  bool                             `json:"success"`
						Response EligibleSubmissionCountsResponse `json:"response"`
					} `json:"info"`
					RequestID string `json:"requestID"`
				}

				err := json.NewDecoder(rr.Body).Decode(&response)
				assert.NoError(t, err)

				err = json.Unmarshal([]byte(responseBody), &response)
				assert.NoError(t, err)
				assert.Equal(t, len(tt.response.SlotCounts), len(response.Info.Response.SlotCounts))
			}
		})
	}
}

func TestHandleDiscardedSubmissions(t *testing.T) {
	// Set the authentication read token
	config.SettingsObj.AuthReadToken = "valid-token"

	// Set the params required
	dataMarketAddr := "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"
	currentDay := "5"
	epochID := "123"

	// Set up the discarded submissions map
	discardedSubmissionsMap := map[string]*DiscardedSubmissionDetails{
		"project1": {
			MostFrequentSnapshotCID:  "CID0",
			DiscardedSubmissionCount: 1,
			DiscardedSubmissions:     map[string][]string{"2": {"CID1"}},
		},
		"project2": {
			MostFrequentSnapshotCID:  "CID1",
			DiscardedSubmissionCount: 1,
			DiscardedSubmissions:     map[string][]string{"3": {"CID5"}},
		},
		"project3": {
			MostFrequentSnapshotCID:  "CID2",
			DiscardedSubmissionCount: 1,
			DiscardedSubmissions:     map[string][]string{"5": {"CID6"}},
		},
	}

	// Store the discarded submission details
	err := storeDiscardedSubmissionDetails(dataMarketAddr, currentDay, epochID, discardedSubmissionsMap)
	assert.NoError(t, err)

	tests := []struct {
		name       string
		body       string
		statusCode int
		response   DiscardedSubmissionsAPIResponse
	}{
		{
			name:       "Valid token, epoch submission details fetched",
			body:       `{"epochID": 123, "token": "valid-token", "day": 5, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusOK,
			response: DiscardedSubmissionsAPIResponse{
				Projects: []DiscardedSubmissionDetailsResponse{
					{ProjectID: "project1", Details: *discardedSubmissionsMap["project1"]},
					{ProjectID: "project2", Details: *discardedSubmissionsMap["project2"]},
					{ProjectID: "project3", Details: *discardedSubmissionsMap["project3"]},
				},
			},
		},
		{
			name:       "Invalid token",
			body:       `{"epochID": 123, "token": "invalid-token", "day": 5, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusUnauthorized,
			response:   DiscardedSubmissionsAPIResponse{},
		},
		{
			name:       "Invalid EpochID",
			body:       `{"epochID": -1, "token": "valid-token", "day": 5, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"}`,
			statusCode: http.StatusBadRequest,
			response:   DiscardedSubmissionsAPIResponse{},
		},
		{
			name:       "Invalid Data Market Address",
			body:       `{"epochID": 123, "token": "valid-token", "day": 5, "dataMarketAddress": "0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200d"}`,
			statusCode: http.StatusBadRequest,
			response:   DiscardedSubmissionsAPIResponse{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("POST", "/discardedSubmissions", strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(handleDiscardedSubmissions)
			testHandler := RequestMiddleware(handler)
			testHandler.ServeHTTP(rr, req)

			responseBody := rr.Body.String()
			t.Log("Response Body:", responseBody)

			// Assert the status code
			assert.Equal(t, tt.statusCode, rr.Code)

			// If the status code is OK, compare the response
			if tt.statusCode == http.StatusOK {
				var response struct {
					Info struct {
						Success  bool                            `json:"success"`
						Response DiscardedSubmissionsAPIResponse `json:"response"`
					} `json:"info"`
					RequestID string `json:"requestID"`
				}
				err := json.NewDecoder(rr.Body).Decode(&response)
				assert.NoError(t, err)
				assert.Equal(t, tt.response.Projects, response.Info.Response.Projects)
			}
		})
	}
}

func storeDiscardedSubmissionDetails(dataMarketAddress, currentDay, epochID string, discardedSubmissionsMap map[string]*DiscardedSubmissionDetails) error {
	// Construct the Redis main key for discarded submission details
	discardedKey := redis.DiscardedSubmissionsKey(dataMarketAddress, currentDay, epochID)

	// Write discarded submission details to Redis as a hashtable
	for projectID, details := range discardedSubmissionsMap {
		// Serialize the DiscardedSubmissionDetails struct
		detailsJSON, err := json.Marshal(details)
		if err != nil {
			return fmt.Errorf("failed to serialize discarded submission details for project %s: %v", projectID, err)
		}

		// Store the details in the Redis hashtable
		if err := redis.RedisClient.HSet(context.Background(), discardedKey, projectID, detailsJSON).Err(); err != nil {
			return fmt.Errorf("failed to write discarded submission details for project %s to Redis: %v", projectID, err)
		}
	}

	return nil
}

func getEpochSubmissionDetails(count int) map[string]*pkgs.SnapshotSubmission {
	epochSubmissions := make(map[string]*pkgs.SnapshotSubmission)

	for i := 1; i <= count; i++ {
		// Generate submissionID
		submissionID := fmt.Sprintf("submission-%d", i)

		// Create a sample SnapshotSubmission
		submission := &pkgs.SnapshotSubmission{
			Request: &pkgs.Request{
				EpochId: 123,
				SlotId:  uint64(i),
			},
		}

		// Add to the map
		epochSubmissions[submissionID] = submission
	}

	return epochSubmissions
}

func refactorEpochSubmissions(eligibleSubmissions map[string]*pkgs.SnapshotSubmission) []SubmissionDetails {
	epochSubmissionsList := make([]SubmissionDetails, 0)

	for submissionID, submission := range eligibleSubmissions {
		epochSubmissionsList = append(epochSubmissionsList, SubmissionDetails{
			SubmissionID: submissionID,
			SubmissionData: &SnapshotSubmissionSwagger{
				Request: &RequestSwagger{
					SlotID:      submission.Request.SlotId,
					Deadline:    submission.Request.Deadline,
					EpochID:     submission.Request.EpochId,
					SnapshotCID: submission.Request.SnapshotCid,
					ProjectID:   submission.Request.ProjectId,
				},
				Signature: submission.Signature,
				Header:    submission.Header,
			},
		})
	}

	sort.Slice(epochSubmissionsList, func(i, j int) bool {
		return epochSubmissionsList[i].SubmissionID < epochSubmissionsList[j].SubmissionID
	})

	return epochSubmissionsList
}
