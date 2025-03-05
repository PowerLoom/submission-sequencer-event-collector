package redis

import (
	"fmt"
	"strings"
	"submission-sequencer-collector/pkgs"
)

func GetDaySizeTableKey() string {
	return pkgs.DaySizeTableKey
}

func GetDailySnapshotQuotaTableKey() string {
	return pkgs.DailySnapshotQuotaTableKey
}

func GetSubmissionLimitTableKey() string {
	return pkgs.SubmissionLimitTableKey
}

func GetCurrentDayKey(dataMarketAddress string) string {
	return fmt.Sprintf("%s.%s", pkgs.CurrentDayKey, strings.ToLower(dataMarketAddress))
}

func LastKnownDay(dataMarketAddress string) string {
	return fmt.Sprintf("%s.%s", pkgs.LastKnownDayKey, strings.ToLower(dataMarketAddress))
}

func EpochMarkerSet(dataMarketAddress string) string {
	return fmt.Sprintf("%s.%s", pkgs.EpochMarkerSetKey, strings.ToLower(dataMarketAddress))
}

func DayRolloverEpochMarkerSet(dataMarketAddress string) string {
	return fmt.Sprintf("%s.%s", pkgs.DayRolloverEpochMarkerSetKey, strings.ToLower(dataMarketAddress))
}

func DayRolloverEpochMarkerDetails(dataMarketAddress, epochID string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.DayRolloverEpochMarkerDetailsKey, strings.ToLower(dataMarketAddress), epochID)
}

func EpochMarkerDetails(dataMarketAddress, epochID string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.EpochMarkerDetailsKey, strings.ToLower(dataMarketAddress), epochID)
}

func ContractStateVariableWithDataMarket(dataMarketAddress string, varName string) string {
	return fmt.Sprintf("ProtocolState.%s.%s", dataMarketAddress, varName)
}

func SubmissionSetByHeaderKey(dataMarketAddress string, epoch uint64, header string) string {
	return fmt.Sprintf("%s.%s.%d.%s", pkgs.CollectorKey, strings.ToLower(dataMarketAddress), epoch, header)
}

func SlotSubmissionKey(dataMarketAddress string, slotID, currentDay string) string {
	return fmt.Sprintf("%s.%s.%s.%s", pkgs.SlotSubmissionsKey, strings.ToLower(dataMarketAddress), currentDay, slotID)
}

func BlockHashByNumber(blockNum int64) string {
	return fmt.Sprintf("%s.%d", pkgs.BlockHashByNumberKey, blockNum)
}

func EligibleSlotSubmissionKey(dataMarketAddress string, slotID, currentDay string) string {
	return fmt.Sprintf("%s.%s.%s.%s", pkgs.EligibleSlotSubmissionsKey, strings.ToLower(dataMarketAddress), currentDay, slotID)
}

func BatchSubmissionKey(dataMarketAddress, epochID, batchID string) string {
	return fmt.Sprintf("%s.%s.%s.%s", pkgs.BatchSubmissionsKey, strings.ToLower(dataMarketAddress), epochID, batchID)
}

func GetBatchCountKey(dataMarketAddress, epochID string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.BatchCountKey, strings.ToLower(dataMarketAddress), epochID)
}

func EligibleNodesByDayKey(dataMarketAddress, currentDay string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.EligibleNodeByDayKey, strings.ToLower(dataMarketAddress), currentDay)
}

func EpochSubmissionsKey(dataMarketAddress string, epochID uint64) string {
	return fmt.Sprintf("%s.%s.%d", pkgs.EpochSubmissionsKey, strings.ToLower(dataMarketAddress), epochID)
}

func EligibleSlotSubmissionsByEpochKey(dataMarketAddress, currentDay, epochID string) string {
	return fmt.Sprintf("%s.%s.%s.%s", pkgs.EligibleSlotSubmissionByEpochKey, strings.ToLower(dataMarketAddress), currentDay, epochID)
}

func DiscardedSubmissionsKey(dataMarketAddress, currentDay, epochID string) string {
	return fmt.Sprintf("%s.%s.%s.%s", pkgs.DiscardedSubmissionKey, strings.ToLower(dataMarketAddress), currentDay, epochID)
}

func DiscardedSubmissionsByDayKey(dataMarketAddress, currentDay string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.DiscardedSubmissionKey, strings.ToLower(dataMarketAddress), currentDay)
}

func ZeroCountUpdateKey(dataMarketAddress, day string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.ZeroCountKey, strings.ToLower(dataMarketAddress), day)
}

func LastSimulatedSubmission(dataMarketAddress string, slotID uint64) string {
	return fmt.Sprintf("%s.%s.%d", pkgs.LastSimulatedSubmissionKey, strings.ToLower(dataMarketAddress), slotID)
}

func LastSnapshotSubmission(dataMarketAddress string, slotID uint64) string {
	return fmt.Sprintf("%s.%s.%d", pkgs.LastSnapshotSubmissionKey, strings.ToLower(dataMarketAddress), slotID)
}

func TotalNodesCountKey() string {
	return pkgs.TotalNodesCount
}
