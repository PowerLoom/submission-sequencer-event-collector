package redis

import (
	"fmt"
	"strings"
	"submission-sequencer-collector/config"
	"submission-sequencer-collector/pkgs"
)

func EpochMarkerSet() string {
	return pkgs.EpochMarkerSetKey
}

func SubmissionSetByHeaderKey(epoch uint64, header string) string {
	return fmt.Sprintf("%s.%d.%s", pkgs.CollectorKey, epoch, header)
}

func BlockHashByNumber(blockNum int64) string {
	return fmt.Sprintf("%s.%d", pkgs.BlockHashByNumberKey, blockNum)
}

func GetSubmissionQueueKey() string {
	return fmt.Sprintf("submissionQueue.%s", strings.ToLower(config.SettingsObj.DataMarketAddress))
}
