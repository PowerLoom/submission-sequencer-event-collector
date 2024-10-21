package redis

import (
	"fmt"
	"strings"
	"submission-sequencer-collector/pkgs"
)

func GetSubmissionLimitTableKey() string {
	return pkgs.SubmissionLimitTableKey
}

func EpochMarkerSet(dataMarketAddress string) string {
	return fmt.Sprintf("%s.%s", pkgs.EpochMarkerSetKey, strings.ToLower(dataMarketAddress))
}

func SubmissionSetByHeaderKey(epoch uint64, header, dataMarketAddress string) string {
	return fmt.Sprintf("%s.%d.%s.%s", pkgs.CollectorKey, epoch, header, dataMarketAddress)
}

func BlockHashByNumber(blockNum int64) string {
	return fmt.Sprintf("%s.%d", pkgs.BlockHashByNumberKey, blockNum)
}
