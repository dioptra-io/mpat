package v1

import (
	"fmt"
	"net"
	"time"

	"dioptra-io/ufuk-research/pkg/config"
	"dioptra-io/ufuk-research/pkg/util"
)

type ArkCycle struct {
	Date time.Time `json:"date"`
	URL  string    `json:"url"`
}

func ArkCycleFromTime(t time.Time) *ArkCycle {
	return &ArkCycle{
		Date: t,
		URL:  ArkCycleURL(t),
	}
}

type ArkWartFile struct {
	Date time.Time `json:"date"`
	Name string    `json:"name"`
	URL  string    `json:"url"`
}

func ArkWartFromTime(t time.Time, name string) *ArkWartFile {
	return &ArkWartFile{
		Date: t,
		URL:  ArkCycleURL(t),
	}
}

func ArkCycleURL(t time.Time) string {
	dateString := util.TimeString(t)
	generatedURL := fmt.Sprintf("%s/%v/cycle-%s", config.ArkIPv4DatabaseBaseUrl, t.Year(), dateString)
	return generatedURL
}

func ArkWartURL(t time.Time, name string) string {
	dateString := ArkCycleURL(t)
	return fmt.Sprintf("%s/%s", dateString, name)
}

// This represents one row for the Iris results table entry.
type ProbeRecord struct {
	CaptureTimestamp time.Time
	ProbeProtocol    uint8
	ProbeSrcAddr     net.IP
	ProbeDstAddr     net.IP
	ProbeSrcPort     uint16
	ProbeDstPort     uint16
	ProbeTTL         uint8
	QuotedTTL        uint8
	ReplySrcAddr     net.IP
	ReplyProtocol    uint8
	ReplyICMPType    uint8
	ReplyICMPCode    uint8
	ReplyTTL         uint8
	ReplySize        uint16
	ReplyMPLSLabels  []struct {
		Label         uint32
		Exp           uint8
		BottomOfStack uint8
		TTL           uint8
	}
	RTT   uint16
	Round uint8
}
