package v1

import (
	"fmt"
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
