package util

import (
	"fmt"
	"math"
	"strings"
	"time"
)

func GetDatesBetween(start, end time.Time) []time.Time {
	var dates []time.Time
	for current := start; current.Before(end) || current.Equal(end); current = current.AddDate(0, 0, 1) {
		dates = append(dates, current)
	}
	return dates
}

func ParseStrings(startTimeStr, endTimeStr string) (time.Time, time.Time, error) {
	startTime, err := ParseDateTime(startTimeStr)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	endTime, err := ParseDateTime(endTimeStr)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return startTime, endTime, nil
}

func GetDatesBetweenFromString(start, end string) ([]time.Time, error) {
	startDate, endDate, err := ParseStrings(start, end)
	if err != nil {
		return nil, err
	}
	datesBetween := GetDatesBetween(startDate, endDate)
	return datesBetween, nil
}

func ParseDateTime(dt string) (time.Time, error) {
	if strings.Contains(dt, " ") {
		return time.Parse("2006-01-02 15:04:05", dt)
	}
	return time.Parse("2006-01-02", dt)
}

func GetUniqueAgentNames(wartLinks []string) []string {
	unique := make(map[string]struct{})

	for _, url := range wartLinks {
		parts := strings.Split(url, "/")
		if len(parts) == 0 {
			continue
		}
		lastPart := parts[len(parts)-1]

		nameParts := strings.Split(lastPart, ".")
		if len(nameParts) == 0 {
			continue
		}
		firstElement := nameParts[0]

		unique[firstElement] = struct{}{}
	}

	result := make([]string, 0, len(unique))
	for key := range unique {
		result = append(result, key)
	}

	return result
}

func TimeString(t time.Time) string {
	dateString := fmt.Sprintf("%d%02d%02d", t.Year(), int(t.Month()), int(t.Day()))
	return dateString
}

func ExponentialBackoff(i int, maxCap time.Duration) time.Duration {
	base := 100 * time.Millisecond
	backoff := float64(base) * math.Pow(2, float64(i))

	// Cap the backoff if it exceeds maxCap
	if backoff > float64(maxCap) {
		return maxCap
	}
	return time.Duration(backoff)
}
