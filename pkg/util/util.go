package util

import (
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
