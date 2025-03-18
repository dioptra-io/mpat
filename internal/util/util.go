package util

import (
	"time"
)

// Given the format is 'YYYY-MM-DD'
func ArgsToDateTime(timeString []string) ([]time.Time, error) {
	times := make([]time.Time, len(timeString))

	for i := 0; i < len(timeString); i++ {
		t, err := time.Parse("2006-01-02", timeString[i])
		if err != nil {
			return nil, err
		}
		times[i] = t
	}
	return times, nil
}
