package ripe

import (
	"fmt"
	"net"
	"time"
)

// TimeOfDay represents one of the three daily RIS snapshot times.
type TimeOfDay string

const (
	// Dawn corresponds to the 08:00 UTC RIS snapshot.
	Dawn TimeOfDay = "dawn"
	// Day corresponds to the 16:00 UTC RIS snapshot.
	Day TimeOfDay = "day"
	// Night corresponds to the 00:00 UTC RIS snapshot of the following day.
	Night TimeOfDay = "night"
)

// QueryTime resolves a TimeOfDay to a concrete UTC timestamp for the given date.
// For Night, the timestamp is advanced to the next day at 00:00 UTC.
func (t TimeOfDay) QueryTime(date time.Time) (time.Time, error) {
	y, m, d := date.Date()
	switch t {
	case Dawn:
		return time.Date(y, m, d, 8, 0, 0, 0, time.UTC), nil
	case Day:
		return time.Date(y, m, d, 16, 0, 0, 0, time.UTC), nil
	case Night:
		return time.Date(y, m, d+1, 0, 0, 0, 0, time.UTC), nil
	default:
		return time.Time{}, fmt.Errorf("ripe: unknown time of day %q, expected dawn, day or night", t)
	}
}

// Prefix represents a single IP prefix originated by an AS.
type Prefix struct {
	ASN       uint32
	Network   net.IP
	PrefixLen uint8
	QueryTime time.Time
}

type risPrefixesResponse struct {
	Status string `json:"status"`
	Data   struct {
		QueryTime string `json:"query_time"`
		Prefixes  struct {
			V4 struct {
				Originating []string `json:"originating"`
			} `json:"v4"`
			V6 struct {
				Originating []string `json:"originating"`
			} `json:"v6"`
		} `json:"prefixes"`
	} `json:"data"`
}
