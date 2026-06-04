package ripe

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultEndpoint = "https://stat.ripe.net"
)

// Tier1ASNs hardcodes the tier 1 ASNs retieved from Better Targeting document
// refer [here](https://docs.google.com/document/d/1GNshr21z6xZ8nhUkdjIgVGrsD7Ybxk1S-d2ZvwOdDzM/edit?tab=t.0).
var Tier1ASNs = []uint32{
	3356,  // Lumen (Level 3)
	1299,  // Arelion
	3257,  // GTT
	2914,  // NTT
	6453,  // Tata
	6461,  // Zayo
	6762,  // Sparkle
	3491,  // PCCW Global
	5511,  // Orange
	12956, // Telxius
	3320,  // Deutsche Telekom
	6830,  // Liberty Global
	7018,  // AT&T
	701,   // Verizon
	174,   // Cogent
	6939,  // Hurricane Electric
}

// RipeConfig holds configuration for the RipeClient.
type RipeConfig struct {
	// Endpoint is the base URL for the RIPE Stat API.
	// Defaults to https://stat.ripe.net.
	Endpoint string
}

func (c *RipeConfig) endpoint() string {
	if c.Endpoint == "" {
		return DefaultEndpoint
	}
	return strings.TrimRight(c.Endpoint, "/")
}

// RipeClient is a client for the RIPE Stat Data API.
// No authentication is required — the API is public.
type RipeClient struct {
	config RipeConfig
	http   *http.Client
}

// NewRipeClient creates a new RipeClient with the given config.
func NewRipeClient(cfg RipeConfig) *RipeClient {
	return &RipeClient{
		config: cfg,
		http:   &http.Client{Timeout: 30 * time.Second},
	}
}

// PrefixQuery is a builder for the ris-prefixes endpoint.
type PrefixQuery struct {
	client    *RipeClient
	asns      []uint32
	queryTime time.Time
	err       error // stored here, surfaced at Fetch()
}

// PrefixesByASN returns a new PrefixQuery for a single ASN.
// It is a convenience wrapper around PrefixesByASNs.
func (c *RipeClient) PrefixesByASN(asn uint32) *PrefixQuery {
	return c.PrefixesByASNs([]uint32{asn})
}

// PrefixesByASNs returns a new PrefixQuery for a list of ASNs.
// Fetch() will query each ASN sequentially and aggregate the results,
// failing fast on the first error.
func (c *RipeClient) PrefixesByASNs(asns []uint32) *PrefixQuery {
	return &PrefixQuery{
		client: c,
		asns:   asns,
	}
}

// At sets the query time using a date and a TimeOfDay enum value.
// For Night, the query time is automatically advanced to the next day at 00:00 UTC.
// Any validation error is stored and surfaced at Fetch() time.
func (q *PrefixQuery) At(date time.Time, tod TimeOfDay) *PrefixQuery {
	t, err := tod.QueryTime(date)
	if err != nil {
		q.err = err
		return q
	}
	q.queryTime = t
	return q
}

// AtTime sets the query time using a raw timestamp.
// The timestamp should be aligned to one of the RIS dump times: 00:00, 08:00 or 16:00 UTC.
func (q *PrefixQuery) AtTime(t time.Time) *PrefixQuery {
	q.queryTime = t.UTC()
	return q
}

// Fetch executes the query and returns the list of prefixes originated by all ASNs.
// Fails fast on the first error.
func (q *PrefixQuery) Fetch() ([]Prefix, error) {
	if q.err != nil {
		return nil, q.err
	}

	var all []Prefix
	for _, asn := range q.asns {
		prefixes, err := q.fetchASN(asn)
		if err != nil {
			return nil, err
		}
		all = append(all, prefixes...)
	}
	return all, nil
}

// fetchASN fetches prefixes for a single ASN.
func (q *PrefixQuery) fetchASN(asn uint32) ([]Prefix, error) {
	params := url.Values{}
	params.Set("resource", fmt.Sprintf("AS%d", asn))
	params.Set("list_prefixes", "true")
	params.Set("types", "o")
	params.Set("noise", "filter")
	if !q.queryTime.IsZero() {
		params.Set("query_time", q.queryTime.UTC().Format(time.RFC3339))
	}

	u := fmt.Sprintf("%s/data/ris-prefixes/data.json?%s", q.client.config.endpoint(), params.Encode())

	resp, err := q.client.http.Get(u)
	if err != nil {
		return nil, fmt.Errorf("ripe: request failed for ASN %d: %w", asn, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ripe: unexpected status %d for ASN %d", resp.StatusCode, asn)
	}

	var result risPrefixesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("ripe: failed to decode response for ASN %d: %w", asn, err)
	}
	if result.Status != "ok" {
		return nil, fmt.Errorf("ripe: API returned status %q for ASN %d", result.Status, asn)
	}

	queryTime, err := time.Parse("2006-01-02T15:04:05", result.Data.QueryTime)
	if err != nil {
		return nil, fmt.Errorf("ripe: failed to parse query_time %q: %w", result.Data.QueryTime, err)
	}

	var prefixes []Prefix
	for _, raw := range result.Data.Prefixes.V4.Originating {
		p, err := parsePrefix(asn, raw, queryTime, false)
		if err != nil {
			return nil, err
		}
		prefixes = append(prefixes, p)
	}
	for _, raw := range result.Data.Prefixes.V6.Originating {
		p, err := parsePrefix(asn, raw, queryTime, true)
		if err != nil {
			return nil, err
		}
		prefixes = append(prefixes, p)
	}

	return prefixes, nil
}

// parsePrefix parses a prefix string like "67.24.0.0/13" or "2001:13b2::/32"
// into a Prefix struct. IPv4 addresses are stored as IPv4-mapped IPv6.
func parsePrefix(asn uint32, raw string, queryTime time.Time, isV6 bool) (Prefix, error) {
	parts := strings.SplitN(raw, "/", 2)
	if len(parts) != 2 {
		return Prefix{}, fmt.Errorf("ripe: invalid prefix %q", raw)
	}

	ip := net.ParseIP(parts[0])
	if ip == nil {
		return Prefix{}, fmt.Errorf("ripe: invalid IP address %q in prefix %q", parts[0], raw)
	}

	// Store IPv4 as IPv4-mapped IPv6 (::ffff:x.x.x.x).
	if !isV6 {
		ip = ip.To16()
	}

	prefixLen, err := strconv.ParseUint(parts[1], 10, 8)
	if err != nil {
		return Prefix{}, fmt.Errorf("ripe: invalid prefix length %q in prefix %q: %w", parts[1], raw, err)
	}

	return Prefix{
		ASN:       asn,
		Network:   ip,
		PrefixLen: uint8(prefixLen),
		QueryTime: queryTime,
	}, nil
}
