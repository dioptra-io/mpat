package util

import (
	"fmt"
	"io"
	"math"
	"net"
	"regexp"
	"strings"
	"time"

	"golang.org/x/net/html"
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

func ContainsIP(addresses []net.IP, addr net.IP) bool {
	for _, ip := range addresses {
		if addr.Equal(ip) {
			return true
		}
	}
	return false
}

func ExtractDigitHrefLinks(r io.Reader) ([]string, error) {
	var hrefs []string
	// Compile regex pattern to match href="digits/"
	hrefPattern := regexp.MustCompile(`^\d+/`)

	// Parse HTML from reader
	doc, err := html.Parse(r)
	if err != nil {
		return nil, fmt.Errorf("error parsing HTML: %w", err)
	}

	// Recursive function to traverse nodes
	var traverse func(*html.Node)
	traverse = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" && hrefPattern.MatchString(attr.Val) {
					hrefs = append(hrefs, attr.Val)
				}
			}
		}
		// Recurse to child nodes
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			traverse(c)
		}
	}

	// Start traversal
	traverse(doc)
	return hrefs, nil
}

func ProtocolToUint8(p string) uint8 {
	switch p {
	case "ICMP":
		return 1
	case "UDP":
		return 2
	case "TCP":
		return 3
	default:
		return 0
	}
}

func MergeMaps(map1, map2 map[string]any) map[string]any {
	result := make(map[string]any)

	for k, v := range map1 {
		result[k] = v
	}

	for k, v := range map2 {
		result[k] = v
	}

	return result
}
