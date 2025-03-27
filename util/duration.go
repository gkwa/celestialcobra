package util

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// ParseDuration parses human-readable duration strings like "1h", "2d", "3w"
func ParseDuration(durationStr string) (time.Duration, error) {
	// Regular expression to match duration format (e.g., 1h, 2d, 3w, 4m, 5y)
	re := regexp.MustCompile(`^(\d+)([hdwmy])$`)
	matches := re.FindStringSubmatch(durationStr)

	if matches == nil {
		return 0, fmt.Errorf("invalid duration format: %s (expected format like 1h, 2d, 3w)", durationStr)
	}

	value, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, fmt.Errorf("invalid duration value: %v", err)
	}

	unit := matches[2]

	// Convert to time.Duration
	switch unit {
	case "h":
		return time.Duration(value) * time.Hour, nil
	case "d":
		return time.Duration(value*24) * time.Hour, nil
	case "w":
		return time.Duration(value*24*7) * time.Hour, nil
	case "m":
		return time.Duration(value*24*30) * time.Hour, nil // Approximate
	case "y":
		return time.Duration(value*24*365) * time.Hour, nil // Approximate
	default:
		return 0, fmt.Errorf("unsupported duration unit: %s", unit)
	}
}
