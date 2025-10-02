package filters

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// HourFilter filters records by hour range (24-hour format)
type HourFilter struct {
	MinHour int // e.g., 6 for 6AM
	MaxHour int // e.g., 23 for 11PM
}

func NewHourFilter(minHour, maxHour int) *HourFilter {
	return &HourFilter{
		MinHour: minHour,
		MaxHour: maxHour,
	}
}

func (hf *HourFilter) Name() string {
	return fmt.Sprintf("hour_filter_%d_%d", hf.MinHour, hf.MaxHour)
}

func (hf *HourFilter) Filter(record protocol.Record) bool {
	createdAt, err := extractCreatedAt(record)
	if err != nil {
		log.Printf("action: HOUR_FILTER_ERROR | error: %v | record_type: %T", err, record)
		return false
	}

	// Parse the datetime to extract hour
	var timePart string
	if strings.Contains(createdAt, "T") {
		// Format: "2024-01-15T10:30:00"
		parts := strings.Split(createdAt, "T")
		if len(parts) > 1 {
			timePart = parts[1]
		}
	} else if strings.Contains(createdAt, " ") {
		// Format: "2024-01-15 10:30:00"
		parts := strings.Split(createdAt, " ")
		if len(parts) > 1 {
			timePart = parts[1]
		}
	}

	if timePart == "" {
		log.Printf("action: HOUR_FILTER_NO_TIME | created_at: %s", createdAt)
		return false
	}

	// Extract hour from time part (format: "HH:MM:SS")
	hourStr := strings.Split(timePart, ":")[0]
	hour, err := strconv.Atoi(hourStr)
	if err != nil {
		log.Printf("action: HOUR_FILTER_PARSE_ERROR | created_at: %s | time_part: %s | hour_str: %s | error: %v", createdAt, timePart, hourStr, err)
		return false
	}

	result := hour >= hf.MinHour && hour <= hf.MaxHour

	return result
}
