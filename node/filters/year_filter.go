package filters

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// YearFilter filters records by year range
type YearFilter struct {
	MinYear int
	MaxYear int
}

func NewYearFilter(minYear, maxYear int) *YearFilter {
	return &YearFilter{
		MinYear: minYear,
		MaxYear: maxYear,
	}
}

func (yf *YearFilter) Name() string {
	return fmt.Sprintf("year_filter_%d_%d", yf.MinYear, yf.MaxYear)
}

func (yf *YearFilter) Filter(record protocol.Record) bool {
	createdAt, err := extractCreatedAt(record)
	if err != nil {
		log.Printf("action: YEAR_FILTER_ERROR | error: %v | record_type: %T", err, record)
		return false
	}

	// Parse the date - assuming format like "2024-01-15T10:30:00" or "2024-01-15"
	var datePart string
	if strings.Contains(createdAt, "T") {
		datePart = strings.Split(createdAt, "T")[0]
	} else {
		datePart = strings.Split(createdAt, " ")[0] // Handle "YYYY-MM-DD HH:MM:SS" format
	}

	yearStr := strings.Split(datePart, "-")[0]
	year, err := strconv.Atoi(yearStr)
	if err != nil {
		log.Printf("action: YEAR_FILTER_PARSE_ERROR | created_at: %s | date_part: %s | year_str: %s | error: %v", createdAt, datePart, yearStr, err)
		return false
	}

	result := year >= yf.MinYear && year <= yf.MaxYear

	return result
}
