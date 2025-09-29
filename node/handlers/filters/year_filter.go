package filters

import (
	"fmt"
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
		return false
	}

	return year >= yf.MinYear && year <= yf.MaxYear
}
