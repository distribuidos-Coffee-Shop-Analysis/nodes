package protocol

import (
	"fmt"
)

// MenuItemRecord represents menu item record: item_id, item_name, category, price, is_seasonal, available_from, available_to
type MenuItemRecord struct {
	ItemID        string
	ItemName      string
	Category      string
	Price         string
	IsSeasonal    string
	AvailableFrom string
	AvailableTo   string
}

const MenuItemRecordParts = 7

// Serialize returns the string representation of the menu item record
func (m *MenuItemRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s",
		m.ItemID, m.ItemName, m.Category, m.Price, m.IsSeasonal, m.AvailableFrom, m.AvailableTo)
}

// GetType returns the dataset type for menu item records
func (m *MenuItemRecord) GetType() DatasetType {
	return DatasetTypeMenuItems
}

// NewMenuItemRecordFromParts creates a MenuItemRecord from string parts
func NewMenuItemRecordFromParts(parts []string) (*MenuItemRecord, error) {
	if len(parts) < MenuItemRecordParts {
		return nil, fmt.Errorf("invalid MenuItemRecord format: expected %d fields, got %d",
			MenuItemRecordParts, len(parts))
	}

	return &MenuItemRecord{
		ItemID:        parts[0],
		ItemName:      parts[1],
		Category:      parts[2],
		Price:         parts[3],
		IsSeasonal:    parts[4],
		AvailableFrom: parts[5],
		AvailableTo:   parts[6],
	}, nil
}

// StoreRecord represents store record: store_id, store_name, street, postal_code, city, state, latitude, longitude
type StoreRecord struct {
	StoreID    string
	StoreName  string
	Street     string
	PostalCode string
	City       string
	State      string
	Latitude   string
	Longitude  string
}

const StoreRecordParts = 8

// Serialize returns the string representation of the store record
func (s *StoreRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s",
		s.StoreID, s.StoreName, s.Street, s.PostalCode, s.City, s.State, s.Latitude, s.Longitude)
}

// GetType returns the dataset type for store records
func (s *StoreRecord) GetType() DatasetType {
	return DatasetTypeStores
}

// NewStoreRecordFromParts creates a StoreRecord from string parts
func NewStoreRecordFromParts(parts []string) (*StoreRecord, error) {
	if len(parts) < StoreRecordParts {
		return nil, fmt.Errorf("invalid StoreRecord format: expected %d fields, got %d",
			StoreRecordParts, len(parts))
	}

	return &StoreRecord{
		StoreID:    parts[0],
		StoreName:  parts[1],
		Street:     parts[2],
		PostalCode: parts[3],
		City:       parts[4],
		State:      parts[5],
		Latitude:   parts[6],
		Longitude:  parts[7],
	}, nil
}
