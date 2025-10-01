package joiners

import (
	"fmt"
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q2Joiner handles joining Q2 aggregate data with menu item names
type Q2Joiner struct {
	// In-memory storage for menu items (reference data)
	menuItems map[string]*protocol.MenuItemRecord // key: item_id, value: menu item record
	mu        sync.RWMutex                        // mutex for thread-safe access
}

// NewQ2Joiner creates a new Q2Joiner instance
func NewQ2Joiner() *Q2Joiner {
	return &Q2Joiner{
		menuItems: make(map[string]*protocol.MenuItemRecord),
	}
}

// Name returns the name of this joiner
func (j *Q2Joiner) Name() string {
	return "q2_joiner_menu_items"
}

// StoreReferenceData stores menu item reference data for future joins
func (j *Q2Joiner) StoreReferenceData(records []protocol.Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	log.Printf("action: q2_store_reference_data | count: %d", len(records))

	for _, record := range records {
		menuItem, ok := record.(*protocol.MenuItemRecord)
		if !ok {
			return fmt.Errorf("expected MenuItemRecord, got %T", record)
		}

		j.menuItems[menuItem.ItemID] = menuItem
		log.Printf("action: q2_menu_item_stored | item_id: %s | item_name: %s",
			menuItem.ItemID, menuItem.ItemName)
	}

	log.Printf("action: q2_reference_data_stored | total_items: %d", len(j.menuItems))
	return nil
}

// PerformJoin joins Q2 aggregated data with stored menu items
func (j *Q2Joiner) PerformJoin(aggregatedRecords []protocol.Record) ([]protocol.Record, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		switch aggRecord := record.(type) {
		case *protocol.Q2GroupWithQuantityRecord:
			// Join best selling data with menu item names
			menuItem, exists := j.menuItems[aggRecord.ItemID]
			if !exists {
				log.Printf("action: q2_join_warning | item_id: %s | error: menu_item_not_found", aggRecord.ItemID)
				continue // Skip records without matching menu items
			}

			joinedRecord := &protocol.Q2BestSellingWithNameRecord{
				YearMonth:   aggRecord.YearMonth,
				ItemName:    menuItem.ItemName,
				SellingsQty: aggRecord.SellingsQty,
			}
			joinedRecords = append(joinedRecords, joinedRecord)

			log.Printf("action: q2_join_best_selling | item_id: %s | item_name: %s | qty: %s",
				aggRecord.ItemID, menuItem.ItemName, aggRecord.SellingsQty)

		case *protocol.Q2GroupWithSubtotalRecord:
			// Join most profitable data with menu item names
			menuItem, exists := j.menuItems[aggRecord.ItemID]
			if !exists {
				log.Printf("action: q2_join_warning | item_id: %s | error: menu_item_not_found", aggRecord.ItemID)
				continue // Skip records without matching menu items
			}

			joinedRecord := &protocol.Q2MostProfitsWithNameRecord{
				YearMonth: aggRecord.YearMonth,
				ItemName:  menuItem.ItemName,
				ProfitSum: aggRecord.ProfitSum,
			}
			joinedRecords = append(joinedRecords, joinedRecord)

			log.Printf("action: q2_join_most_profits | item_id: %s | item_name: %s | profit: %s",
				aggRecord.ItemID, menuItem.ItemName, aggRecord.ProfitSum)

		default:
			return nil, fmt.Errorf("unsupported Q2 aggregated record type: %T", record)
		}
	}

	return joinedRecords, nil
}

// GetOutputDatasetType returns the dataset type for Q2 joined output
func (j *Q2Joiner) GetOutputDatasetType() protocol.DatasetType {
	return protocol.DatasetTypeQ2AggWithName
}

// AcceptsReferenceType checks if this joiner accepts menu items as reference data
func (j *Q2Joiner) AcceptsReferenceType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeMenuItems
}

// AcceptsAggregateType checks if this joiner accepts Q2 aggregate data
func (j *Q2Joiner) AcceptsAggregateType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeQ2Agg
}
