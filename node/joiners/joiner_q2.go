package joiners

import (
	"fmt"
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q2JoinerState struct {
	MenuItems map[string]*protocol.MenuItemRecord // key: item_id, value: menu item record
}

// Q2Joiner handles joining Q2 aggregate data with menu item names
type Q2Joiner struct {
	state       *Q2JoinerState
	mu          sync.RWMutex
	persistence *common.StatePersistence
	clientID    string
}

// NewQ2Joiner creates a new Q2Joiner instance
func NewQ2Joiner() *Q2Joiner {
	return NewQ2JoinerWithPersistence("/app/state")
}

func NewQ2JoinerWithPersistence(stateDir string) *Q2Joiner {
	persistence, err := common.NewStatePersistence(stateDir)
	if err != nil {
		log.Printf("action: q2_joiner_init | result: fail | error: %v | fallback: memory_only", err)
		persistence = nil
	}

	return &Q2Joiner{
		state: &Q2JoinerState{
			MenuItems: make(map[string]*protocol.MenuItemRecord),
		},
		persistence: persistence,
	}
}

// Name returns the name of this joiner
func (j *Q2Joiner) Name() string {
	return "q2_joiner_menu_items"
}

// StoreReferenceDataset stores menu item reference data for future joins
func (j *Q2Joiner) StoreReferenceDataset(records []protocol.Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	log.Printf("action: q2_store_reference_data | count: %d", len(records))

	for _, record := range records {
		menuItem, ok := record.(*protocol.MenuItemRecord)
		if !ok {
			return fmt.Errorf("expected MenuItemRecord, got %T", record)
		}

		j.state.MenuItems[menuItem.ItemID] = menuItem
		log.Printf("action: q2_menu_item_stored | item_id: %s | item_name: %s",
			menuItem.ItemID, menuItem.ItemName)
	}

	log.Printf("action: q2_reference_data_stored | total_items: %d", len(j.state.MenuItems))

	if j.persistence != nil && j.clientID != "" {
		if err := j.persistence.SaveState(j.Name(), j.clientID, j.state); err != nil {
			log.Printf("action: q2_joiner_save_state | result: fail | client_id: %s | error: %v",
				j.clientID, err)
		}
	}

	return nil
}

// PerformJoin joins Q2 aggregated data with stored menu items
func (j *Q2Joiner) PerformJoin(aggregatedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {

	if j.clientID == "" {
		j.clientID = clientId

		if j.persistence != nil {
			var savedState Q2JoinerState
			if err := j.persistence.LoadState(j.Name(), clientId, &savedState); err != nil {
				log.Printf("action: q2_joiner_load_state | result: fail | client_id: %s | error: %v",
					clientId, err)
			} else if savedState.MenuItems != nil {
				j.mu.Lock()
				for key, item := range savedState.MenuItems {
					j.state.MenuItems[key] = item
				}
				j.mu.Unlock()
				log.Printf("action: q2_joiner_load_state | result: success | client_id: %s | items: %d",
					clientId, len(savedState.MenuItems))
			}
		}
	}

	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		switch aggRecord := record.(type) {
		case *protocol.Q2BestSellingRecord:
			// Join best selling data with menu item names
			menuItem, exists := j.state.MenuItems[aggRecord.ItemID]
			if !exists {
				log.Printf("action: q2_join_warning | client_id: %s | item_id: %s | error: menu_item_not_found", clientId, aggRecord.ItemID)
				continue // Skip records without matching menu items
			}

			joinedRecord := &protocol.Q2BestSellingWithNameRecord{
				YearMonth:   aggRecord.YearMonth,
				ItemName:    menuItem.ItemName,
				SellingsQty: aggRecord.SellingsQty,
			}
			joinedRecords = append(joinedRecords, joinedRecord)

			log.Printf("action: q2_join_best_selling | client_id: %s | item_id: %s | item_name: %s | qty: %s",
				clientId, aggRecord.ItemID, menuItem.ItemName, aggRecord.SellingsQty)

		case *protocol.Q2MostProfitsRecord:
			// Join most profitable data with menu item names
			menuItem, exists := j.state.MenuItems[aggRecord.ItemID]
			if !exists {
				log.Printf("action: q2_join_warning | client_id: %s | item_id: %s | error: menu_item_not_found", clientId, aggRecord.ItemID)
				continue // Skip records without matching menu items
			}

			joinedRecord := &protocol.Q2MostProfitsWithNameRecord{
				YearMonth: aggRecord.YearMonth,
				ItemName:  menuItem.ItemName,
				ProfitSum: aggRecord.ProfitSum,
			}
			joinedRecords = append(joinedRecords, joinedRecord)

			log.Printf("action: q2_join_most_profits | client_id: %s | item_id: %s | item_name: %s | profit: %s",
				clientId, aggRecord.ItemID, menuItem.ItemName, aggRecord.ProfitSum)

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

// Cleanup releases resources and deletes state file
func (j *Q2Joiner) Cleanup() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.persistence != nil && j.clientID != "" {
		if err := j.persistence.DeleteState(j.Name(), j.clientID); err != nil {
			log.Printf("action: q2_joiner_delete_state | result: fail | client_id: %s | error: %v",
				j.clientID, err)
		}
	}

	j.state.MenuItems = nil
	j.state = nil

	return nil
}
