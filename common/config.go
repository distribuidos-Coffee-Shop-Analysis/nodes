package common

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"gopkg.in/ini.v1"
)

// RabbitmqConfig holds RabbitMQ configuration
type RabbitmqConfig struct {
	Host     string
	Port     int
	Username string
	Password string
}

// FilterConfig holds filter node specific configuration
type FilterConfig struct {
	InputQueue               string
	TransactionsExchange     string
	TransactionItemsExchange string
}

// Config represents the configuration manager for the filter node
type Config struct {
	configPath string
	cfg        *ini.File
}

type NodeConfig struct {
	NodeID string
	Role   NodeRole
}

// Global configuration instance with thread safety
var (
	configInstance *Config
	configOnce     sync.Once
)

// NewConfig creates a new Config instance
func NewConfig(configPath ...string) *Config {
	path := "config.ini"
	if len(configPath) > 0 && configPath[0] != "" {
		path = configPath[0]
	}

	config := &Config{
		configPath: path,
	}

	config.loadConfig()
	return config
}

// loadConfig loads configuration from file
func (c *Config) loadConfig() {
	// Check if file exists
	if _, err := os.Stat(c.configPath); os.IsNotExist(err) {
		log.Fatalf("action: config_load | result: fail | error: Configuration file not found: %s", c.configPath)
	}

	// Load INI file
	cfg, err := ini.Load(c.configPath)
	if err != nil {
		log.Fatalf("action: config_load | result: fail | error: %v", err)
	}

	c.cfg = cfg
	log.Printf("action: config_loaded | result: success | file: %s", c.configPath)
}

// GetRabbitmqConfig returns RabbitMQ configuration
func (c *Config) GetRabbitmqConfig() *RabbitmqConfig {
	section := c.cfg.Section("DEFAULT")

	return &RabbitmqConfig{
		Host:     section.Key("RABBITMQ_HOST").MustString("rabbitmq"),
		Port:     section.Key("RABBITMQ_PORT").MustInt(5672),
		Username: section.Key("RABBITMQ_USER").MustString("admin"),
		Password: section.Key("RABBITMQ_PASSWORD").MustString("admin"),
	}
}

// GetNodeConfig returns the node configuration
func (c *Config) GetNodeConfig() *NodeConfig {

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		log.Fatalf("action: config_load | result: fail | error: NODE_ID environment variable is required")
	}

	nodeRole := os.Getenv("NODE_ROLE")
	if nodeRole == "" {
		log.Fatalf("action: config_load | result: fail | error: NODE_ROLE environment variable is required")
	}

	return &NodeConfig{
		NodeID: nodeID,
		Role:   NodeRole(nodeRole),
	}
}

// GetLoggingLevel returns the logging level as a string
func (c *Config) GetLoggingLevel() string {
	section := c.cfg.Section("DEFAULT")
	return section.Key("LOGGING_LEVEL").MustString("INFO")
}

// GetQ4JoinersCount returns the number of Q4 joiner nodes for partitioning
func (c *Config) GetQ4JoinersCount() int {
	section := c.cfg.Section("DEFAULT")
	return section.Key("Q4_JOINERS_COUNT").MustInt(1)
}

// GetWorkerAmount returns the number of workers for a given node role
func (c *Config) GetWorkerAmount(role NodeRole) int {
	section := c.cfg.Section("DEFAULT")

	switch role {
	// Aggregates
	case RoleAggregateQ2, RoleAggregateQ3, RoleAggregateQ4:
		return section.Key("AGGREGATE_WORKER_AMOUNT").MustInt(300)

	// Q4 User Joiner
	case RoleJoinerQ4U:
		return section.Key("JOINER_Q4_USERS_WORKER_AMOUNT").MustInt(300)

	// Simple Joiners (Q2, Q3, Q4 Stores)
	case RoleJoinerQ2, RoleJoinerQ3, RoleJoinerQ4S:
		return section.Key("SIMPLE_JOINER_WORKER_AMOUNT").MustInt(2)

	// Filters and GroupBys
	case RoleFilterYear, RoleFilterHour, RoleFilterAmount,
		RoleGroupByQ2, RoleGroupByQ3, RoleGroupByQ4:
		return section.Key("FILTER_AND_GROUPBY_WORKER_AMOUNT").MustInt(500)

	default:
		// Default fallback
		return section.Key("FILTER_AND_GROUPBY_WORKER_AMOUNT").MustInt(500)
	}
}

// GetConfig returns the global configuration instance (singleton pattern)
func GetConfig() *Config {
	configOnce.Do(func() {
		configInstance = NewConfig()
	})
	return configInstance
}

// ReloadConfig reloads configuration from file
func ReloadConfig() *Config {
	// Reset the sync.Once to allow reinitialization
	configOnce = sync.Once{}
	configInstance = nil
	return GetConfig()
}

// SetConfigPath allows setting a custom config path for testing
func SetConfigPath(path string) {
	configOnce = sync.Once{}
	configInstance = nil
	configOnce.Do(func() {
		configInstance = NewConfig(path)
	})
}

type NodeRole string

const (
	RoleFilterYear   NodeRole = "filter_year"
	RoleFilterHour   NodeRole = "filter_hour"
	RoleFilterAmount NodeRole = "filter_amount"
	RoleGroupByQ2    NodeRole = "q2_group"
	RoleGroupByQ3    NodeRole = "q3_group"
	RoleGroupByQ4    NodeRole = "q4_group"
	RoleAggregateQ2  NodeRole = "q2_aggregate"
	RoleAggregateQ3  NodeRole = "q3_aggregate"
	RoleAggregateQ4  NodeRole = "q4_aggregate"
	RoleJoinerQ2     NodeRole = "q2_join"
	RoleJoinerQ3     NodeRole = "q3_join"
	RoleJoinerQ4U    NodeRole = "q4_join_users"
	RoleJoinerQ4S    NodeRole = "q4_join_stores"
)

type Binding struct {
	Exchange       string
	RoutingKey     string
	UseSharedQueue bool `json:"use_shared_queue"`
}

type OutputRoute struct {
	Exchange   string
	RoutingKey string
}

type NodeWiring struct {
	Role                NodeRole
	NodeID              string
	SharedQueueName     string                               // Queue name for shared consumption (use_shared_queue=true)
	IndividualQueueName string                               // Queue name for individual consumption (use_shared_queue=false), format: role.nodeID
	Bindings            []Binding                            // de dónde leo
	Outputs             map[protocol.DatasetType]OutputRoute // a dónde publico por dataset
	DeclareExchs        []string                             // exchanges a declarar
}

// JSON configuration for node wiring
type WiringConfig struct {
	Role             string                 `json:"role"`
	SharedQueueName  string                 `json:"shared_queue_name"` // Queue name for shared consumption (optional)
	Bindings         []Binding              `json:"bindings"`
	Outputs          map[string]OutputRoute `json:"outputs"`
	DeclareExchanges []string               `json:"declare_exchanges"`
}

// creates NodeWiring from JSON configuration
func BuildWiringFromConfig(configPath string, nodeID string) (*NodeWiring, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("error reading wiring config file %s: %v", configPath, err)
	}

	var config WiringConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error parsing wiring config JSON: %v", err)
	}

	outputs := make(map[protocol.DatasetType]OutputRoute)
	for datasetTypeStr, route := range config.Outputs {
		datasetType := getDatasetTypeFromString(datasetTypeStr)
		outputs[datasetType] = route
	}

	// Shared queue name (for use_shared_queue=true bindings)
	sharedQueueName := config.SharedQueueName

	// Individual queue name (for use_shared_queue=false bindings)
	individualQueueName := config.Role + "." + nodeID

	// Special handling for Q4 User Joiner: generate dynamic routing key based on NODE_ID
	bindings := config.Bindings
	if config.Role == "q4_join_users" {
		partition := NodeIDToPartition(nodeID)
		if partition != -1 {
			// Update bindings with dynamic routing keys
			// For each binding, set appropriate routing key based on exchange
			updatedBindings := make([]Binding, len(bindings))
			for i, binding := range bindings {
				updatedBindings[i] = binding
				// Set routing key based on which exchange this binding is for
				switch binding.Exchange {
				case "users_exchange":
					// For users_exchange: joiner.{partition}.users
					updatedBindings[i].RoutingKey = fmt.Sprintf("joiner.%d.users", partition)
					log.Printf("action: build_wiring | role: %s | node_id: %s | partition: %d | exchange: %s | routing_key: %s",
						config.Role, nodeID, partition, binding.Exchange, updatedBindings[i].RoutingKey)
				case "q4_aggregated_exchange":
					// For q4_aggregated_exchange: joiner.{partition}.q4_agg
					updatedBindings[i].RoutingKey = BuildQ4UserJoinerRoutingKey(partition)
					log.Printf("action: build_wiring | role: %s | node_id: %s | partition: %d | exchange: %s | routing_key: %s",
						config.Role, nodeID, partition, binding.Exchange, updatedBindings[i].RoutingKey)
				default:
					// Keep original routing key for other exchanges
					log.Printf("action: build_wiring | role: %s | node_id: %s | partition: %d | exchange: %s | routing_key: %s (unchanged)",
						config.Role, nodeID, partition, binding.Exchange, updatedBindings[i].RoutingKey)
				}
			}
			bindings = updatedBindings
		} else {
			log.Printf("action: build_wiring | role: %s | node_id: %s | result: fail | error: invalid NODE_ID for partition",
				config.Role, nodeID)
		}
	}

	return &NodeWiring{
		Role:                NodeRole(config.Role),
		NodeID:              nodeID,
		SharedQueueName:     sharedQueueName,
		IndividualQueueName: individualQueueName,
		Bindings:            bindings,
		Outputs:             outputs,
		DeclareExchs:        config.DeclareExchanges,
	}, nil
}

// getDatasetTypeFromString converts string to DatasetType
func getDatasetTypeFromString(datasetTypeStr string) protocol.DatasetType {
	switch datasetTypeStr {
	case "TRANSACTIONS":
		return protocol.DatasetTypeTransactions
	case "TRANSACTION_ITEMS":
		return protocol.DatasetTypeTransactionItems
	case "MENU_ITEMS":
		return protocol.DatasetTypeMenuItems
	case "STORES":
		return protocol.DatasetTypeStores
	case "USERS":
		return protocol.DatasetTypeUsers
	case "Q1":
		return protocol.DatasetTypeQ1
	case "Q2Groups":
		return protocol.DatasetTypeQ2Groups
	case "Q2Agg":
		return protocol.DatasetTypeQ2Agg
	case "Q2AggWithName":
		return protocol.DatasetTypeQ2AggWithName
	case "Q3Groups":
		return protocol.DatasetTypeQ3Groups
	case "Q3Agg":
		return protocol.DatasetTypeQ3Agg
	case "Q3AggWithName":
		return protocol.DatasetTypeQ3AggWithName
	case "Q4Groups":
		return protocol.DatasetTypeQ4Groups
	case "Q4Agg":
		return protocol.DatasetTypeQ4Agg
	case "Q4AggWithUser":
		return protocol.DatasetTypeQ4AggWithUser
	case "Q4AggWithUserAndStore":
		return protocol.DatasetTypeQ4AggWithUserAndStore
	default:
		log.Printf("action: convert_dataset_type | result: warning | unknown_type: %s", datasetTypeStr)
		return protocol.DatasetTypeTransactions
	}
}
