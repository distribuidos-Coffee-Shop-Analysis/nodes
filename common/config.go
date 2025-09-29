package common

import (
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
	section := c.cfg.Section("DEFAULT")

	return &NodeConfig{
		NodeID: section.Key("NODE_ID").MustString("default-01"),
		Role:   NodeRole(section.Key("NODE_ROLE").MustString("filter_year")),
	}
}

// GetLoggingLevel returns the logging level as a string
func (c *Config) GetLoggingLevel() string {
	section := c.cfg.Section("DEFAULT")
	return section.Key("LOGGING_LEVEL").MustString("INFO")
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
	Exchange   string
	RoutingKey string
}

type OutputRoute struct {
	Exchange   string
	RoutingKey string
}

const (
	// Exchanges
	
	// Filer by year
	InputExchangeYear         = "transactions_and_transaction_items_exchange"
	TransactionsExchange     = "transactions_exchange"
	TransactionItemsExchange = "transaction_items_exchange"

	// Filter by hour
	InputExchangeHour = TransactionsExchange
	TransactionsFilteredHourExchange  = "transactions_filtered_hour_exchange"

	// Filter by amount
	InputExchangeAmount = TransactionsFilteredHourExchange 
	RepliesExchange = "replies_exchange" // finish Q1

	// Q2
	InputQ2Exchange = TransactionItemsExchange
	Q2GroupedExchange = "q2_grouped_exchange"
	Q2AggregatedExchange = "q2_aggregated_exchange"
	Q2JoinExchange = "q2_joined_exchange"

	// Q3
	InputQ3Exchange = TransactionsFilteredHourExchange
	Q3GroupedExchange = "q3_grouped_exchange"
	Q3AggregatedExchange = "q3_aggregated_exchange"
	Q3JoinExchange = "q3_joined_exchange"

	// Q4
	InputQ4Exchange = TransactionsExchange
	Q4GroupedExchange = "q4_grouped_exchange"
	Q4AggregatedExchange = "q4_aggregated_exchange"
	Q4JoinUsersExchange = "q4_joined_users_exchange"
	Q4JoinStoresExchange = "q4_joined_stores_exchange"
)

type NodeWiring struct {
	Role         NodeRole
	NodeID       string
	QueueName    string                               // se calcula role.nodeID
	Bindings     []Binding                            // de dónde leo
	Outputs      map[protocol.DatasetType]OutputRoute // a dónde publico por dataset
	DeclareExchs []string                             // exchanges a declarar (durable)
}

func BuildWiringForFilterYear(role NodeRole, nodeID string) *NodeWiring {
	return &NodeWiring{
		Role:      role,
		NodeID:    nodeID,
		QueueName: string(role) + "." + nodeID, // e.g. "filter_year.01"
		Bindings: []Binding{
			{Exchange: InputExchangeYear, RoutingKey: ""},
		},
		Outputs: map[protocol.DatasetType]OutputRoute{
			protocol.DatasetTypeTransactions:     {Exchange: TransactionsExchange, RoutingKey: ""},
			protocol.DatasetTypeTransactionItems: {Exchange: TransactionItemsExchange, RoutingKey: ""},
		},
		DeclareExchs: []string{
			InputExchangeYear, TransactionsExchange, TransactionItemsExchange,
		},
	}
}

func BuildWiringForFilterHour(role NodeRole, nodeID string) *NodeWiring {
	return &NodeWiring{
		Role:      role,
		NodeID:    nodeID,
		QueueName: string(role) + "." + nodeID, // e.g. "filter_hour.01"
		Bindings: []Binding{
			{Exchange: InputExchangeHour, RoutingKey: ""},
		},
		Outputs: map[protocol.DatasetType]OutputRoute{
			protocol.DatasetTypeTransactions:     {Exchange: TransactionsFilteredHourExchange, RoutingKey: ""},
		},
		DeclareExchs: []string{
			InputExchangeHour, TransactionsFilteredHourExchange,
		},
	}
}

func BuildWiringForFilterAmount(nodeID string) *NodeWiring {
	return &NodeWiring{
		Role:      RoleFilterAmount,
		NodeID:    nodeID,
		QueueName: string(RoleFilterAmount) + "." + nodeID, // e.g. "filter_amount.01"
		Bindings: []Binding{
			{Exchange: InputExchangeAmount, RoutingKey: ""},
		},
		Outputs: map[protocol.DatasetType]OutputRoute{
			protocol.DatasetTypeTransactions:     {Exchange: RepliesExchange, RoutingKey: ""},
		},
		DeclareExchs: []string{
			InputExchangeAmount, RepliesExchange,
		},
	}
}

// Q2
func BuildWiringForGroupByQ2(role NodeRole, nodeID string) *NodeWiring {
	return &NodeWiring{
		Role:      role,
		NodeID:    nodeID,
		QueueName: string(role) + "." + nodeID, // e.g. "filter_year.01"
		Bindings: []Binding{
			{Exchange: InputQ2Exchange, RoutingKey: ""},
		},
		Outputs: map[protocol.DatasetType]OutputRoute{
			protocol.DatasetTypeTransactionItems:     {Exchange: Q2GroupedExchange, RoutingKey: ""},
		},
		DeclareExchs: []string{
			InputQ2Exchange, Q2GroupedExchange,
		},
	}
}

func BuildWiringForAggregateQ2(role NodeRole, nodeID string) *NodeWiring {
	return &NodeWiring{
		Role:      role,
		NodeID:    nodeID,
		QueueName: string(role) + "." + nodeID, // e.g. "filter_year.01"
		Bindings: []Binding{
			{Exchange: InputQ2Exchange, RoutingKey: ""},
		},
		Outputs: map[protocol.DatasetType]OutputRoute{
			protocol.DatasetTypeTransactionItems:     {Exchange: Q2GroupedExchange, RoutingKey: ""},
		},
		DeclareExchs: []string{
			InputQ2Exchange, Q2GroupedExchange,
		},
	}
}