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
	RoleFilterYear NodeRole = "filter_year"
	RoleFilterHour NodeRole = "filter_hour"
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
	InputExchange            = "transactions_and_transaction_items_exchange"
	TransactionsExchange     = "transactions_exchange"
	TransactionItemsExchange = "transaction_items_exchange"
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
			{Exchange: InputExchange, RoutingKey: ""},
		},
		Outputs: map[protocol.DatasetType]OutputRoute{
			protocol.DatasetTypeTransactions:     {Exchange: TransactionsExchange, RoutingKey: ""},
			protocol.DatasetTypeTransactionItems: {Exchange: TransactionItemsExchange, RoutingKey: ""},
		},
		DeclareExchs: []string{
			InputExchange, TransactionsExchange, TransactionItemsExchange,
		},
	}
}
