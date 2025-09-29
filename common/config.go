package common

import (
	"log"
	"os"
	"sync"

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

// ServerConfig holds server configuration
type ServerConfig struct {
	Port          int
	IP            string
	ListenBacklog int
}

// Config represents the configuration manager for the filter node
type Config struct {
	configPath string
	cfg        *ini.File
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

// GetFilterConfig returns filter node specific configuration
func (c *Config) GetFilterConfig() *FilterConfig {
	section := c.cfg.Section("DEFAULT")

	return &FilterConfig{
		InputQueue:               section.Key("INPUT_QUEUE").MustString("transactions_queue"),
		TransactionsExchange:     section.Key("TRANSACTIONS_EXCHANGE").MustString("transactions_exchange"),
		TransactionItemsExchange: section.Key("TRANSACTION_ITEMS_EXCHANGE").MustString("transaction_items_exchange"),
	}
}

// GetServerConfig returns server configuration
func (c *Config) GetServerConfig() *ServerConfig {
	section := c.cfg.Section("DEFAULT")

	return &ServerConfig{
		Port:          section.Key("SERVER_PORT").MustInt(12345),
		IP:            section.Key("SERVER_IP").MustString("connection-node"),
		ListenBacklog: section.Key("SERVER_LISTEN_BACKLOG").MustInt(5),
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
