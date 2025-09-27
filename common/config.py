import configparser
import os
import logging


class Config:
    """Configuration manager for the filter node"""

    def __init__(self, config_path="config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self._load_config()

    def _load_config(self):
        """Load configuration from file"""
        try:
            if not os.path.exists(self.config_path):
                raise FileNotFoundError(
                    f"Configuration file not found: {self.config_path}"
                )

            self.config.read(self.config_path)
            logging.info(
                f"action: config_loaded | result: success | file: {self.config_path}"
            )

        except Exception as e:
            logging.error(f"action: config_load | result: fail | error: {e}")
            raise

    def get_rabbitmq_config(self):
        """Get RabbitMQ configuration"""
        return {
            "host": self.config.get("DEFAULT", "RABBITMQ_HOST", fallback="rabbitmq"),
            "port": self.config.getint("DEFAULT", "RABBITMQ_PORT", fallback=5672),
            "username": self.config.get("DEFAULT", "RABBITMQ_USER", fallback="admin"),
            "password": self.config.get(
                "DEFAULT", "RABBITMQ_PASSWORD", fallback="admin"
            ),
        }

    def get_filter_config(self):
        """Get filter node specific configuration"""
        input_queue = self.config.get(
            "DEFAULT", "INPUT_QUEUE", fallback="transactions_queue"
        )
        transactions_exchange = self.config.get(
            "DEFAULT", "TRANSACTIONS_EXCHANGE", fallback="transactions_exchange"
        )
        transaction_items_exchange = self.config.get(
            "DEFAULT",
            "TRANSACTION_ITEMS_EXCHANGE",
            fallback="transaction_items_exchange",
        )

        return {
            "input_queue": input_queue,
            "transactions_exchange": transactions_exchange,
            "transaction_items_exchange": transaction_items_exchange,
        }

    def get_logging_level(self):
        """Get logging level"""
        level_str = self.config.get("DEFAULT", "LOGGING_LEVEL", fallback="INFO")
        return getattr(logging, level_str.upper(), logging.INFO)


# Global configuration instance
_config_instance = None


def get_config():
    """Get the global configuration instance"""
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance


def reload_config():
    """Reload configuration from file"""
    global _config_instance
    _config_instance = Config()
    return _config_instance
