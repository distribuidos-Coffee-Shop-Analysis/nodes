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

    def get_server_config(self):
        """Get server configuration"""
        return {
            "port": self.config.getint("DEFAULT", "SERVER_PORT", fallback=12345),
            "ip": self.config.get("DEFAULT", "SERVER_IP", fallback="localhost"),
            "listen_backlog": self.config.getint(
                "DEFAULT", "SERVER_LISTEN_BACKLOG", fallback=5
            ),
        }

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
        output_queues_str = self.config.get(
            "DEFAULT", "OUTPUT_QUEUES", fallback="q1_queue,q2_queue,q3_queue,q4_queue"
        )

        # Parse output queues (comma-separated list)
        output_queues = [
            queue.strip() for queue in output_queues_str.split(",") if queue.strip()
        ]

        return {
            "input_queue": input_queue,
            "output_queues": output_queues,
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
