import pika
import logging
import json
from protocol.messages import MESSAGE_TYPE_BATCH, BatchMessage, DatasetType, _create_record_from_string
from ..common.config import get_config
from middleware.middleware import (
    MessageMiddleware,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
)


class QueueManager(MessageMiddleware):
    """Manages RabbitMQ connections and queue operations for the filter node"""

    def __init__(
        self, host=None, queue_name=None, port=None, username=None, password=None
    ):
        # Load configuration
        config = get_config()
        rabbitmq_config = config.get_rabbitmq_config()
        filter_config = config.get_filter_config()

        # Use provided parameters or fall back to configuration
        self.host = host or rabbitmq_config["host"]
        self.port = port or rabbitmq_config["port"]
        self.username = username or rabbitmq_config["username"]
        self.password = password or rabbitmq_config["password"]
        self.connection = None
        self.channel = None
        self._consuming = False
        self._consumer_tag = None

        # Configure input queue and output exchanges from config file
        self.input_queue = filter_config["input_queue"]
        self.transactions_exchange = filter_config["transactions_exchange"]
        self.transaction_items_exchange = filter_config["transaction_items_exchange"]

        # Create input queue mapping
        self.input_queue_mapping = {
            DatasetType.TRANSACTIONS: self.input_queue,
            DatasetType.TRANSACTION_ITEMS: self.input_queue,
        }

        logging.info(
            f"action: queue_manager_init | input_queue: {self.input_queue} | "
            f"transactions_exchange: {self.transactions_exchange} | "
            f"transaction_items_exchange: {self.transaction_items_exchange} | "
        )

    def connect(self):
        """Establish connection to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host, port=self.port, credentials=credentials
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # Declare input queue
            self.channel.queue_declare(queue=self.input_queue, durable=True)

            # Declare all output exchanges
            if self.transactions_exchange:
                self.channel.exchange_declare(
                    exchange=self.transactions_exchange,
                    exchange_type="direct",  # Direct exchange for specific routing
                    durable=True,
                )
            if self.transaction_items_exchange:
                self.channel.exchange_declare(
                    exchange=self.transaction_items_exchange,
                    exchange_type="direct",  # Direct exchange for specific routing
                    durable=True,
                )

            logging.info(
                f"action: rabbitmq_connect | result: success | host: {self.host} | "
                f"input_queue: {self.input_queue} | transactions_exchange: {self.transactions_exchange} | "
                f"transaction_items_exchange: {self.transaction_items_exchange}"
            )
            return True

        except Exception as e:
            logging.error(f"action: rabbitmq_connect | result: fail | error: {e}")
            return False

    def disconnect(self):
        """Close RabbitMQ connection"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            logging.info("action: rabbitmq_disconnect | result: success")
        except Exception as e:
            logging.error(f"action: rabbitmq_disconnect | result: fail | error: {e}")

    def send_dataset_batch(self, batch_message):
        """Route dataset batch to appropriate input queue based on dataset type (for compatibility)"""
        try:
            queue_name = self.input_queue_mapping.get(batch_message.dataset_type)
            if not queue_name:
                raise ValueError(
                    f"No input queue mapping for dataset type: {batch_message.dataset_type}"
                )

            # Serialize the batch message for the queue
            message_data = {
                "dataset_type": batch_message.dataset_type,
                "records": [record.serialize() for record in batch_message.records],
                "eof": batch_message.eof,
            }

            message_body = json.dumps(message_data)

            self.channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2
                ),  # Make message persistent
            )

            logging.info(
                f"action: send_batch | result: success | queue: {queue_name} | "
                f"dataset_type: {batch_message.dataset_type} | "
                f"record_count: {len(batch_message.records)} | eof: {batch_message.eof}"
            )

            return True

        except Exception as e:
            logging.error(f"action: send_batch | result: fail | error: {e}")
            return False

    def start_consuming_transactions(self, callback):
        """Start consuming from configured input queue and call callback for each message"""
        try:
            input_queue = self.input_queue

            def wrapper(ch, method, properties, body):
                try:
                    msg_type = body[0]

                    if msg_type == MESSAGE_TYPE_BATCH:
                        batch_message = BatchMessage.from_data(body)
                    else:
                        raise ValueError(f"Unknown message type: {msg_type}")

                    # Call the callback with the parsed data
                    callback(batch_message)

                    # Acknowledge the message
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    logging.error(
                        f"action: process_transaction | result: fail | error: {e}"
                    )
                    # Reject and requeue the message
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            self.channel.basic_consume(queue=input_queue, on_message_callback=wrapper)

            logging.info(
                f"action: start_consuming_transactions | result: success | queue: {input_queue}"
            )
            self.channel.start_consuming()

        except Exception as e:
            logging.error(
                f"action: start_consuming_transactions | result: fail | error: {e}"
            )

    def encode_to_byte_array(batch_message):
        # [MessageType][DatasetType][EOF][RecordCount][Records...]
        data = bytearray()
        data.append(MESSAGE_TYPE_BATCH)
        data.append(batch_message.dataset_type)

        # Build content: EOF|RecordCount|Record1|Record2|...
        content = f"{1 if batch_message.eof else 0}|{len(batch_message.records)}"
        for record in batch_message.records:
            content += "|" + record.serialize()

        data.extend(content.encode("utf-8"))

        return data

    def send_filtered_batch(self, exchange_name, batch_message):
        """Send filtered batch to specific output exchange by name"""
        try:
            byte_array = self.encode_to_byte_array(batch_message)

            self.channel.basic_publish(
                exchange=exchange_name,
                routing_key="",  # No specific routing key needed for direct exchange
                body=byte_array,
                properties=pika.BasicProperties(
                    delivery_mode=2
                ),  # Make message persistent
            )

            logging.info(
                f"action: send_filtered_batch | result: success | exchange: {exchange_name} | "
                f"original_type: {batch_message.dataset_type} | "
                f"record_count: {len(batch_message.records)} | eof: {batch_message.eof}"
            )

            return True

        except Exception as e:
            logging.error(f"action: send_filtered_batch | result: fail | error: {e}")
            return False

    def send_to_dataset_output_exchanges(self, batch_message):
        """Send filtered batch to all appropriate output exchanges for a dataset type"""
        if batch_message.dataset_type == DatasetType.TRANSACTIONS:
            exchange = self.transactions_exchange
        elif batch_message.dataset_type == DatasetType.TRANSACTION_ITEMS:
            exchange = self.transaction_items_exchange
        else:
            logging.warning(
                f"action: send_to_dataset_output_exchanges | result: fail | "
                f"error: DATASET TYPE: {batch_message.dataset_type} NOT SUPPORTED"
            )
            return False

        success = self.send_filtered_batch(
            exchange, batch_message
        )
        if success:
            logging.info(
                f"action: send_to_dataset_output_exchanges | result: success | "
                f"dataset_type: {batch_message.dataset_type}"
            )
            return True
        else:
            logging.error(
                f"action: send_to_dataset_output_exchanges | result: partial_fail | "
                f"dataset_type: {batch_message.dataset_type}"
            )
            return False

    def stop_consuming(self):
        """Stop consuming messages"""
        try:
            if self.channel:
                self.channel.stop_consuming()
            logging.info("action: stop_consuming | result: success")
        except Exception as e:
            logging.error(f"action: stop_consuming | result: fail | error: {e}")

    # MessageMiddleware interface methods
    def start_consuming(self, callback):
        """Start consuming messages - delegates to start_consuming_transactions"""
        self.start_consuming_transactions(callback)

    def send(self, message):
        """Send message to default transactions exchange (MessageMiddleware interface)"""
        try:
            self.channel.basic_publish(
                exchange=self.transactions_exchange,  
                routing_key="",
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2
                ),  # Make message persistent
            )
            return True
        except Exception as e:
            logging.error(f"action: send | result: fail | error: {e}")
            return False

    def close(self):
        """Close the connection"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            logging.info("action: close | result: success")
        except Exception as e:
            logging.error(f"action: close | result: fail | error: {e}")

    def delete(self, exchange_name=None):
        """Delete an exchange"""
        try:
            if self.channel:
                # Use provided exchange or default to transactions_exchange
                target_exchange = exchange_name or self.transactions_exchange
                self.channel.exchange_delete(exchange=target_exchange)
            logging.info(
                f"action: delete | result: success | exchange: {target_exchange}"
            )
        except Exception as e:
            logging.error(
                f"action: delete | result: fail | exchange: {target_exchange} | error: {e}"
            )
