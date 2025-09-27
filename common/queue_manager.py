import pika
import logging
import json
from protocol.messages import DatasetType, _create_record_from_string


# TODO: Checkear esto bien
class QueueManager:
    """Manages RabbitMQ connections and queue operations for the connection node"""

    def __init__(self, host="rabbitmq", port=5672, username="admin", password="admin"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection = None
        self.channel = None

        # Queues known to connection node
        self.queue_names = {
            DatasetType.STORES: "joiner_n_queue_stores",
            DatasetType.TRANSACTION_ITEMS: "transactions_queue",
            DatasetType.TRANSACTIONS: "transactions_queue",
            DatasetType.USERS: "joiner_n_queue_users",
            DatasetType.MENU_ITEMS: "joiner_n_queue_menu_items",
            # Output queue for query responses
            "replies": "replies_queue",
        }

    def connect(self):
        """Establish connection to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host, port=self.port, credentials=credentials
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # Declare all queues
            for queue_name in self.queue_names.values():
                self.channel.queue_declare(queue=queue_name, durable=True)

            logging.info(
                f"action: rabbitmq_connect | result: success | host: {self.host}"
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
        """Route dataset batch to appropriate queue based on dataset type"""
        try:
            queue_name = self.queue_names.get(batch_message.dataset_type)
            if not queue_name:
                raise ValueError(
                    f"No queue mapping for dataset type: {batch_message.dataset_type}"
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

    def start_consuming_replies(self, callback):
        """Start consuming from replies queue and call callback for each message"""
        try:
            replies_queue = self.queue_names["replies"]

            def wrapper(ch, method, properties, body):
                try:
                    # Parse the reply message
                    message_data = json.loads(body.decode("utf-8"))
                    dataset_type = message_data["dataset_type"]
                    record_strings = message_data["records"]
                    eof = message_data.get("eof", False)

                    # Convert strings back to record objects
                    records = []
                    for record_string in record_strings:
                        record = _create_record_from_string(dataset_type, record_string)
                        records.append(record)

                    # Call the callback with the parsed data
                    callback(dataset_type, records, eof)

                    # Acknowledge the message
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    logging.error(f"action: process_reply | result: fail | error: {e}")
                    # Reject and requeue the message
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            self.channel.basic_consume(queue=replies_queue, on_message_callback=wrapper)

            logging.info(
                f"action: start_consuming | result: success | queue: {replies_queue}"
            )
            self.channel.start_consuming()

        except Exception as e:
            logging.error(f"action: start_consuming | result: fail | error: {e}")

    def stop_consuming(self):
        """Stop consuming messages"""
        try:
            if self.channel:
                self.channel.stop_consuming()
            logging.info("action: stop_consuming | result: success")
        except Exception as e:
            logging.error(f"action: stop_consuming | result: fail | error: {e}")
