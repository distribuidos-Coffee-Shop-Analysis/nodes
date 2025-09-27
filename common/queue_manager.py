import pika
import logging
import json
from protocol.messages import DatasetType, _create_record_from_string
from .config import get_config


class QueueManager:
    """Manages RabbitMQ connections and queue operations for the filter node"""

    def __init__(self, host=None, port=None, username=None, password=None):
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

        # Configure queues from config file
        self.input_queue = filter_config["input_queue"]
        self.output_queues = filter_config["output_queues"]

        # Create input queue mapping
        self.input_queue_mapping = {
            DatasetType.TRANSACTIONS: self.input_queue,
            DatasetType.TRANSACTION_ITEMS: self.input_queue,
        }

        # Create output queue mapping based on business logic
        # TRANSACTIONS go to q1q3_queue and q4_queue
        # TRANSACTION_ITEMS go to q2_queue only
        self.output_queue_mapping = self._create_output_mapping()

        logging.info(
            f"action: queue_manager_init | input_queue: {self.input_queue} | "
            f"output_queues: {self.output_queues} | "
            f"output_mapping: {self.output_queue_mapping}"
        )

    def _create_output_mapping(self):
        """Create output queue mapping based on dataset types and configured queues"""
        q1q3_queue = None
        q2_queue = None
        q4_queue = None

        for queue_name in self.output_queues:
            if "q1q3" in queue_name.lower() or "q1_q3" in queue_name.lower():
                q1q3_queue = queue_name
            elif "q2" in queue_name.lower() and "q1" not in queue_name.lower():
                q2_queue = queue_name
            elif "q4" in queue_name.lower():
                q4_queue = queue_name

        output_mapping = {}

        # TRANSACTIONS -> q1q3_queue and q4_queue
        if q1q3_queue:
            output_mapping.setdefault(DatasetType.TRANSACTIONS, []).append(q1q3_queue)
        if q4_queue:
            output_mapping.setdefault(DatasetType.TRANSACTIONS, []).append(q4_queue)

        # TRANSACTION_ITEMS -> q2_queue only
        if q2_queue:
            output_mapping[DatasetType.TRANSACTION_ITEMS] = [q2_queue]

        if not output_mapping.get(DatasetType.TRANSACTIONS):
            logging.warning("No output queues found for TRANSACTIONS dataset")
        if not output_mapping.get(DatasetType.TRANSACTION_ITEMS):
            logging.warning("No output queues found for TRANSACTION_ITEMS dataset")

        return output_mapping

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

            # Declare all output queues
            for queue_name in self.output_queues:
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
        """Route dataset batch to appropriate input queue based on dataset type"""
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
                    # Parse the transaction message
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

    def send_filtered_batch(self, queue_name, dataset_type, records, eof):
        """Send filtered batch to specific output queue by name"""
        try:
            if queue_name not in self.output_queues:
                raise ValueError(
                    f"Queue '{queue_name}' is not in configured output queues: {self.output_queues}"
                )

            # Serialize the filtered batch message
            message_data = {
                "dataset_type": dataset_type,
                "records": [record.serialize() for record in records],
                "eof": eof,
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
                f"action: send_filtered_batch | result: success | queue: {queue_name} | "
                f"original_type: {dataset_type} | "
                f"record_count: {len(records)} | eof: {eof}"
            )

            return True

        except Exception as e:
            logging.error(f"action: send_filtered_batch | result: fail | error: {e}")
            return False

    def get_output_queues_for_dataset(self, dataset_type):
        """Get the list of output queues for a specific dataset type"""
        return self.output_queue_mapping.get(dataset_type, [])

    def send_to_dataset_output_queues(self, dataset_type, records, eof):
        """Send filtered batch to all appropriate output queues for a dataset type"""
        output_queues = self.get_output_queues_for_dataset(dataset_type)

        if not output_queues:
            logging.warning(
                f"action: send_to_dataset_output_queues | result: no_queues | "
                f"dataset_type: {dataset_type}"
            )
            return False

        success_count = 0
        total_queues = len(output_queues)

        for queue_name in output_queues:
            success = self.send_filtered_batch(queue_name, dataset_type, records, eof)
            if success:
                success_count += 1

        if success_count == total_queues:
            logging.info(
                f"action: send_to_dataset_output_queues | result: success | "
                f"dataset_type: {dataset_type} | queues_sent: {success_count}/{total_queues}"
            )
            return True
        else:
            logging.error(
                f"action: send_to_dataset_output_queues | result: partial_fail | "
                f"dataset_type: {dataset_type} | queues_sent: {success_count}/{total_queues}"
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
