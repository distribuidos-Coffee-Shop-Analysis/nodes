import logging
from threading import Thread
from middleware.queue_manager import QueueManager
from protocol.messages import DatasetType


class TransactionFilterHandler(Thread):
    """Handler for consuming transactions from RabbitMQ, filtering by years 2024-2025, and routing to output queues"""

    def __init__(self, cleanup_callback=None):
        """
        Initialize the transaction filter handler

        Args:
            cleanup_callback: Function to call when handler finishes
        """
        super().__init__(daemon=True)
        self._cleanup_callback = cleanup_callback
        self._shutdown_requested = False
        self._queue_manager = None

        # Filter configuration
        self._min_year = 2024
        self._max_year = 2025

    def request_shutdown(self):
        """Request graceful shutdown of the handler"""
        self._shutdown_requested = True
        logging.info("action: transaction_filter_handler_shutdown | result: requested")

        # Stop consuming to unblock the handler
        if self._queue_manager:
            self._queue_manager.stop_consuming()

    def run(self):
        """Main handler loop for consuming and filtering transactions"""
        try:
            logging.info("action: transaction_filter_handler_start | result: success")

            self._queue_manager = QueueManager()

            if not self._queue_manager.connect():
                logging.error(
                    "action: transaction_filter_handler | result: fail | error: Could not connect to RabbitMQ"
                )
                return

            # Start consuming transactions with callback (this blocks until shutdown)
            self._queue_manager.start_consuming(self._handle_transaction_batch)

        except Exception as e:
            if not self._shutdown_requested:  # Only log as error if not shutting down
                logging.error(
                    f"action: transaction_filter_handler | result: fail | error: {e}"
                )
        finally:
            self._cleanup()

    def _handle_transaction_batch(self, dataset_type, records, eof):
        """
        Handle a transaction batch - filter by year and route to output queues

        Args:
            dataset_type: The type of dataset (should be TRANSACTIONS or TRANSACTION_ITEMS)
            records: List of record objects
            eof: End of file flag
        """
        if self._shutdown_requested:
            return

        try:
            # Validate dataset type - only process TRANSACTIONS and TRANSACTION_ITEMS
            if dataset_type not in [
                DatasetType.TRANSACTIONS,
                DatasetType.TRANSACTION_ITEMS,
            ]:
                logging.info(
                    f"action: batch_dropped | result: success | "
                    f"dataset_type: {dataset_type} | reason: invalid_dataset_type | "
                    f"record_count: {len(records)}"
                )
                return

            logging.info(
                f"action: transaction_batch_received | result: success | "
                f"dataset_type: {dataset_type} | record_count: {len(records)} | eof: {eof}"
            )

            # Filter records by year (2024-2025)
            filtered_records = self._filter_records_by_year(records)

            if len(filtered_records) != len(records):
                logging.info(
                    f"action: records_filtered | result: success | "
                    f"original_count: {len(records)} | filtered_count: {len(filtered_records)} | "
                    f"dropped: {len(records) - len(filtered_records)}"
                )

            # Send filtered records to output queues
            if (
                filtered_records or eof
            ):  # Send even empty batches if EOF to signal completion
                self._route_to_output_exchanges(dataset_type, filtered_records, eof)

        except Exception as e:
            logging.error(
                f"action: handle_transaction_batch | result: fail | error: {e}"
            )

    def _filter_records_by_year(self, records):
        """
        Filter records based on created_at field being between 2024-2025

        Args:
            records: List of record objects

        Returns:
            List of filtered records
        """
        filtered_records = []

        for record in records:
            try:
                # Extract year from created_at field
                created_at = record.created_at

                # Parse the date - assuming format like "2024-01-15T10:30:00" or "2024-01-15"
                if "T" in created_at:
                    date_part = created_at.split("T")[0]
                else:
                    date_part = created_at.split(" ")[
                        0
                    ]  # Handle "YYYY-MM-DD HH:MM:SS" format

                year = int(date_part.split("-")[0])

                # Filter by year range
                if self._min_year <= year <= self._max_year:
                    filtered_records.append(record)
                else:
                    logging.debug(
                        f"action: record_filtered | year: {year} | "
                        f"transaction_id: {getattr(record, 'transaction_id', 'unknown')}"
                    )

            except (ValueError, AttributeError, IndexError) as e:
                logging.warning(
                    f"action: record_parse_error | result: dropped | "
                    f"transaction_id: {getattr(record, 'transaction_id', 'unknown')} | "
                    f"created_at: {getattr(record, 'created_at', 'unknown')} | error: {e}"
                )
                # Drop records with invalid dates
                continue

        return filtered_records

    def _route_to_output_exchanges(self, dataset_type, records, eof):
        """
        Route filtered records to the appropriate output queues based on dataset type

        Args:
            dataset_type: Original dataset type (TRANSACTIONS -> q1q3_queue + q4_queue, TRANSACTION_ITEMS -> q2_queue)
            records: Filtered records
            eof: End of file flag
        """
        try:
            # Use the dataset-specific routing from QueueManager
            success = self._queue_manager.send_to_dataset_output_exchanges(
                dataset_type, records, eof
            )

            if success:
                logging.info(
                    f"action: batch_routed | result: success | "
                    f"dataset_type: {dataset_type} | "
                    f"record_count: {len(records)} | eof: {eof}"
                )
            else:
                logging.error(
                    f"action: batch_routed | result: fail | "
                    f"dataset_type: {dataset_type}"
                )

        except Exception as e:
            logging.error(
                f"action: route_to_output_exchanges | result: fail | error: {e}"
            )

    def _cleanup(self):
        """Cleanup resources and notify server"""
        try:
            if self._queue_manager:
                self._queue_manager.close()

            if self._cleanup_callback:
                self._cleanup_callback(self)

            logging.info("action: transaction_filter_handler_cleanup | result: success")
        except Exception as e:
            logging.error(
                f"action: transaction_filter_handler_cleanup | result: fail | error: {e}"
            )
