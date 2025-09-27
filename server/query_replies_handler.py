import logging
from threading import Thread
from common.queue_manager import QueueManager


class QueryRepliesHandler(Thread):
    """Handler for consuming query replies from RabbitMQ and sending them to clients"""

    def __init__(self, server_callbacks, cleanup_callback=None):
        """
        Initialize the query replies handler

        Args:
            server_callbacks: Dictionary with callback functions to server methods
            cleanup_callback: Function to call when handler finishes
        """
        super().__init__(daemon=True)
        self._server_callbacks = server_callbacks
        self._cleanup_callback = cleanup_callback
        self._shutdown_requested = False
        self._queue_manager = None

    def request_shutdown(self):
        """Request graceful shutdown of the handler"""
        self._shutdown_requested = True
        logging.info("action: query_replies_handler_shutdown | result: requested")

        # Stop consuming to unblock the handler
        if self._queue_manager:
            self._queue_manager.stop_consuming()

    def run(self):
        """Main handler loop for consuming query replies"""
        try:
            logging.info("action: query_replies_handler_start | result: success")

            self._queue_manager = QueueManager()

            if not self._queue_manager.connect():
                logging.error(
                    "action: query_replies_handler | result: fail | error: Could not connect to RabbitMQ"
                )
                return

            # Start consuming replies with callback (this blocks until shutdown)
            self._queue_manager.start_consuming_replies(self._handle_reply)

        except Exception as e:
            if not self._shutdown_requested:  # Only log as error if not shutting down
                logging.error(
                    f"action: query_replies_handler | result: fail | error: {e}"
                )
        finally:
            self._cleanup()

    def _handle_reply(self, dataset_type, records, eof):
        """
        Handle a single reply message from the replies queue

        Args:
            dataset_type: The type of dataset (Q1-Q4)
            records: List of record objects
            eof: End of file flag
        """
        if self._shutdown_requested:
            return

        try:
            logging.info(
                f"action: reply_received | result: success | "
                f"dataset_type: {dataset_type} | record_count: {len(records)} | eof: {eof}"
            )

            # TODO: Again, here we send replies to all clients, need to change to filter
            # Send reply to all connected clients via server callback
            if (
                self._server_callbacks
                and "send_reply_to_clients" in self._server_callbacks
            ):
                self._server_callbacks["send_reply_to_clients"](
                    dataset_type, records, eof
                )
                logging.info(
                    f"action: reply_forwarded_to_clients | result: success | "
                    f"dataset_type: {dataset_type} | record_count: {len(records)} | eof: {eof}"
                )

        except Exception as e:
            logging.error(f"action: handle_reply | result: fail | error: {e}")

    def _cleanup(self):
        """Cleanup resources and notify server"""
        try:
            if self._queue_manager:
                self._queue_manager.disconnect()

            if self._cleanup_callback:
                self._cleanup_callback(self)

            logging.info("action: query_replies_handler_cleanup | result: success")
        except Exception as e:
            logging.error(
                f"action: query_replies_handler_cleanup | result: fail | error: {e}"
            )
