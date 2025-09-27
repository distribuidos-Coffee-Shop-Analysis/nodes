import logging
import threading
from common.queue_manager import QueueManager
from .transaction_filter_handler import TransactionFilterHandler


class FilterNode:
    """Simple filter node that processes transactions from RabbitMQ queues"""
    
    def __init__(self):
        # Initialize queue manager for RabbitMQ communication
        self._queue_manager = QueueManager()

        # Transaction filter handler management
        self._transaction_filter_handler = None
        self._handlers_lock = threading.Lock()
        self._shutdown_requested = False

        logging.info("action: filter_node_init | result: success")

    def run(self):
        """Main filter node entry point - process transactions from RabbitMQ"""
        try:
            if not self._queue_manager.connect():
                logging.error(
                    "action: filter_node_startup | result: fail | error: Could not connect to RabbitMQ"
                )
                return

            # Start transaction filter handler
            self._start_transaction_filter_handler()

            logging.info("action: filter_node_started | result: success | msg: processing transactions...")
            
            # Keep the main thread alive while the filter handler processes
            try:
                while not self._shutdown_requested:
                    if self._transaction_filter_handler and self._transaction_filter_handler.is_alive():
                        self._transaction_filter_handler.join(timeout=1.0) # Verify every second if still alive
                    else:
                        break
            except KeyboardInterrupt:
                logging.info("action: filter_node_shutdown | result: requested")
                
        except KeyboardInterrupt:
            logging.info("action: filter_node_shutdown | result: in_progress")
            self._shutdown()
        except Exception as e:
            logging.error(f"action: filter_node_run | result: fail | error: {e}")
            self._shutdown()



    def _start_transaction_filter_handler(self):
        """Start the transaction filter handler"""
        try:
            self._transaction_filter_handler = TransactionFilterHandler(
                cleanup_callback=self._remove_filter_handler,
            )

            with self._handlers_lock:
                self._transaction_filter_handler.start()

            logging.info("action: transaction_filter_handler_started | result: success")
        except Exception as e:
            logging.error(
                f"action: start_transaction_filter_handler | result: fail | error: {e}"
            )

    def _remove_filter_handler(self, handler):
        """Remove the finished transaction filter handler"""
        try:
            with self._handlers_lock:
                if self._transaction_filter_handler == handler:
                    self._transaction_filter_handler = None
            logging.debug("action: remove_filter_handler | result: success")
        except Exception as e:
            logging.error(f"action: remove_filter_handler | result: fail | error: {e}")

    def _shutdown(self):
        """Graceful filter node shutdown"""
        self._shutdown_requested = True
        logging.info(
            "action: filter_node_shutdown | result: in_progress | msg: starting graceful shutdown"
        )

        # Shutdown transaction filter handler
        if (
            self._transaction_filter_handler
            and self._transaction_filter_handler.is_alive()
        ):
            try:
                self._transaction_filter_handler.request_shutdown()
                self._transaction_filter_handler.join(timeout=5.0)
                if self._transaction_filter_handler.is_alive():
                    logging.warning(
                        "action: filter_node_shutdown | result: fail | msg: filter handler didn't stop gracefully"
                    )
            except Exception as e:
                logging.error(
                    f"action: filter_node_shutdown | result: fail | msg: error shutting down filter handler | error: {e}"
                )

        # Disconnect from RabbitMQ
        try:
            self._queue_manager.disconnect()
        except Exception as e:
            logging.error(
                f"action: filter_node_shutdown | result: fail | msg: error disconnecting from RabbitMQ | error: {e}"
            )

        logging.info(
            "action: filter_node_shutdown | result: success | msg: graceful shutdown completed"
        )
