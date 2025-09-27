import socket
import logging
import threading
from protocol.protocol import send_batch_message
from common.queue_manager import QueueManager
from .listener import Listener
from .query_replies_handler import QueryRepliesHandler


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)

        # Initialize queue manager for RabbitMQ communication
        self._queue_manager = QueueManager()

        # Client connections management
        self._client_connections_lock = threading.Lock()
        self._client_connections = (
            []
        )  # Store active client connections for sending replies

        # Query replies handler management
        self._query_replies_handler = None
        self._handlers_lock = threading.Lock()
        self._shutdown_requested = False

        # Server callbacks for handlers
        self._server_callbacks = {
            "handle_batch_message": self._handle_batch_message,
            "send_reply_to_clients": self._send_reply_to_clients,
            "add_client_connection": self.add_client_connection,
            "remove_client_connection": self.remove_client_connection,
        }

        logging.info(f"action: server_init | result: success | port: {port}")

    def run(self):
        """Main server entry point - setup queues and start listener"""
        try:
            # Connect to RabbitMQ
            if not self._queue_manager.connect():
                logging.error(
                    "action: server_startup | result: fail | error: Could not connect to RabbitMQ"
                )
                return

            # Start query replies handler
            self._start_query_replies_handler()

            # Create and start the listener
            listener = Listener(
                server_socket=self._server_socket,
                server_callbacks=self._server_callbacks,
            )

            # Start listening for connections
            listener.run()

        except KeyboardInterrupt:
            logging.info("action: server_shutdown | result: in_progress")
            self._shutdown()
        except Exception as e:
            logging.error(f"action: server_run | result: fail | error: {e}")
            self._shutdown()

    def _handle_batch_message(self, batch_message):
        """Handle dataset batch message - route to appropriate queue"""
        try:
            # Send the batch to the appropriate queue via RabbitMQ
            success = self._queue_manager.send_dataset_batch(batch_message)

            if success:
                logging.info(
                    f"action: dataset_received | result: success | "
                    f"dataset_type: {batch_message.dataset_type} | "
                    f"record_count: {len(batch_message.records)} | "
                    f"eof: {batch_message.eof}"
                )
            else:
                logging.error(
                    f"action: dataset_received | result: fail | dataset_type: {batch_message.dataset_type}"
                )

        except Exception as e:
            logging.error(f"action: handle_batch_message | result: fail | error: {e}")

    def _start_query_replies_handler(self):
        """Start the query replies handler"""
        try:
            self._query_replies_handler = QueryRepliesHandler(
                server_callbacks=self._server_callbacks,
                cleanup_callback=self._remove_query_handler,
            )

            with self._handlers_lock:
                self._query_replies_handler.start()

            logging.info("action: query_replies_handler_started | result: success")
        except Exception as e:
            logging.error(
                f"action: start_query_replies_handler | result: fail | error: {e}"
            )

    def _remove_query_handler(self, handler):
        """Remove the finished query replies handler"""
        try:
            with self._handlers_lock:
                if self._query_replies_handler == handler:
                    self._query_replies_handler = None
            logging.debug("action: remove_query_handler | result: success")
        except Exception as e:
            logging.error(f"action: remove_query_handler | result: fail | error: {e}")

    def _send_reply_to_clients(self, dataset_type, records, eof):
        """Send reply to all connected clients - callback for QueryRepliesHandler"""
        # TODO: This responds to all clients the replies from the queue, we need to filter
        # the client and send only to the one who made the request (include client_id task)
        with self._client_connections_lock:
            clients_to_remove = []
            for client_socket in self._client_connections[:]:
                try:
                    send_batch_message(client_socket, dataset_type, records, eof)
                    logging.info(
                        f"action: reply_sent | result: success | "
                        f"dataset_type: {dataset_type} | "
                        f"record_count: {len(records)} | "
                        f"eof: {eof}"
                    )
                except Exception as e:
                    logging.error(f"action: reply_sent | result: fail | error: {e}")
                    clients_to_remove.append(client_socket)

            # Remove disconnected clients
            for client in clients_to_remove:
                self._client_connections.remove(client)

    def add_client_connection(self, client_socket):
        """Add a client connection for receiving replies"""
        with self._client_connections_lock:
            self._client_connections.append(client_socket)

    def remove_client_connection(self, client_socket):
        """Remove a client connection"""
        with self._client_connections_lock:
            if client_socket in self._client_connections:
                self._client_connections.remove(client_socket)

    def _shutdown(self):
        """Graceful server shutdown"""
        self._shutdown_requested = True
        logging.info(
            "action: shutdown | result: in_progress | msg: starting graceful shutdown"
        )

        # Shutdown query replies handler
        if self._query_replies_handler and self._query_replies_handler.is_alive():
            try:
                self._query_replies_handler.request_shutdown()
                self._query_replies_handler.join()
                if self._query_replies_handler.is_alive():
                    logging.warning(
                        "action: shutdown | result: fail | msg: query handler didn't stop gracefully"
                    )
            except Exception as e:
                logging.error(
                    f"action: shutdown | result: fail | msg: error shutting down query handler | error: {e}"
                )

        # Close all client connections
        with self._client_connections_lock:
            for client_socket in self._client_connections[:]:
                try:
                    client_socket.close()
                except:
                    pass
            self._client_connections.clear()

        # Disconnect from RabbitMQ
        try:
            self._queue_manager.disconnect()
        except Exception as e:
            logging.error(
                f"action: shutdown | result: fail | msg: error disconnecting from RabbitMQ | error: {e}"
            )

        # Close server socket
        try:
            if self._server_socket:
                self._server_socket.close()
            logging.info(
                "action: shutdown | result: success | msg: server socket closed"
            )
        except Exception as e:
            logging.error(
                f"action: shutdown | result: fail | msg: error closing server socket | error: {e}"
            )

        logging.info(
            "action: shutdown | result: success | msg: graceful shutdown completed"
        )
