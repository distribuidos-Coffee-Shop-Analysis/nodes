import socket
import threading
import logging
import signal
from .client_handler import ClientHandler


class Listener:
    def __init__(self, server_socket, server_callbacks):
        """
        Initialize the listener

        Args:
            server_socket: The server socket to listen on
            server_callbacks: Dictionary with callback functions to server methods
        """
        self._server_socket = server_socket
        self._server_callbacks = server_callbacks

        self._shutdown_requested = False

        # Track active client handlers
        self._active_handlers = set()
        self._handlers_lock = (
            threading.Lock()
        )  # Protect concurrent access to _active_handlers

        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle SIGTERM signal for graceful shutdown"""
        logging.info("action: shutdown | result: in_progress | msg: received SIGTERM")
        self._shutdown_requested = True

        # Close server socket to stop accepting new connections
        try:
            self._server_socket.close()
            logging.info(
                "action: shutdown | result: success | msg: server socket closed"
            )
        except Exception as e:
            logging.error(
                f"action: shutdown | result: fail | msg: error closing server socket | error: {e}"
            )

    def run(self):
        """Main listener loop with graceful shutdown support and concurrent connection handling"""
        while not self._shutdown_requested:
            try:
                client_sock = self._accept_new_connection()
                if client_sock and not self._shutdown_requested:
                    # Create a new ClientHandler to handle this connection
                    client_handler = ClientHandler(
                        client_socket=client_sock,
                        client_address=client_sock.getpeername(),
                        server_callbacks=self._server_callbacks,
                        cleanup_callback=self._remove_handler,
                    )

                    # Track the handler
                    with self._handlers_lock:
                        self._active_handlers.add(client_handler)

                    # Start the handler thread
                    client_handler.start()

            except socket.error:
                logging.error(
                    "action: accept_connections | result: fail | error: socket error"
                )
            except Exception as e:
                logging.error(f"action: server_loop | result: fail | error: {e}")

        # Wait for all handlers to complete and final cleanup
        self._wait_for_handlers()
        logging.info(
            "action: shutdown | result: success | msg: graceful shutdown completed"
        )

    def _accept_new_connection(self):
        """Accept new connections (blocking call)"""
        logging.info("action: accept_connections | result: in_progress")
        c, addr = self._server_socket.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return c

    def _wait_for_handlers(self):
        """Wait for all active client handlers to complete"""
        logging.info(
            "action: shutdown | result: in_progress | msg: waiting for handlers to complete"
        )

        # Other threads could try to modify _active_handlers
        # so we need to create a copy of the handlers to
        # avoid iteration issues during shutdown and let them
        # finish naturally
        with self._handlers_lock:
            handlers_to_wait = list(self._active_handlers)

        # Then wait for them to complete
        for handler in handlers_to_wait:
            if handler.is_alive():
                try:
                    # Request shutdown for the handler
                    handler.request_shutdown()
                    handler.join()
                except Exception as e:
                    logging.error(
                        f"action: shutdown | result: fail | msg: error waiting for handler | error: {e}"
                    )

    def _remove_handler(self, handler):
        """Remove a finished handler from the active handlers"""
        try:
            with self._handlers_lock:
                self._active_handlers.discard(handler)
            logging.debug(
                f"action: remove_handler | result: success | ip: {handler.client_address[0]}"
            )
        except Exception as e:
            logging.error(f"action: remove_handler | result: fail | error: {e}")
