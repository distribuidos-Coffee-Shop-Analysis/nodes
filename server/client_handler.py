import socket
import logging
from threading import Thread
from protocol.protocol import (
    read_packet_from,
    send_response,
    MESSAGE_TYPE_BATCH,
)


class ClientHandler(Thread):
    def __init__(
        self,
        client_socket,
        client_address,
        server_callbacks,
        cleanup_callback=None,
    ):
        """
        Initialize the client handler

        Args:
            client_socket: The client socket connection
            client_address: The client address tuple (ip, port)
            server_callbacks: Dictionary with callback functions to server methods:
                - handle_batch_message: callback for batch messages
                - add_client_connection: callback to register client connection
                - remove_client_connection: callback to unregister client connection
            cleanup_callback: Optional callback function to call when handler finishes
                             Should accept (handler_instance) as parameter
        """
        super().__init__(daemon=True)
        self.client_socket = client_socket
        self.client_address = client_address
        self.server_callbacks = server_callbacks
        self.cleanup_callback = cleanup_callback

        self._shutdown_requested = False

    def request_shutdown(self):
        """Request graceful shutdown of this handler"""
        self._shutdown_requested = True
        try:
            self.client_socket.shutdown(
                socket.SHUT_RDWR
            )  # First notify the client about the disconnection
            self.client_socket.close()
        except Exception as e:
            self._log_action("close_connection", "fail", level=logging.ERROR, error=e)

    def run(self):
        """Handle persistent communication with a client"""
        try:
            # Register this client connection for receiving replies
            if "add_client_connection" in self.server_callbacks:
                self.server_callbacks["add_client_connection"](self.client_socket)

            self._handle_client_communication()
        finally:
            # Unregister the client connection
            if "remove_client_connection" in self.server_callbacks:
                self.server_callbacks["remove_client_connection"](self.client_socket)

            self._cleanup_connection()
            # Call cleanup callback to notify listener this handler is done
            if self.cleanup_callback:
                try:
                    self.cleanup_callback(self)
                except Exception as e:
                    self._log_action(
                        "cleanup_callback", "fail", level=logging.ERROR, error=e
                    )

    def _handle_client_communication(self):
        """Main communication loop with the client"""
        while not self._shutdown_requested:
            try:
                message = read_packet_from(self.client_socket)

                session_completed = self._process_message(message)
                if session_completed:
                    break

            except socket.error as e:
                self._log_action("socket_error", "connection_closed", error=e)
                break
            except ValueError as e:
                self._log_action(
                    "receive_message", "fail", level=logging.ERROR, error=e
                )
                self._send_error_response(str(e))
            except Exception as e:
                self._log_action(
                    "receive_message", "fail", level=logging.ERROR, error=e
                )
                self._send_error_response("Internal server error")

    def _process_message(self, message):
        """
        Process a received message and return whether the session is complete.

        Returns:
            bool: True if the client session is complete, False otherwise
        """
        if message.type == MESSAGE_TYPE_BATCH:
            self.server_callbacks["handle_batch_message"](message)
            return False
        else:
            self._log_action(
                "process_message",
                "fail",
                level=logging.ERROR,
                error=f"Unknown message type: {message.type}",
            )
            return False

    def _log_action(
        self, action, result, level=logging.INFO, error=None, extra_fields=None
    ):
        """
        Centralized logging function for consistent log format

        Args:
            action: The action being performed
            result: The result of the action (success, fail, etc.)
            level: Logging level (INFO, ERROR, DEBUG, etc.)
            error: Optional error information
            extra_fields: Optional dict with additional fields to log
        """
        log_parts = [
            f"action: {action}",
            f"result: {result}",
            f"ip: {self.client_address[0]}",
        ]

        if error:
            log_parts.append(f"error: {error}")

        if extra_fields:
            for key, value in extra_fields.items():
                log_parts.append(f"{key}: {value}")

        log_message = " | ".join(log_parts)
        logging.log(level, log_message)

    def _send_error_response(self, error_message):
        """Send error response to client"""
        try:
            send_response(self.client_socket, success=False, error=error_message)
        except Exception as e:
            self._log_action(
                "send_error_response", "fail", level=logging.ERROR, error=e
            )

    def _cleanup_connection(self):
        """Clean up client connection"""
        try:
            self.client_socket.shutdown(
                socket.SHUT_RDWR
            )  # First notify the client about the disconnection
            self.client_socket.close()
        except Exception as e:
            # Socket possibly already closed
            self._log_action(
                "cleanup_connection", "already_closed", level=logging.DEBUG
            )
