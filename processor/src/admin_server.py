import json
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class AdminHandler(BaseHTTPRequestHandler):
    """HTTP handler for admin commands."""
    
    flush_callback: Optional[Callable[[], dict]] = None
    status_callback: Optional[Callable[[], dict]] = None
    
    def log_message(self, format, *args):
        # Route HTTP logs through the standard logger
        logger.debug("%s - %s", self.address_string(), format % args)
    
    def _send_json(self, data: dict, status: int = 200):
        # Send JSON response
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    
    def do_GET(self):
        # Handle GET requests
        if self.path == "/admin/status":
            if self.status_callback:
                result = self.status_callback()
                self._send_json(result)
            else:
                self._send_json({"error": "Status callback not configured"}, 500)
        elif self.path == "/admin/health":
            self._send_json({"status": "healthy"})
        else:
            self._send_json({"error": "Not found"}, 404)
    
    def do_POST(self):
        # Handle POST requests
        if self.path == "/admin/flush":
            if self.flush_callback:
                result = self.flush_callback()
                logger.info("Manual flush triggered via admin API")
                self._send_json(result)
            else:
                self._send_json({"error": "Flush callback not configured"}, 500)
        else:
            self._send_json({"error": "Not found"}, 404)


class AdminServer:
    """Simple HTTP server for admin commands."""
    
    def __init__(self, port: int):
        self._port = port
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[Thread] = None
    
    def set_flush_callback(self, callback: Callable[[], dict]):
        # Set the callback for flush command
        AdminHandler.flush_callback = callback
    
    def set_status_callback(self, callback: Callable[[], dict]):
        # Set the callback for status command
        AdminHandler.status_callback = callback
    
    def start(self):
        # Start the admin server in a background thread
        self._server = HTTPServer(("0.0.0.0", self._port), AdminHandler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info("Admin server started on port %d", self._port)
    
    def stop(self):
        # Stop the admin server
        if self._server:
            self._server.shutdown()
            logger.info("Admin server stopped")

