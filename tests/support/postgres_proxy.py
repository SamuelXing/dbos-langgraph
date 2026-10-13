"""A connection relay used only by PostgreSQL outage tests."""

import select
import socket
import socketserver
import threading
from pathlib import Path

from sqlalchemy.engine import make_url


class PostgresProxy:
    def __init__(self, database_url):
        url = make_url(database_url)
        port = int(url.query.get("port", url.port or 5432))
        host = url.query.get("host", url.host or "localhost")
        self.unix_socket = str(Path(host) / f".s.PGSQL.{port}") if host.startswith("/") else None
        self.upstream = (host, port)
        self.blocked = threading.Event()
        self.lock = threading.Lock()
        self.connections = set()
        proxy = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                proxy.forward(self.request)

        class Server(socketserver.ThreadingTCPServer):
            daemon_threads = True
            block_on_close = False

        self.server = Server(("127.0.0.1", 0), Handler)
        query = {key: value for key, value in url.query.items() if key not in ("host", "port")}
        self.database_url = url.set(
            host="127.0.0.1",
            port=self.server.server_address[1],
            query=query,
        ).render_as_string(hide_password=False)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def forward(self, client):
        upstream = None
        try:
            if self.blocked.is_set():
                return
            if self.unix_socket:
                upstream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                upstream.settimeout(2)
                upstream.connect(self.unix_socket)
            else:
                upstream = socket.create_connection(self.upstream, timeout=2)
            client.settimeout(2)
            with self.lock:
                if self.blocked.is_set():
                    return
                self.connections.update((client, upstream))
            sockets = (client, upstream)
            while not self.blocked.is_set():
                readable, _, _ = select.select(sockets, [], [], 0.1)
                for source in readable:
                    data = source.recv(65536)
                    if not data:
                        return
                    destination = upstream if source is client else client
                    self.before_forward(client, upstream, source, data)
                    destination.sendall(data)
        except (OSError, ValueError):
            # Dropping a connection is the fault the test intends to inject.
            return
        finally:
            with self.lock:
                self.connections.discard(client)
                if upstream is not None:
                    self.connections.discard(upstream)
            if upstream is not None:
                upstream.close()

    def before_forward(self, client, upstream, source, data):
        """Allow a specialized fault probe to inspect or hold a relay packet."""

    def interrupt(self):
        self.blocked.set()
        with self.lock:
            connections = list(self.connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def restore(self):
        self.blocked.clear()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.interrupt()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
