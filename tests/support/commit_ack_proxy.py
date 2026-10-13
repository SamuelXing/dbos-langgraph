"""Suppress a commit response for one known test step transaction."""

import threading

from sqlalchemy.engine import make_url

from .postgres_proxy import PostgresProxy


class CommitAckProxy(PostgresProxy):
    def __init__(self, database_url, step_name):
        url = make_url(database_url)
        host = url.query.get("host", url.host or "localhost")
        if not host.startswith("/") and host not in ("localhost", "127.0.0.1", "::1"):
            raise ValueError("commit acknowledgment probes require a local test database")
        if url.query.get("sslmode") in ("require", "verify-ca", "verify-full"):
            raise ValueError("commit acknowledgment probes require a non-TLS test database")
        query = {**url.query, "sslmode": "disable"}
        super().__init__(url.set(query=query).render_as_string(hide_password=False))
        self.step_name = step_name.encode()
        self.armed = threading.Event()
        self.commit_intercepted = threading.Event()
        self.response_held = threading.Event()
        self.drop_acknowledgment = threading.Event()
        self.target_connection = None
        self.client_buffers = {}

    def before_forward(self, client, upstream, source, data):
        if source is client:
            buffer = self.client_buffers.setdefault(client, bytearray())
            buffer.extend(data)
            # This probe is specific to the pinned psycopg/SQLAlchemy transaction
            # stream. Buffering handles a COMMIT command split across packets.
            if b"COMMIT\x00" not in buffer:
                return
            target_transaction = (
                self.armed.is_set()
                and b"INSERT INTO dbos.operation_outputs" in buffer
                and self.step_name in buffer
            )
            buffer.clear()
            if target_transaction:
                with self.lock:
                    if self.target_connection is None:
                        self.target_connection = client
                        self.commit_intercepted.set()
            return
        if client is self.target_connection:
            # The server can execute COMMIT, but none of its response reaches the
            # worker. The test checks durability through a separate connection.
            self.response_held.set()
            if not self.drop_acknowledgment.wait(timeout=30):
                raise ConnectionResetError("test did not release the held commit response")
            raise ConnectionResetError("commit acknowledgment deliberately discarded")

    def __exit__(self, *args):
        self.drop_acknowledgment.set()
        super().__exit__(*args)
