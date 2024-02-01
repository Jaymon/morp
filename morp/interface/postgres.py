# -*- coding: utf-8 -*-
from contextlib import contextmanager
import time

import psycopg
from psycopg_pool import ConnectionPool
from datatypes import Datetime, Enum

from ..compat import *
from .base import Interface


class Postgres(Interface):
    """PostgreSQL interface using "FOR UPDATE SKIP LOCKED" to treat postgres
    tables like a queue

    https://github.com/Jaymon/morp/issues/18
    """
#     _connection = None
    """Will hold the postgres connection

    https://www.psycopg.org/psycopg3/docs/api/connections.html#the-connection-class
    """

    _pool = None


    class Status(Enum):
        """The values for the status field in each queue table
        """
        NEW = "new"
        """Rows will be first inserted into the table using this value"""

        PROCESSING = "processing"
        """If a row has been pulled out for processing it will have this value,
        if it doesn't have this value then it can be pulled out of the queue
        for processing and its status field will be updated to this value"""

        RELEASED = "released"
        """If a message was manually released it will contain this value and
        count will be incremented"""

    @contextmanager
    def cursor(self, name, connection, **kwargs):
        """Return a connection cursor

        https://www.psycopg.org/psycopg3/docs/api/cursors.html

        :param name: str, the queue name
        :param connection: psycopg.Connection, the postgres connection
        :param **kwargs:
            - transaction: bool, if True start a tx
        returns: psycopg.Cursor
        """
        cursor = None

        try:
            cursor = connection.cursor()

            if kwargs.get("transaction", False):
                with connection.transaction():
                    yield cursor

            else:
                yield cursor

        finally:
            if cursor is not None:
                cursor.close()

    @contextmanager
    def connection(self, name, fields=None, connection=None, **kwargs):
        if connection:
            kwargs["connection"] = connection
            with super().connection(name, **kwargs) as connection:
                yield connection

        else:
            self.connect()

            with self._pool.connection() as connection:
                kwargs["connection"] = connection
                with super().connection(
                    name,
                    fields=fields,
                    **kwargs
                ) as connection:
                    yield connection

    def _connect(self, connection_config):
        """Connect to the db

        https://www.psycopg.org/psycopg3/docs/api/connections.html
        """
        self._pool = ConnectionPool(
            kwargs=dict(
                dbname=connection_config.path.lstrip("/"),
                user=connection_config.username,
                password=connection_config.password,
                host=connection_config.hosts[0][0],
                port=connection_config.hosts[0][1],
                row_factory=psycopg.rows.dict_row,
                # https://www.psycopg.org/psycopg3/docs/basic/transactions.html#autocommit-transactions
                autocommit=True,
            ),
            min_size=connection_config.options.get("min_size", 1),
            max_size=connection_config.options.get("max_size", 10),
            open=True,

        )

#     def get_connection(self):
#         return self._pool.getconn()

    def _close(self):
        self._pool.close()
        self._pool = None
#         self._connection.close()
#         self._connection = None

    def _render_sql(self, rows, *names):
        if not isinstance(rows, str):
            rows = "\n".join(rows)

        #table_name = "\"{}\"".format(name)
        return rows.format(*map(lambda n: f"\"{n}\"", names))

    def _create_table(self, name, connection):
        """Internal method that will create the queue table named `name` if it
        doesn't already exist

        :param name: str, the queue name
        :param connection: psycopg.Connection
        """
        sqls = [
            self._render_sql(
                [
                    "CREATE TABLE IF NOT EXISTS {} (",
                    # https://www.postgresql.org/docs/current/functions-uuid.html
                    # after postgres 13 gen_random_uuid() is builtin
                    "  _id UUID DEFAULT gen_random_uuid(),",
                    "  body BYTEA,",
                    "  status TEXT,",
                    "  count INTEGER DEFAULT 1,",
                    "  valid TIMESTAMPTZ,",
                    "  _created TIMESTAMPTZ,",
                    "  _updated TIMESTAMPTZ",
                    ")"
                ],
                name
            ),
            self._render_sql(
                [
                    "CREATE INDEX IF NOT EXISTS {} ON {} (",
                    "  valid,",
                    "  status,",
                    "  _created",
                    ")"
                ],
                f"{name}_index",
                name
            )
        ]

        with connection.transaction():
            with self.cursor(name, connection) as cursor:
                for sql in sqls:
                    cursor.execute(sql)

    def _send(self, name, connection, body, **kwargs):
        now = valid = Datetime()
        if delay_seconds := kwargs.get('delay_seconds', 0):
            valid += delay_seconds

        sql = self._render_sql(
            [
                "INSERT INTO {}",
                "  (body, status, valid, _created, _updated)",
                "VALUES",
                "  (%s, %s, %s, %s, %s)",
                "RETURNING _id"
            ],
            name
        )

        sql_vars = [
            body,
            self.Status.NEW.value,
            valid,
            now,
            now
        ]

        try:
            with self.cursor(name, connection) as cursor:
                cursor.execute(sql, sql_vars)
                d = cursor.fetchone()
                return d["_id"], sql_vars

        except psycopg.errors.UndefinedTable as e:
            self._create_table(name, connection)
            return self._send(name, connection, body, **kwargs)

    def _count(self, name, connection, **kwargs):
        sql = self._render_sql("SELECT count(*) FROM {}", name)

        with self.cursor(name, connection) as cursor:
            cursor.execute(sql)
            d = cursor.fetchone()
            return d["count"]

    def recv_to_fields(self, _id, body, raw):
        fields = super().recv_to_fields(_id, body, raw)
        fields["_count"] = int(raw["count"])
        return fields

    def _recv(self, name, connection, **kwargs):
        _id = body = raw = None
        timeout = kwargs.get('timeout', None) or 0.0
        valid = Datetime()
        count = 0.0

        with connection.transaction():
            while count <= timeout:
                now = time.time_ns()

                sql = self._render_sql(
                    [
                        "UPDATE {}",
                        "SET status = %s",
                        "WHERE _id = (",
                        "  SELECT _id",
                        "  FROM {}",
                        "  WHERE valid <= %s",
                        "  AND status != %s",
                        "  ORDER BY _created ASC",
                        "  FOR UPDATE SKIP LOCKED",
                        "  LIMIT 1"
                        ")",
                        "RETURNING",
                        "  _id,",
                        "  body,",
                        "  status,",
                        "  count,",
                        "  valid,",
                        "  _created,",
                        "  _updated"
                    ],
                    name,
                    name
                )

                sql_vars = [
                    self.Status.PROCESSING,
                    valid,
                    self.Status.PROCESSING
                ]

                try:
                    with self.cursor(name, connection) as cursor:
                        cursor.execute(sql, sql_vars)
                        raw = cursor.fetchone()

                except psycopg.errors.UndefinedTable:
                    pass

                finally:
                    if raw:
                        _id = raw["_id"]
                        body = raw["body"]
                        break

                    else:
                        count += 0.1
                        if count < timeout:
                            time.sleep(0.1)

        return _id, body, raw

    def _ack(self, name, connection, fields, **kwargs):
        sql = self._render_sql("DELETE FROM {} WHERE _id = %s", name)
        with self.cursor(name, connection) as cursor:
            cursor.execute(sql, [fields["_id"]])

    def _release(self, name, connection, fields, **kwargs):
        _update = Datetime()
        if delay_seconds := kwargs.get('delay_seconds', 0):
            sql = self._render_sql(
                [
                    "UPDATE {} SET",
                    "  status = %s,",
                    "  count = count + 1,",
                    "  valid = %s",
                    "  _update = %s",
                    "WHERE _id = %s"
                ],
                name
            )

            sql_vars = [
                self.Status.RELEASED.value,
                _update + delay_seconds,
                _update,
                fields["_id"]
            ]

        else:
            sql = self._render_sql(
                [
                    "UPDATE {} SET",
                    "  status = %s,",
                    "  count = count + 1,",
                    "  _update = %s",
                    "WHERE _id = %s"
                ],
                name
            )

            sql_vars = [
                self.Status.RELEASED.value,
                _update,
                fields["_id"]
            ]

        with self.cursor(name, connection) as cursor:
            cursor.execute(sql, sql_vars)

    def _clear(self, name, connection, **kwargs):
        sql = self._render_sql("DELETE FROM TABLE {} CASCADE", name)
        with self.cursor(name, connection) as cursor:
            cursor.execute(sql)

    def _delete(self, name, connection, **kwargs):
        sql = self._render_sql("DROP TABLE IF EXISTS {} CASCADE", name)
        with self.cursor(name, connection) as cursor:
            cursor.execute(sql)

