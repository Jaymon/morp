from contextlib import asynccontextmanager, AbstractAsyncContextManager
import time
import select

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
    _connection = None
    """Will hold the postgres connection

    https://www.psycopg.org/psycopg3/docs/api/connections.html
    """

    _pool = None
    """Will hold the postgres connections

    https://www.psycopg.org/psycopg3/docs/advanced/pool.html
    https://www.psycopg.org/psycopg3/docs/api/pool.html
    https://www.psycopg.org/psycopg3/docs/api/connections.html
    https://www.psycopg.org/psycopg3/docs/basic/install.html#pool-installation
    """

    class Status(Enum):
        """The values for the status field in each queue table
        """
        NEW = 1
        """Rows will be first inserted into the table using this value"""

        PROCESSING = 2
        """If a row has been pulled out for processing it will have this value,
        if it doesn't have this value then it can be pulled out of the queue
        for processing and its status field will be updated to this value"""

        RELEASED = 3
        """If a message was manually released it will contain this value and
        count will be incremented"""

    @asynccontextmanager
    async def cursor(self, name, connection, **kwargs):
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
                async with connection.transaction():
                    yield cursor

            else:
                yield cursor

        finally:
            if cursor is not None:
                await cursor.close()

    async def _connect(self, connection_config):
        """Connect to the db

        https://www.psycopg.org/psycopg3/docs/api/connections.html
        """
        self._connection = await psycopg.AsyncConnection.connect(
            dbname=connection_config.path.lstrip("/"),
            user=connection_config.username,
            password=connection_config.password,
            host=connection_config.hosts[0][0],
            port=connection_config.hosts[0][1],
            row_factory=psycopg.rows.dict_row,
            # https://www.psycopg.org/psycopg3/docs/basic/transactions.html#autocommit-transactions
            autocommit=True,
        )

    async def _get_connection(self):
        return self._connection

    async def _close(self):
        await self._connection.close()
        self._connection = None

    def _render_sql(self, rows, *names):
        """Given a list of rows and names turn that into valid sql

        Internal method, used to make wrapping names and joining rows of a query
        a bit easier 

        :param rows: list[str]|str, if a list then all the rows will be joined
            with a newline
        :param *names: str, one or more values to be wrapped in quotations and
            formatted into the string at {} locations
        :returns: str, the SQL
        """
        if not isinstance(rows, str):
            rows = "\n".join(rows)

        return rows.format(*map(lambda n: f"\"{n}\"", names))

    def _render_pubsub_name(self, name):
        """The LISTEN/NOTIFY name that ._send and ._recv uses"""
        return f"{name}_notify"

    def _render_index_name(self, name):
        """The name of the table index"""
        return f"{name}_index"

    async def _create_table(self, name, connection):
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
                    "  status INTEGER,",
                    "  count INTEGER DEFAULT 1,",
                    "  valid TIMESTAMPTZ,",
                    "  _created TIMESTAMPTZ,",
                    "  _updated TIMESTAMPTZ",
                    ")",
                ],
                name
            ),
            self._render_sql(
                [
                    "CREATE INDEX IF NOT EXISTS {} ON {} (",
                    "  valid,",
                    "  status,",
                    "  _created",
                    ")",
                ],
                self._render_index_name(name),
                name
            )
        ]

        async with connection.transaction():
            async with self.cursor(name, connection) as cursor:
                for sql in sqls:
                    await cursor.execute(sql)

    async def _send(self, name, connection, body, **kwargs):
        now = valid = Datetime()
        if delay_seconds := kwargs.get('delay_seconds', 0):
            valid += delay_seconds

        sql = self._render_sql(
            [
                "INSERT INTO {}",
                "  (body, status, valid, _created, _updated)",
                "VALUES",
                "  (%s, %s, %s, %s, %s)",
                "RETURNING _id",
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
            async with self.cursor(name, connection) as cursor:
                await cursor.execute(sql, sql_vars)
                d = await cursor.fetchone()

                # https://www.postgresql.org/docs/current/sql-notify.html
                await cursor.execute(self._render_sql(
                    "NOTIFY {}",
                    self._render_pubsub_name(name)
                ))

                return d["_id"], sql_vars

        except psycopg.errors.UndefinedTable as e:
            await self._create_table(name, connection)
            return await self._send(name, connection, body, **kwargs)

    async def _count(self, name, connection, **kwargs):
        sql = self._render_sql("SELECT count(*) FROM {}", name)

        async with self.cursor(name, connection) as cursor:
            await cursor.execute(sql)
            d = await cursor.fetchone()
            return d["count"]

    def _recv_to_fields(self, _id, body, raw):
        fields = super()._recv_to_fields(_id, body, raw)
        fields["_count"] = int(raw["count"])
        return fields

    async def _get_raw(self, name, connection):
        """Try and grab a row from the db queue

        Internal method. This is broken out from ._recv because ._recv will
        first try and get a row and if that fails it will subscribe to the 
        postgres pubsub until timeout expires. If it gets a hit on pubsub it
        will call this method again looking for a matching row

        :param name: str, the queue name
        :param connection: Connection
        :returns: dict|None, the found row
        """
        valid = Datetime()
        # https://www.postgresql.org/docs/current/sql-select.html
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
                "  LIMIT 1",
                ")",
                "RETURNING",
                "  _id,",
                "  body,",
                "  status,",
                "  count,",
                "  valid,",
                "  _created,",
                "  _updated",
            ],
            name,
            name,
        )

        sql_vars = [
            self.Status.PROCESSING.value,
            valid,
            self.Status.PROCESSING.value,
        ]

        try:
            # https://www.psycopg.org/psycopg3/docs/basic/transactions.html
            async with connection.transaction():
                async with self.cursor(name, connection) as cursor:
                    await cursor.execute(sql, sql_vars)
                    raw = await cursor.fetchone()

        except psycopg.errors.UndefinedTable:
            raw = None

        return raw

    async def _recv(self, name, connection, **kwargs):
        _id = body = raw = None
        timeout = kwargs.get('timeout', 0.0)

        raw = await self._get_raw(name, connection)
        if not raw:
            async with self.cursor(name, connection) as cursor:
                # https://www.postgresql.org/docs/current/sql-listen.html
                await cursor.execute(self._render_sql(
                    "LISTEN {}",
                    self._render_pubsub_name(name)
                ))

            # this answer https://stackoverflow.com/a/41649275 pointed me in
            # the right direction on how to "consume" a message. I could've
            # made this more complicated by wrapping it in a while loop and
            # subtracting the elapsed time from timeout until it gets to zero
            # since receiving the message is no guarrantee it will be able to
            # consume the message, but it wouldn't have added much except make
            # it technically more correct since other recv methods already
            # check for None return values and re-call if no actual message was
            # received.
            #
            # https://www.psycopg.org/docs/advanced.html#asynchronous-notifications
            s = select.select([connection], [], [], timeout)
            if s[0]:
                raw = await self._get_raw(name, connection)

            # this only works on psycopg 3.2+ which is still in development as
            # of 2024-02-01
            # https://www.psycopg.org/psycopg3/docs/api/objects.html#psycopg.Notify
            # for notify in connection.notifies(timeout=timeout, stop_after=1):
            #     pout.v(notify)


        if raw:
            _id = raw["_id"]
            body = raw["body"]

        return _id, body, raw

    async def _ack(self, name, connection, fields, **kwargs):
        sql = self._render_sql("DELETE FROM {} WHERE _id = %s", name)
        async with self.cursor(name, connection) as cursor:
            await cursor.execute(sql, [fields["_id"]])

    async def _release(self, name, connection, fields, **kwargs):
        _updated = Datetime()
        if delay_seconds := kwargs.get('delay_seconds', 0):
            sql = self._render_sql(
                [
                    "UPDATE {} SET",
                    "  status = %s,",
                    "  count = count + 1,",
                    "  valid = %s,",
                    "  _updated = %s",
                    "WHERE _id = %s",
                ],
                name
            )

            sql_vars = [
                self.Status.RELEASED.value,
                _updated + delay_seconds,
                _updated,
                fields["_id"]
            ]

        else:
            sql = self._render_sql(
                [
                    "UPDATE {} SET",
                    "  status = %s,",
                    "  count = count + 1,",
                    "  _updated = %s",
                    "WHERE _id = %s",
                ],
                name
            )

            sql_vars = [
                self.Status.RELEASED.value,
                _updated,
                fields["_id"]
            ]

        async with self.cursor(name, connection) as cursor:
            await cursor.execute(sql, sql_vars)

    async def _clear(self, name, connection, **kwargs):
        sql = self._render_sql("DELETE FROM TABLE {} CASCADE", name)
        async with self.cursor(name, connection) as cursor:
            await cursor.execute(sql)

    async def _delete(self, name, connection, **kwargs):
        sql = self._render_sql("DROP TABLE IF EXISTS {} CASCADE", name)
        async with self.cursor(name, connection) as cursor:
            await cursor.execute(sql)

