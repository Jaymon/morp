# -*- coding: utf-8 -*-
from contextlib import contextmanager
import time

import psycopg
from datatypes import Datetime, Enum

from ..compat import *
from .base import Interface


class Postgres(Interface):
    """Dropfile interface using local files as messages, great for quick
    prototyping or passing messages from the frontend to the backend on the same
    machine
    """
    _connection = None

    class Status(Enum):
        NEW = "new"
        PROCESSING = "processing"
        RELEASED = "released"

    @contextmanager
    def cursor(self, name, connection, **kwargs):
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

#         except Exception:
#             if cursor is not None:
#                 cursor.close()
# 
#             raise

    def _connect(self, connection_config):
        self._connection = psycopg.connect(
            dbname=connection_config.path.lstrip("/"),
            user=connection_config.username,
            password=connection_config.password,
            host=connection_config.hosts[0][0],
            port=connection_config.hosts[0][1],
            row_factory=psycopg.rows.dict_row,
            # https://www.psycopg.org/psycopg3/docs/basic/transactions.html#autocommit-transactions
            autocommit=True,
        )

    def get_connection(self):
        return self._connection

    def _close(self):
        self._connection.close()
        self._connection = None

    def _create_table(self, name, connection):
        sql = "\n".join([
            "CREATE TABLE IF NOT EXISTS \"{}\" (".format(name),
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
        ])

        with self.cursor(name, connection) as cursor:
            cursor.execute(sql)

    def _send(self, name, connection, body, **kwargs):
        try:
            now = valid = Datetime()
            if delay_seconds := kwargs.get('delay_seconds', 0):
                valid += delay_seconds

            sql = "\n".join([
                "INSERT INTO \"{}\"".format(name),
                "  (body, status, valid, _created, _updated)",
                "VALUES",
                "  (%s, %s, %s, %s, %s)",
                "RETURNING _id"
            ])

            sql_vars = [
                body,
                self.Status.NEW.value,
                valid,
                now,
                now
            ]

            with self.cursor(name, connection) as cursor:
                cursor.execute(sql, sql_vars)
                d = cursor.fetchone()
                return d["_id"], sql_vars

        except psycopg.errors.UndefinedTable as e:
            self._create_table(name, connection)
            return self._send(name, connection, body, **kwargs)

    def _count(self, name, connection, **kwargs):
        sql = "SELECT count(*) FROM \"{}\"".format(name)

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

        with self.cursor(name, connection, transaction=True) as cursor:
            while count <= timeout:
                now = time.time_ns()

#                 sql = "\n".join([
#                     "DELETE FROM \"{}\"".format(name),
#                     "WHERE _id = (",
#                     "  SELECT _id",
#                     "  FROM \"{}\"".format(name),
#                     "  WHERE status = %s AND valid <= %s",
#                     "  ORDER BY _created ASC",
#                     "  FOR UPDATE SKIP LOCKED",
#                     "  LIMIT 1",
#                     ")",
#                     "RETURNING *",
#                 ])


                sql = "\n".join([
                    "UPDATE \"{}\"".format(name),
                    "SET status = %s",
                    "WHERE _id = (",
                    "  SELECT _id",
                    "  FROM \"{}\"".format(name),
                    "  WHERE valid <= %s",
                    "  AND status != %s",
                    "  ORDER BY _created ASC",
                    "  FOR UPDATE SKIP LOCKED",
                    "  LIMIT 1"
                    ")",
                    "RETURNING *"
                ])

#                 sql = "\n".join([
#                     "SELECT",
#                     "  _id,",
#                     "  body,",
#                     "  status,",
#                     "  count,",
#                     "  valid,",
#                     "  _created,",
#                     "  _updated",
#                     "FROM \"{}\"".format(name),
#                     "WHERE valid <= %s",
#                     "AND status != %s",
#                     "ORDER BY _created ASC",
#                     "FOR UPDATE SKIP LOCKED",
#                     "LIMIT 1"
#                 ])

                try:
                    with connection.transaction():
                        cursor.execute(sql, [self.Status.PROCESSING, valid, self.Status.PROCESSING])
                        raw = cursor.fetchone()

                except psycopg.errors.UndefinedTable:
                    pass

                finally:
                    if raw:
                        _id = raw["_id"]
                        body = raw["body"]

#                         sql = "\n".join([
#                             "UPDATE \"{}\"".format(name),
#                             "SET status = %s",
#                             "WHERE _id = %s"
#                         ])
# 
#                         cursor.execute(sql, [self.Status.PROCESSING, _id])

                        break

                    else:
                        count += 0.1
                        if count < timeout:
                            time.sleep(0.1)

        return _id, body, raw

    def _ack(self, name, connection, fields, **kwargs):
        sql = "DELETE FROM \"{}\" WHERE _id = %s".format(name)
        with self.cursor(name, connection) as cursor:
            cursor.execute(sql, [fields["_id"]])

    def _release(self, name, connection, fields, **kwargs):
        delay_seconds = kwargs.get('delay_seconds', 0)
        _id = fields["_id"]

        with self.cursor(name, connection) as cursor:
            if delay_seconds:
                valid = Datetime() + delay_seconds
                sql = "\n".join([
                    "UPDATE \"{}\" SET".format(name),
                    "  status = %s,",
                    "  count = count + 1,",
                    "  valid = %s",
                    "WHERE _id = %s"
                ])

                sql_vars = [
                    self.Status.RELEASED.value,
                    valid,
                    _id
                ]

            else:
                sql = "\n".join([
                    "UPDATE \"{}\" SET".format(name),
                    "  status = %s,",
                    "  count = count + 1,",
                    "WHERE _id = %s"
                ])

                sql_vars = [
                    self.Status.RELEASED.value,
                    _id
                ]

            cursor.execute(sql, sql_vars)

    def _clear(self, name, connection, **kwargs):
        with self.cursor(name, connection) as cursor:
            cursor.execute("DELETE FROM TABLE \"{}\" CASCADE".format(name))

    def _delete(self, name, connection, **kwargs):
        with self.cursor(name, connection) as cursor:
            cursor.execute("DROP TABLE IF EXISTS \"{}\" CASCADE".format(name))

