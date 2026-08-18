import asyncio
import ssl as ssl_module
from typing import List

import asyncmy
from asyncmy.cursors import DictCursor
from asyncmy.errors import OperationalError
from asyncmy.replication import BinLogStream
from asyncmy.replication.row_events import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from loguru import logger

from meilisync.enums import EventType, SourceType
from meilisync.schemas import Event, ProgressEvent
from meilisync.settings import Sync
from meilisync.source import Source


class MySQL(Source):
    type = SourceType.mysql

    def __init__(
        self,
        progress: dict,
        tables: List[str],
        server_id: int = 1,
        **kwargs,
    ):
        kwargs = self._normalize_ssl(kwargs)
        super().__init__(progress, tables, **kwargs)
        self.server_id = int(server_id)
        self.database = kwargs.get("database")

    @staticmethod
    def _normalize_ssl(kwargs: dict) -> dict:
        """Allow ``ssl`` to be expressed in YAML.

        asyncmy expects an ``ssl.SSLContext``; a bare ``ssl: true`` in the config
        makes the connection hang instead of failing, which is hard to diagnose.
        Accepts:

            ssl: true              encrypt, do NOT verify the server certificate
            ssl: {ca: /path/ca.pem}  encrypt and verify against that CA

        ``ssl: true`` deliberately skips verification, matching how ProxySQL is
        configured against the same backend (use_ssl=1 with no ssl_p2s_ca): the
        server presents MySQL's auto-generated self-signed certificate, so
        verification would fail. That protects against passive capture on the
        LAN, NOT against an active man-in-the-middle. Supply ``ca`` to get both.
        """
        opt = kwargs.get("ssl")
        if opt is None or isinstance(opt, ssl_module.SSLContext):
            return kwargs
        if isinstance(opt, bool):
            if not opt:
                kwargs.pop("ssl", None)
                return kwargs
            ctx = ssl_module.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl_module.CERT_NONE
            kwargs["ssl"] = ctx
            return kwargs
        if isinstance(opt, dict):
            ca = opt.get("ca")
            ctx = ssl_module.create_default_context(cafile=ca) if ca else ssl_module.create_default_context()
            if not ca:
                ctx.check_hostname = False
                ctx.verify_mode = ssl_module.CERT_NONE
            else:
                ctx.check_hostname = bool(opt.get("check_hostname", False))
            if opt.get("cert") and opt.get("key"):
                ctx.load_cert_chain(opt["cert"], opt["key"])
            kwargs["ssl"] = ctx
            return kwargs
        raise ValueError(f"source.ssl must be bool, mapping or SSLContext, got {type(opt).__name__}")

    async def get_full_data(self, sync: Sync, size: int):
        if sync.fields:
            fields = ", ".join(f"{field} as {sync.fields[field] or field}" for field in sync.fields)
        else:
            fields = "*"
        async with asyncmy.connect(**self.kwargs) as conn:
            async with conn.cursor(cursor=DictCursor) as cur:
                offset = 0
                while True:
                    await cur.execute(
                        f"SELECT {fields} FROM `{sync.table}` "
                        f"ORDER BY {sync.pk} LIMIT {size} OFFSET {offset}"
                    )
                    ret = await cur.fetchall()
                    if not ret:
                        break
                    offset += size
                    yield ret

    async def get_count(self, sync: Sync):
        async with asyncmy.connect(**self.kwargs) as conn:
            async with conn.cursor(cursor=DictCursor) as cur:
                await cur.execute(f"SELECT COUNT(*) as count FROM `{sync.table}`")
                ret = await cur.fetchone()
                return ret["count"]

    async def ping(self):
        async with asyncmy.connect(**self.kwargs) as conn:
            return await conn.ping()

    async def get_current_progress(self):
        async with asyncmy.connect(**self.kwargs) as conn:
            async with conn.cursor(cursor=DictCursor) as cur:
                await cur.execute("SELECT VERSION()")
                ret = await cur.fetchone()
                version = ret["VERSION()"]
                if version >= "8.2.0":
                    await cur.execute("SHOW BINARY LOG STATUS")
                else:
                    await cur.execute("SHOW BINARY LOG STATUS")
                ret = await cur.fetchone()
                return {
                    "master_log_file": ret["File"],
                    "master_log_position": ret["Position"],
                }

    async def _create_stream(self):
        await self.ctl_conn.connect()
        self.stream = BinLogStream(
            self.conn,
            self.ctl_conn,
            server_id=self.server_id,
            master_log_file=self.progress["master_log_file"],
            master_log_position=int(self.progress["master_log_position"]),
            resume_stream=True,
            blocking=True,
            only_schemas=[self.database],
            only_tables=[f"{self.database}.{table}" for table in self.tables],
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        )

    async def __aiter__(self):
        self.conn = await asyncmy.connect(**self.kwargs)
        self.ctl_conn = await asyncmy.connect(**self.kwargs)
        if not self.progress:
            self.progress = await self.get_current_progress()
        yield ProgressEvent(
            progress=self.progress,
        )
        await self._create_stream()
        while True:
            try:
                async for event in self.stream:
                    if isinstance(event, WriteRowsEvent):
                        event_type = EventType.create
                        data_list = [row["values"] for row in event.rows]
                    elif isinstance(event, UpdateRowsEvent):
                        event_type = EventType.update
                        data_list = [row["after_values"] for row in event.rows]
                    elif isinstance(event, DeleteRowsEvent):
                        event_type = EventType.delete
                        data_list = [row["values"] for row in event.rows]
                    else:
                        continue
                    self.progress["master_log_file"] = self.stream._master_log_file
                    self.progress["master_log_position"] = self.stream._master_log_position
                    for data in data_list:
                        yield Event(
                            type=event_type,
                            table=event.table,
                            data=data,
                            progress=self.progress,
                        )
            except (OperationalError, AttributeError) as e:
                # AttributeError is caught on purpose. When the server closes an
                # idle connection (wait_timeout), asyncmy leaves its writer as
                # None and the next call raises
                #   AttributeError: 'NoneType' object has no attribute 'write'
                # from Connection._write_bytes - NOT an OperationalError. It
                # therefore escaped this handler and killed the process: 59
                # restarts were logged against a backend with wait_timeout=1800.
                logger.exception(f"Binlog stream error: {e}, sleep 10s and retry...")
                await asyncio.sleep(10)
                try:
                    await self.stream.close()
                except Exception as close_err:
                    logger.warning(f"Closing binlog stream failed: {close_err}")
                # Both connections are rebuilt, not just the control one. The
                # previous code reconnected ctl_conn alone, so a dead self.conn
                # left the stream permanently broken while the process kept
                # looping.
                for attr in ("ctl_conn", "conn"):
                    try:
                        getattr(self, attr).close()
                    except Exception:
                        pass
                try:
                    self.conn = await asyncmy.connect(**self.kwargs)
                    self.ctl_conn = await asyncmy.connect(**self.kwargs)
                    await self._create_stream()
                except Exception as e:
                    logger.exception(f"Recreate binlog stream error: {e}")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        self.ctl_conn.close()
