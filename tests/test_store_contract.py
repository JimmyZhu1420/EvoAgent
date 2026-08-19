import inspect
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from evoagent.postgres_store import PostgresTaskStore
from evoagent.store import TaskStore


class _Result:
    def __init__(self, row=None, rows=None):
        self.row = row
        self.rows = rows or []

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows


class _RecordingConnection:
    def __init__(self):
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self

    def execute(self, statement, params=()):
        self.statements.append((statement, params))
        return _Result()


class _ShadowConnection(_RecordingConnection):
    def execute(self, statement, params=()):
        self.statements.append((statement, params))
        if statement.startswith("UPDATE deployments SET shadow_samples"):
            return _Result({
                "status": "running", "auto_promote": True,
                "shadow_samples": 2, "disagreements": 0, "min_samples": 2,
                "max_disagreement_rate": .2, "samples": 0, "errors": 0,
                "max_error_rate": .1,
            })
        if statement.startswith("SELECT * FROM release_observations"):
            return _Result(rows=[{
                "id": 1, "tenant_id": "tenant", "skill_name": "skill",
                "task_id": "task", "lane": "shadow", "primary_json": {"risk": "low"},
                "candidate_json": {"risk": "low"}, "disagreement": 0.0,
                "candidate_failed": False,
                "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            }])
        return _Result()


class StoreContractTests(unittest.TestCase):
    def test_sqlite_connections_close_on_success_and_rollback(self):
        handle, path = tempfile.mkstemp(suffix=".db")
        os.close(handle)
        try:
            store = TaskStore(path)
            with store._connect() as conn:
                conn.execute("CREATE TABLE rollback_probe(value TEXT)")
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

            with self.assertRaisesRegex(RuntimeError, "rollback"):
                with store._connect() as failed_conn:
                    failed_conn.execute("INSERT INTO rollback_probe(value) VALUES ('x')")
                    raise RuntimeError("rollback")
            with self.assertRaises(sqlite3.ProgrammingError):
                failed_conn.execute("SELECT 1")
            with store._connect() as verify_conn:
                count = verify_conn.execute("SELECT COUNT(*) FROM rollback_probe").fetchone()[0]
            self.assertEqual(0, count)

            store.create("task", "org/repo", 1, {})
            self.assertIsNotNone(store.get("task"))
            os.unlink(path)
            self.assertFalse(os.path.exists(path))
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_sqlite_connection_closes_if_row_factory_setup_fails(self):
        class BrokenConnection:
            closed = False

            @property
            def row_factory(self):
                return None

            @row_factory.setter
            def row_factory(self, _value):
                raise RuntimeError("row factory")

            def close(self):
                self.closed = True

        connection = BrokenConnection()
        store = TaskStore.__new__(TaskStore)
        store.path = "unused.db"
        with patch("evoagent.store.sqlite3.connect", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "row factory"):
                with store._connect():
                    pass
        self.assertTrue(connection.closed)

    def test_sqlite_and_postgres_expose_the_same_public_contract(self):
        def public_methods(cls):
            return {
                name for name, member in inspect.getmembers(cls, inspect.isfunction)
                if not name.startswith("_")
            }

        sqlite_methods = public_methods(TaskStore)
        postgres_methods = public_methods(PostgresTaskStore)
        self.assertSetEqual(sqlite_methods, postgres_methods)
        for method in sqlite_methods:
            self.assertEqual(
                inspect.signature(getattr(TaskStore, method)),
                inspect.signature(getattr(PostgresTaskStore, method)),
                method,
            )

    def test_postgres_schema_initializes_shadow_release_storage(self):
        connection = _RecordingConnection()
        psycopg = types.ModuleType("psycopg")
        rows = types.ModuleType("psycopg.rows")
        rows.dict_row = object()
        psycopg.connect = lambda *_args, **_kwargs: connection

        with patch.dict(sys.modules, {"psycopg": psycopg, "psycopg.rows": rows}):
            PostgresTaskStore("postgresql://test")

        schema = "\n".join(statement for statement, _params in connection.statements)
        self.assertIn("CREATE TABLE IF NOT EXISTS release_observations", schema)
        for column in (
            "max_disagreement_rate", "auto_promote", "shadow_samples", "disagreements",
        ):
            self.assertIn("ADD COLUMN IF NOT EXISTS " + column, schema)

    def test_postgres_shadow_observations_can_promote_and_be_listed(self):
        connection = _ShadowConnection()
        store = PostgresTaskStore.__new__(PostgresTaskStore)
        store._connect = lambda: connection

        store.save_deployment("tenant", "skill", {
            "stable_version": 1, "candidate_version": 2,
            "shadow_percent": 100, "auto_promote": True,
            "max_disagreement_rate": .2,
        })
        deployment = store.record_shadow_observation(
            "tenant", "skill", "task", "shadow",
            {"risk": "low"}, {"risk": "low"}, 0.0,
        )
        observations = store.list_release_observations("tenant", "skill")

        self.assertEqual("promoted", deployment["status"])
        self.assertEqual({"risk": "low"}, observations[0]["primary"])
        self.assertEqual({"risk": "low"}, observations[0]["candidate"])
        self.assertEqual("2026-01-01T00:00:00+00:00", observations[0]["created_at"])
        statements = "\n".join(statement for statement, _params in connection.statements)
        self.assertIn("SET max_disagreement_rate=%s,auto_promote=%s", statements)
        self.assertIn("INSERT INTO release_observations", statements)
        self.assertIn("status='promoted'", statements)


if __name__ == "__main__":
    unittest.main()
