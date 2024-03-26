import asyncmy
import pytest
import run_tests as t
import utils as u
from datetime import date

try:
    import asyncmy as db
except ModuleNotFoundError:
    pytest.skip("missing driver: asyncmy", allow_module_level=True)

pytestmark = [
    pytest.mark.mysql,
    pytest.mark.skipif(not u.has_pkg("pytest_mysql"), reason="no pytest_mysql"),
    pytest.mark.skipif(not u.has_pkg("pytest_asyncio"), reason="no pytest_asyncio"),
]

DRIVER = "asyncmy"


@pytest.fixture
def queries():
    return t.queries(DRIVER)

@pytest.fixture
@pytest.mark.asyncio
async def conn(my_dsn):
    conn = await asyncmy.connect(my_dsn)
    return conn

@pytest.fixture
@pytest.mark.asyncio
async def cur(conn):
    async with conn.cursor(cursor=db.cursors.DictCursor) as cur:
        yield cur


@pytest.mark.asyncio
async def test_record_query(cur, queries):
    await t.run_record_query(cur, queries)


@pytest.mark.asyncio
async def test_parameterized_query(cur, queries):
    await t.run_parameterized_query(cur, queries)

@pytest.mark.asyncio
async def test_parameterized_record_query(cur, queries):  # pragma: no cover
    await t.run_parameterized_record_query(cur, queries, DRIVER, date)


@pytest.mark.asyncio
async def test_record_class_query(cur, queries):
    await t.run_record_class_query(cur, queries, date)


@pytest.mark.asyncio
async def test_select_cursor_context_manager(cur, queries):
    await t.run_select_cursor_context_manager(cur, queries, date)


@pytest.mark.asyncio
async def test_select_one(cur, queries):
    await t.run_select_one(cur, queries)


@pytest.mark.asyncio
async def test_select_value(cur, queries):
    await t.run_select_value(cur, queries, DRIVER)


@pytest.mark.skip("MySQL does not support RETURNING")
@pytest.mark.asyncio
async def test_insert_returning(cur, queries):  # pragma: no cover
    await t.run_insert_returning(cur, queries, DRIVER, date)


@pytest.mark.asyncio
async def test_delete(cur, queries):
    await t.run_delete(cur, queries)


@pytest.mark.asyncio
async def test_insert_many(cur, queries):
    await t.run_insert_many(cur, queries, date)


@pytest.mark.asyncio
async def test_date_time(cur, queries):
    await t.run_date_time(cur, queries, DRIVER)


