"""pytest 公共夹具：临时文件 SQLite（WAL）+ 进程内 Broker + FastAPI TestClient。

测试库刻意不用 :memory: + StaticPool：消费者线程与测试断言线程共享同一
DBAPI 连接时存在跨线程结果可见性抖动。改为每个测试一个临时文件库、
WAL 模式、连接各自独立，既快速又贴近 PostgreSQL 的多连接语义。
"""
from __future__ import annotations

import os

import pytest

# 在导入应用模块前固定环境变量
os.environ.setdefault("BROKER_BACKEND", "memory")
os.environ.setdefault("TOPIC", "weighbridge-events")
os.environ.setdefault("TOPIC_PARTITIONS", "2")
os.environ.setdefault("CONSUMER_GROUP", "test-group")
os.environ.setdefault("INSTANCE_ID", "test-instance")
# 测试中缩短轮询超时，避免空分区等待拖慢/饿死有消息分区
os.environ.setdefault("POLL_TIMEOUT_SECONDS", "0.05")


def make_file_engine_factory(tmp_path):
    """构造 (engine, session_factory, url)，临时文件 + WAL。"""
    from sqlalchemy import create_engine, event
    from sqlalchemy.orm import sessionmaker

    from app.database import Base

    db_path = tmp_path / "test.db"
    url = f"sqlite:///{db_path}"
    engine = create_engine(
        url,
        connect_args={"check_same_thread": False, "timeout": 30},
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _record):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA busy_timeout=30000")
        cur.close()

    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    return engine, factory, url


@pytest.fixture()
def db_setup(tmp_path):
    """临时文件库单例：FastAPI 依赖、消费者与并发线程共用同一引擎。"""
    from app import database as db_module

    engine, factory, url = make_file_engine_factory(tmp_path)

    singleton = db_module.Database.__new__(db_module.Database)
    singleton.url = url
    singleton.engine = engine
    singleton._session_factory = factory
    db_module._db = singleton

    yield factory

    db_module._db = None
    engine.dispose()


@pytest.fixture()
def db_session(db_setup):
    session = db_setup()
    yield session
    session.close()


@pytest.fixture()
def memory_broker():
    from app.config import get_settings
    from app.messaging import InMemoryBroker, set_broker

    settings = get_settings()
    broker = InMemoryBroker()
    broker.create_topic(settings.topic, settings.topic_partitions)
    set_broker(broker)
    yield broker
    set_broker(None)


@pytest.fixture(autouse=True)
def _isolate_global_state():
    """每个测试前后清理可能跨用例残留的全局单例与配置缓存。"""
    from app import database as db_module
    from app.config import get_settings
    from app.messaging import set_broker

    get_settings.cache_clear()
    set_broker(None)
    db_module._db = None
    yield
    get_settings.cache_clear()
    set_broker(None)
    db_module._db = None


@pytest.fixture()
def client(db_setup, memory_broker):
    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as c:
        yield c
