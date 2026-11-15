"""数据库引擎、会话工厂与建表逻辑。

业务表只追加、不更新业务数据（隔离记录仅在补正成功时翻状态），
并以 (topic, source_partition, source_offset) 作为全流程幂等键，
保证每条源消息最多入仓一次、最多补正一次。
"""
from __future__ import annotations

import logging
from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


# 延迟导入，避免模型模块循环依赖
from app import models  # noqa: E402,F401  (side effect: 注册 ORM 模型)


class Database:
    """进程级数据库资源封装。"""

    def __init__(self, url: str | None = None) -> None:
        settings = get_settings()
        self.url = url or settings.database_url
        # pool_pre_ping：连接断后取连接时自动重验，减少“写库失败/断连”场景的脏错误
        self.engine: Engine = create_engine(
            self.url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=5,
            future=True,
        )
        self._session_factory = sessionmaker(
            bind=self.engine, expire_on_commit=False, autoflush=False, future=True
        )

    def init_schema(self) -> None:
        """幂等建表（演示项目用 create_all；生产可换成 Alembic）。"""
        Base.metadata.create_all(self.engine)
        logger.info("database schema ready")

    def session(self) -> Session:
        return self._session_factory()

    def dispose(self) -> None:
        self.engine.dispose()


_db: Database | None = None


def get_database() -> Database:
    """FastAPI 依赖与后台线程共用的单例。"""
    global _db
    if _db is None:
        _db = Database()
        _db.init_schema()
    return _db


def get_session() -> Iterator[Session]:
    """FastAPI 请求级会话。"""
    db = get_database()
    session = db.session()
    try:
        yield session
    finally:
        session.close()


def reset_database_singleton() -> None:
    """测试辅助：释放单例，便于切换内存库。"""
    global _db
    if _db is not None:
        _db.dispose()
    _db = None
