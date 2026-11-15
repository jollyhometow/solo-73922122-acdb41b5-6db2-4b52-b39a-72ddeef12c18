"""运行配置，全部可由环境变量覆盖（Docker Compose 注入）。"""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False, extra="ignore")

    # ---- 数据库 ----
    database_url: str = Field(
        default="postgresql+psycopg2://weigh:weigh@localhost:5432/weighbridge",
        description="SQLAlchemy 数据库连接串",
    )

    # ---- Redpanda / Kafka ----
    kafka_bootstrap: str = Field(default="localhost:9092")
    topic: str = Field(default="weighbridge-events")
    topic_partitions: int = Field(default=2)
    consumer_group: str = Field(default="weighbridge-consumer-group")
    # 演示用生产者的默认交付超时
    produce_timeout_seconds: float = Field(default=10.0)
    # 轮询间隔
    poll_timeout_seconds: float = Field(default=1.0)

    # ---- 业务规则 ----
    # 箱号长度上下限
    container_no_min_len: int = 4
    container_no_max_len: int = 17
    # 重量绝对值上限（整数千克），防止离谱值
    max_weight_kg: int = 1_000_000_000

    # ---- 运行控制 ----
    consumer_enabled: bool = Field(default=True)
    instance_id: str = Field(default="instance-1")
    # 消费线程在进程内单实例
    log_level: str = Field(default="INFO")


@lru_cache
def get_settings() -> Settings:
    return Settings()
