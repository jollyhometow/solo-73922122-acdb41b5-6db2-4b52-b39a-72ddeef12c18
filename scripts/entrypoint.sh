#!/usr/bin/env bash
# 容器入口：等待 Postgres 与 Redpanda 可用后启动 API（内含后台消费者线程）。
set -euo pipefail

echo "[entrypoint] instance=${INSTANCE_ID:-instance-1} waiting for dependencies..."

# 1) 等待 PostgreSQL
python - <<'PY'
import os, time, sys
import sqlalchemy
url = os.environ["DATABASE_URL"]
deadline = time.time() + 90
last = None
while time.time() < deadline:
    try:
        engine = sqlalchemy.create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("SELECT 1"))
        engine.dispose()
        print("[entrypoint] postgres ready")
        sys.exit(0)
    except Exception as exc:  # noqa: BLE001
        last = exc
        time.sleep(2)
print(f"[entrypoint] postgres not ready: {last}", file=sys.stderr)
sys.exit(1)
PY

# 2) 等待 Redpanda/Kafka 端口可连
python - <<'PY'
import os, socket, time, sys
host_port = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
host, port = host_port.split(":")
deadline = time.time() + 90
while time.time() < deadline:
    try:
        with socket.create_connection((host, int(port)), timeout=3):
            print("[entrypoint] redpanda port open")
            sys.exit(0)
    except OSError:
        time.sleep(2)
print("[entrypoint] redpanda not reachable", file=sys.stderr)
sys.exit(1)
PY

# 3) 启动 API（单 uvicorn worker，保证每实例一个消费者线程）
exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1 --no-access-log
