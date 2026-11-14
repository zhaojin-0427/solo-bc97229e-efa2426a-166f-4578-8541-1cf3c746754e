FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    APP_DATA_DIR=/data

WORKDIR /srv/app

# 依赖清单位于 app/ 子目录；先单独拷贝以利用 Docker 层缓存
COPY app/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY demo/ ./demo/

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# 单 worker + 多线程：后台分组线程与 Web 请求同进程协作；
# SQLite 在 WAL + busy_timeout 下串行化写事务。
# 注意：不要开启 --preload，否则领导锁文件描述符会在 fork 时被
# 复制，破坏平滑重载期间"每进程独立竞选后台领导锁"的语义。
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "8", "--timeout", "120", "app:app"]
