import os
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict


def load_env_file(path: str = None) -> None:
    if path is None:
        path = os.getenv("ENV_FILE", ".env")
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value

#.env 파일이 없으면 기본값으로 설정
def get_db_config() -> Dict[str, str]:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "database": os.getenv("DB_NAME", "postgres"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "password"),
        "port": os.getenv("DB_PORT", "5432"),
    }


def setup_logging() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, log_level, logging.INFO)

    log_dir = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, os.getenv("LOG_FILE", "batch.log"))

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        log_path, maxBytes=int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024))), backupCount=int(os.getenv("LOG_BACKUP_COUNT", "5"))
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(level)

    root = logging.getLogger()
    root.setLevel(level)
    # 핸들러 중복 방지
    root.handlers = []
    root.addHandler(file_handler)
    root.addHandler(console)


def get_batch_settings() -> Dict[str, str]:
    return {
        "MAX_ITEMS_PER_FEED": int(os.getenv("MAX_ITEMS_PER_FEED", "50")),
        "REQUEST_TIMEOUT": float(os.getenv("REQUEST_TIMEOUT", "10")),
        "USER_AGENT": os.getenv(
            "USER_AGENT",
            "Mozilla/5.0 (compatible; RSSBatch/1.0; +https://example.local)",
        ),
    }


