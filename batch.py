import logging
from rss_batch_service import RssBatchService
from config import get_db_config, setup_logging
setup_logging()
logger = logging.getLogger("batch")

DB_CONFIG = get_db_config()

if __name__ == "__main__":
    logger.info("RSS 피드 수집 배치 시작")
    service = RssBatchService(DB_CONFIG)
    service.run()
    logger.info("RSS 피드 수집 배치 완료")
