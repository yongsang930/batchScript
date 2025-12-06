import psycopg2
import logging
import json
import time
from config import get_db_config, setup_logging

setup_logging()
logger = logging.getLogger("clean-up")

SQL_FIND_ACTIVE_KEYWORDS = """
SELECT keyword_id
FROM keywords
WHERE is_active = TRUE;
"""

SQL_COUNT_POSTS = """
SELECT COUNT(*)
FROM post_keywords pk
JOIN posts p ON p.post_id = pk.post_id
WHERE pk.keyword_id = %s;
"""

SQL_FIND_OLD_POSTS = """
SELECT pk.post_id
FROM post_keywords pk
JOIN posts p ON p.post_id = pk.post_id
WHERE pk.keyword_id = %s
ORDER BY p.published_at DESC
OFFSET 30;
"""

SQL_DELETE_POST_KEYWORD = """
DELETE FROM post_keywords
WHERE post_id = %s AND keyword_id = %s;
"""

SQL_DELETE_POST = """
DELETE FROM posts
WHERE post_id = %s;
"""

def _get_conn():
    return psycopg2.connect(**get_db_config())

def _log_batch(status: str, total_keywords: int, total_deleted_posts: int, total_deleted_mappings: int, error_message: str = None):
    """batch_logs 테이블에 배치 실행 로그를 기록합니다."""
    conn = _get_conn()
    cur = conn.cursor()
    try:
        detail = {
            "total_keywords": total_keywords,
            "total_deleted_posts": total_deleted_posts,
            "total_deleted_mappings": total_deleted_mappings,
        }
        
        log_level = "ERROR" if status == "FAILED" else "INFO"
        
        cur.execute(
            """
            INSERT INTO batch_logs (job_type, log_level, status, affected_count, detail, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ("CLEANUP_OLD_POSTS", log_level, status, total_deleted_posts, json.dumps(detail), error_message),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

def run():
    start_time = time.time()
    logger.info("키워드별 오래된 게시물 정리 시작")

    conn = _get_conn()
    cur = conn.cursor()

    total_keywords = 0
    total_deleted_posts = 0
    total_deleted_mappings = 0
    status = "SUCCESS"
    error_message = None

    try:
        # 1) 활성 키워드 조회
        cur.execute(SQL_FIND_ACTIVE_KEYWORDS)
        keywords = cur.fetchall()
        total_keywords = len(keywords)
        logger.info(f"활성 키워드 {total_keywords}개 발견")

        for idx, (keyword_id,) in enumerate(keywords, 1):
            logger.info(f"[{idx}/{total_keywords}] 키워드 {keyword_id} 처리 중...")

            # 1) 전체 포스트 개수 조회
            cur.execute(SQL_COUNT_POSTS, (keyword_id,))
            (total_post_count,) = cur.fetchone()
            
            # 2) 오래된 게시물 조회 (31번째부터 끝까지)
            cur.execute(SQL_FIND_OLD_POSTS, (keyword_id,))
            old_posts = cur.fetchall()
            old_count = len(old_posts)
            
            if old_count == 0:
                if total_post_count < 30:
                    logger.info(f"  └─ 전체 포스트 {total_post_count}개 (30개 미만이라 삭제할 포스트 없음)")
                else:
                    logger.info(f"  └─ 전체 포스트 {total_post_count}개 (30개 이상이지만 삭제 대상 없음)")
                continue

            logger.info(f"  └─ 전체 포스트 {total_post_count}개 중 삭제 대상 {old_count}개 발견")

            deleted_posts_count = 0
            deleted_mappings_count = 0

            for (post_id,) in old_posts:
                # 3) post_keywords 삭제
                cur.execute(SQL_DELETE_POST_KEYWORD, (post_id, keyword_id))
                deleted_mappings_count += 1

                # 4) 다른 키워드도 연결 안 되어 있으면 posts 삭제
                cur.execute("SELECT COUNT(*) FROM post_keywords WHERE post_id = %s;", (post_id,))
                (count,) = cur.fetchone()

                if count == 0:
                    cur.execute(SQL_DELETE_POST, (post_id,))
                    deleted_posts_count += 1
                    logger.debug(f"    └─ post_id={post_id} 삭제됨")

            conn.commit()
            total_deleted_posts += deleted_posts_count
            total_deleted_mappings += deleted_mappings_count
            logger.info(f"  └─ 완료: 포스트 {deleted_posts_count}개, 매핑 {deleted_mappings_count}개 삭제")

        elapsed = time.time() - start_time
        logger.info(f"정리 배치 완료 - 키워드: {total_keywords}개, 삭제된 포스트: {total_deleted_posts}개, 삭제된 매핑: {total_deleted_mappings}개, 소요시간: {elapsed:.2f}초")

        # 배치 로그 기록
        _log_batch(status, total_keywords, total_deleted_posts, total_deleted_mappings, error_message)

    except Exception as e:
        conn.rollback()
        status = "FAILED"
        error_message = str(e)[:1000]
        logger.error(f"정리 중 오류 발생: {error_message}")
        _log_batch(status, total_keywords, total_deleted_posts, total_deleted_mappings, error_message)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run()
