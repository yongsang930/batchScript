import hashlib
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import feedparser
import psycopg2
from psycopg2.extras import execute_values
import logging
from config import get_batch_settings


def _clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    # 공백만 정리, HTML은 피드마다 다르니 그대로 둠
    return " ".join(str(text).split())


def _parse_feed(url: str) -> List[Dict]:
    settings = get_batch_settings()
    posts: List[Dict] = []

    feedparser.USER_AGENT = settings["USER_AGENT"]
    feed = feedparser.parse(url)

    max_items = settings["MAX_ITEMS_PER_FEED"]

    for entry in feed.entries[:max_items]:
        title = _clean_text(getattr(entry, 'title', '') or '')
        link = getattr(entry, 'link', '') or ''

        summary = None
        if getattr(entry, 'summary', None):
            summary = entry.summary
        elif getattr(entry, 'description', None):
            summary = entry.description
        elif getattr(entry, 'content', None):
            c = entry.content
            if isinstance(c, list) and c:
                summary = c[0].value
            elif isinstance(c, str):
                summary = c
        summary = _clean_text(summary or '')

        published_dt: Optional[datetime] = None
        try:
            if getattr(entry, 'published_parsed', None):
                published_dt = datetime(*entry.published_parsed[:6])
            elif getattr(entry, 'updated_parsed', None):
                published_dt = datetime(*entry.updated_parsed[:6])
        except Exception:
            published_dt = None

        posts.append({
            'title': title,
            'link': link,
            'summary': summary,
            'published_at': published_dt,
        })

    return posts


class RssBatchService:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.logger = logging.getLogger("rss-batch")

    def _get_conn(self):
        return psycopg2.connect(**self.db_config)

    def fetch_active_feeds(self) -> List[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT feed_id, region, feed_url
                FROM rss_feeds
                WHERE is_active = TRUE
                """
            )
            rows = cur.fetchall()
            return [
                {
                    'feed_id': r[0],
                    'region': r[1],
                    'feed_url': r[2],
                }
                for r in rows
            ]
        finally:
            cur.close()
            conn.close()

    def run(self) -> None:
        start_time = time.time()
        feeds = self.fetch_active_feeds()
        total = len(feeds)
        self.logger.info(f"활성 피드 {total}개 수집 시작")

        success_count = 0
        fail_count = 0
        total_new = 0
        total_duplicate = 0

        for idx, f in enumerate(feeds, 1):
            self.logger.info(f"[{idx}/{total}] {f['feed_url']}")
            try:
                new_count, dup_count = self._process_feed(f['feed_id'], f['region'], f['feed_url'])
                success_count += 1
                total_new += new_count
                total_duplicate += dup_count
            except Exception:
                fail_count += 1

        elapsed = time.time() - start_time
        self.logger.info(f"수집 완료 - 성공: {success_count}, 실패: {fail_count}")
        self.logger.info(f"신규: {total_new}개, 중복: {total_duplicate}개, 소요시간: {elapsed:.2f}초")

    def _process_feed(self, feed_id: int, region: str, url: str) -> Tuple[int, int]:
        status = 'SUCCESS'
        error_message = None
        collected = 0
        new_count = 0
        dup_count = 0

        try:
            posts = _parse_feed(url)
            collected = len(posts)
            new_count, dup_count = self._save_posts_and_mappings(posts, region)
            self.logger.info(f"  └─ 수집: {collected}개 (신규: {new_count}, 중복: {dup_count})")

        except Exception as e:
            status = 'FAILED'
            error_message = str(e)[:1000]
            self.logger.error(f"  └─ 실패: {error_message}")

        finally:
            self._log_crawl(feed_id, status, collected, error_message)
            self._touch_last_crawled(feed_id)

        return new_count, dup_count

    def _save_posts_and_mappings(self, posts: List[Dict], region: str) -> Tuple[int, int]:
        if not posts:
            return 0, 0

        conn = self._get_conn()
        cur = conn.cursor()

        try:
            values_posts = []
            link_hashes = []
            for p in posts:
                link = p['link']
                link_hash = hashlib.sha256((link or '').encode('utf-8')).hexdigest()
                link_hashes.append(link_hash)
                values_posts.append((
                    p['title'] or '',
                    link,
                    link_hash,
                    p['summary'] or '',
                    region,
                    p['published_at'],
                ))

            # 중복 체크용
            cur.execute(
                f"SELECT link_hash FROM posts WHERE link_hash = ANY(%s)",
                (link_hashes,)
            )
            existing_hashes = {row[0] for row in cur.fetchall()}
            duplicate_count = len(existing_hashes)
            new_count = len(link_hashes) - duplicate_count

            execute_values(
                cur,
                """
                INSERT INTO posts (title, link, link_hash, summary, region, published_at)
                VALUES %s
                ON CONFLICT (link_hash) DO NOTHING
                """,
                values_posts,
            )

            conn.commit()
            return new_count, duplicate_count

        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def _log_crawl(self, feed_id: int, status: str, collected_count: int, error_message: Optional[str]) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO crawl_logs (feed_id, status, collected_count, error_message)
                VALUES (%s, %s, %s, %s)
                """,
                (feed_id, status, collected_count, error_message),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def _touch_last_crawled(self, feed_id: int) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("UPDATE rss_feeds SET last_crawled_at = NOW() WHERE feed_id = %s", (feed_id,))
            conn.commit()
        finally:
            cur.close()
            conn.close()


