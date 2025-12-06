import hashlib
import json
import time
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Set

import feedparser
import psycopg2
from psycopg2.extras import execute_values
import logging
from bs4 import BeautifulSoup
from config import get_batch_settings


def _extract_text_from_html(html: Optional[str]) -> str:
    """HTML에서 텍스트만 추출합니다."""
    if not html:
        return ""
    
    try:
        # BeautifulSoup으로 HTML 파싱
        soup = BeautifulSoup(str(html), 'html.parser')
        
        # 스크립트와 스타일 태그 제거
        for script in soup(["script", "style", "noscript"]):
            script.decompose()
        
        # 텍스트 추출 및 정리
        text = soup.get_text(separator=' ', strip=True)
        
        # 여러 공백을 하나로 정리
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    except Exception:
        # HTML 파싱 실패 시 원본 텍스트 반환
        return str(html)


def _clean_text(text: Optional[str]) -> str:
    """텍스트를 정리합니다. HTML이 포함된 경우 파싱하여 텍스트만 추출합니다."""
    if not text:
        return ""
    
    text_str = str(text)
    
    # HTML 태그가 포함되어 있는지 확인 (<로 시작하고 >로 끝나는 태그 패턴)
    if re.search(r'<[^>]+>', text_str):
        # HTML이 포함된 경우 파싱
        return _extract_text_from_html(text_str)
    else:
        # 일반 텍스트인 경우 공백만 정리
        return " ".join(text_str.split())


def _parse_feed(url: str) -> List[Dict]:
    settings = get_batch_settings()
    posts: List[Dict] = []
    
    # 최근 1년 기준 날짜 계산
    one_year_ago = datetime.now() - timedelta(days=365)

    feedparser.USER_AGENT = settings["USER_AGENT"]
    feed = feedparser.parse(url)

    max_items = settings["MAX_ITEMS_PER_FEED"]
    skipped_old = 0

    for entry in feed.entries[:max_items]:
        # published_at를 먼저 확인하여 1년 이내인지 체크
        published_dt: Optional[datetime] = None
        try:
            if getattr(entry, 'published_parsed', None):
                published_dt = datetime(*entry.published_parsed[:6])
            elif getattr(entry, 'updated_parsed', None):
                published_dt = datetime(*entry.updated_parsed[:6])
        except Exception:
            published_dt = None

        # published_at가 없거나 1년 이전이면 pass
        if published_dt is None or published_dt < one_year_ago:
            skipped_old += 1
            continue

        title = _clean_text(getattr(entry, 'title', '') or '')
        link = getattr(entry, 'link', '') or ''

        # content 수집: 여러 소스에서 시도하고, HTML이 포함된 경우 파싱
        content = None
        if getattr(entry, 'summary', None):
            content = entry.summary
        elif getattr(entry, 'description', None):
            content = entry.description
        elif getattr(entry, 'content', None):
            c = entry.content
            if isinstance(c, list) and c:
                # content 리스트의 모든 항목을 시도
                for item in c:
                    if hasattr(item, 'value'):
                        content = item.value
                        break
                    elif isinstance(item, str):
                        content = item
                        break
            elif isinstance(c, str):
                content = c
        
        # HTML 파싱 및 텍스트 추출
        content = _clean_text(content or '')

        posts.append({
            'title': title,
            'link': link,
            'content': content,
            'published_at': published_dt,
        })

    return posts


class RssBatchService:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.logger = logging.getLogger("rss-batch")
        self._active_keywords_cache: Optional[List[Dict]] = None

    def _get_conn(self):
        return psycopg2.connect(**self.db_config)

    def _fetch_active_keywords(self) -> List[Dict]:
        """활성 키워드 목록을 가져옵니다 (캐시 사용)."""
        if self._active_keywords_cache is not None:
            return self._active_keywords_cache

        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT keyword_id, en_name, ko_name
                FROM keywords
                WHERE is_active = TRUE AND deleted_at IS NULL
                """
            )
            rows = cur.fetchall()
            keywords = [
                {
                    'keyword_id': r[0],
                    'en_name': r[1],
                    'ko_name': r[2],
                }
                for r in rows
            ]
            self._active_keywords_cache = keywords
            return keywords
        finally:
            cur.close()
            conn.close()

    def _match_keywords(self, title: str, content: str, keywords: List[Dict]) -> Set[int]:
        """제목과 내용에서 키워드를 매칭하여 keyword_id 집합을 반환합니다."""
        matched_keyword_ids: Set[int] = set()
        search_text = f"{title} {content}".lower()
        
        for kw in keywords:
            en_name = kw['en_name'].lower()
            ko_name = kw['ko_name']
            
            # 영어 키워드 매칭 개선
            if en_name:
                # 특수 케이스: "Go" 언어는 너무 짧아서 잘못 매칭될 수 있음
                # "go" 단독 매칭은 제외하고 "golang", "go programming", "go language" 등 컨텍스트 확인
                if en_name == 'go':
                    # Go 언어 관련 컨텍스트 확인
                    go_contexts = [
                        r'\bgolang\b',
                        r'\bgo\s+programming\b',
                        r'\bgo\s+language\b',
                        r'\bgo\s+code\b',
                        r'\bgo\s+developer\b',
                        r'\bgo\s+package\b',
                        r'\bgo\s+module\b',
                        r'\bgo\s+runtime\b',
                        r'\bgo\s+goroutine\b',
                        r'\bgo\s+channel\b',
                        r'\bgo\s+interface\b',
                        r'\bprogramming\s+in\s+go\b',
                        r'\bwritten\s+in\s+go\b',
                        r'\bbuilt\s+with\s+go\b',
                    ]
                    matched = False
                    for context_pattern in go_contexts:
                        if re.search(context_pattern, search_text, re.IGNORECASE):
                            matched = True
                            break
                    if matched:
                        matched_keyword_ids.add(kw['keyword_id'])
                # 공백이나 하이픈이 포함된 복합 키워드 처리
                # 예: "Next js", "GitHub Actions", "On-device AI", "RESTful API"
                elif ' ' in en_name or '-' in en_name:
                    # 복합 키워드는 여러 변형으로 매칭 시도
                    patterns = [
                        r'\b' + re.escape(en_name) + r'\b',  # 원본: "on-device ai"
                        r'\b' + re.escape(en_name.replace('-', ' ')) + r'\b',  # 하이픈을 공백으로: "on device ai"
                        r'\b' + re.escape(en_name.replace('-', '')) + r'\b',  # 하이픈 제거: "ondevice ai"
                    ]
                    # 중복 제거
                    patterns = list(dict.fromkeys(patterns))
                    
                    for pattern in patterns:
                        if re.search(pattern, search_text, re.IGNORECASE):
                            matched_keyword_ids.add(kw['keyword_id'])
                            break
                else:
                    # 단일 단어 키워드는 단어 경계로 매칭
                    # 단, 너무 짧은 키워드(2글자 이하)는 제외 (잘못된 매칭 방지)
                    if len(en_name) > 2:
                        pattern = r'\b' + re.escape(en_name) + r'\b'
                        if re.search(pattern, search_text, re.IGNORECASE):
                            matched_keyword_ids.add(kw['keyword_id'])
                
                # 키워드 변형 처리 (예: "PostgreSQL" vs "Postgres", "Node.js" vs "Node js", "NestJS" vs "Nest.js")
                # 단, "go"는 이미 특수 처리했으므로 제외
                if en_name != 'go' and ' ' not in en_name and '-' not in en_name:
                    en_variants = [
                        en_name.replace('.', '').replace(' ', ''),  # 점과 공백 제거
                        en_name.replace('.', ' '),  # 점을 공백으로
                        en_name.replace(' ', ''),  # 공백 제거
                        en_name.lower(),  # 소문자 변환 (대소문자 혼합 대응)
                    ]
                    # 중복 제거
                    en_variants = list(dict.fromkeys([v for v in en_variants if v != en_name]))
                    
                    for variant in en_variants:
                        if len(variant) > 2:  # 너무 짧은 변형은 제외
                            pattern = r'\b' + re.escape(variant) + r'\b'
                            if re.search(pattern, search_text, re.IGNORECASE):
                                matched_keyword_ids.add(kw['keyword_id'])
                                break
            
            # 한국어 키워드 매칭
            if ko_name:
                if ko_name in title or ko_name in content:
                    matched_keyword_ids.add(kw['keyword_id'])
        
        return matched_keyword_ids

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
            except Exception as e:
                fail_count += 1
                self.logger.error(f"피드 처리 실패 [{f['feed_url']}]: {str(e)[:500]}")

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
            # 1년 이전 포스트는 _parse_feed에서 필터링되어 collected에 포함되지 않음
            self.logger.info(f"  └─ 수집: {collected}개 (신규: {new_count}, 중복: {dup_count})")

        except Exception as e:
            status = 'FAILED'
            error_message = str(e)[:1000]
            self.logger.error(f"  └─ 실패: {error_message}")

        finally:
            self._log_batch(feed_id, url, status, collected, error_message)
            self._touch_last_crawled(feed_id)

        return new_count, dup_count

    def _save_posts_and_mappings(self, posts: List[Dict], region: str) -> Tuple[int, int]:
        if not posts:
            return 0, 0

        conn = self._get_conn()
        cur = conn.cursor()

        try:
            active_keywords = self._fetch_active_keywords()
            
            values_posts = []
            link_hashes = []
            post_data_map = {}
            
            for p in posts:
                link = p['link']
                link_hash = hashlib.sha256((link or '').encode('utf-8')).hexdigest()
                link_hashes.append(link_hash)
                
                # published_at이 None이면 현재 시간 사용 (NOT NULL 제약조건 대응)
                published_at = p['published_at'] if p['published_at'] else datetime.now()
                
                values_posts.append((
                    p['title'] or '',
                    link,
                    link_hash,
                    p['content'] or '',
                    region,
                    published_at,
                ))
                post_data_map[link_hash] = {
                    'title': p['title'] or '',
                    'content': p['content'] or '',
                }

            cur.execute(
                "SELECT link_hash FROM posts WHERE link_hash = ANY(%s)",
                (link_hashes,)
            )
            existing_hashes = {row[0] for row in cur.fetchall()}
            duplicate_count = len(existing_hashes)
            new_count = len(link_hashes) - duplicate_count

            execute_values(
                cur,
                """
                INSERT INTO posts (title, link, link_hash, content, region, published_at)
                VALUES %s
                ON CONFLICT (link_hash) DO NOTHING
                """,
                values_posts,
            )

            cur.execute(
                """
                SELECT post_id, link_hash
                FROM posts
                WHERE link_hash = ANY(%s)
                """,
                (link_hashes,)
            )
            
            all_post_mappings = {row[1]: row[0] for row in cur.fetchall()}
            new_post_mappings = {
                link_hash: post_id
                for link_hash, post_id in all_post_mappings.items()
                if link_hash not in existing_hashes
            }

            post_keywords_values = []
            for link_hash, post_id in new_post_mappings.items():
                post_data = post_data_map.get(link_hash)
                if not post_data:
                    continue
                
                matched_keyword_ids = self._match_keywords(
                    post_data['title'],
                    post_data['content'],
                    active_keywords
                )
                
                for keyword_id in matched_keyword_ids:
                    post_keywords_values.append((post_id, keyword_id))

            if post_keywords_values:
                execute_values(
                    cur,
                    """
                    INSERT INTO post_keywords (post_id, keyword_id)
                    VALUES %s
                    ON CONFLICT (post_id, keyword_id) DO NOTHING
                    """,
                    post_keywords_values,
                )
                self.logger.debug(f"키워드 매핑 {len(post_keywords_values)}개 추가")

            conn.commit()
            return new_count, duplicate_count

        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def _log_batch(self, feed_id: int, feed_url: str, status: str, collected_count: int, error_message: Optional[str]) -> None:
        """batch_logs 테이블에 배치 실행 로그를 기록합니다."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            detail = {
                "feed_id": feed_id,
                "feed_url": feed_url,
                "collected": collected_count,
            }
            
            log_level = "ERROR" if status == "FAILED" else "INFO"
            
            cur.execute(
                """
                INSERT INTO batch_logs (job_type, log_level, status, affected_count, detail, error_message)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                ("RSS_CRAWLER", log_level, status, collected_count, json.dumps(detail), error_message),
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


