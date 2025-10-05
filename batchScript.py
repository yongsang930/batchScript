import feedparser
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import re
import json
import os
from typing import List, Dict, Optional
import psycopg2
from psycopg2.extras import execute_values

class RSSFeedReader:
    def __init__(self, feeds_file: str = "feeds.json", db_config: dict = None):
        """RSS 피드 리더 초기화"""
        self.feeds_file = feeds_file
        self.korean_feeds = []
        self.global_feeds = []
        self.feed_urls = []
        self.db_config = db_config
        
        # JSON 파일에서 피드 목록 로드
        self.load_feeds()
    
    def load_feeds(self):
        """JSON 파일에서 RSS 피드 목록 로드"""
        try:
            if os.path.exists(self.feeds_file):
                with open(self.feeds_file, 'r', encoding='utf-8') as f:
                    feeds_data = json.load(f)
                
                # 국내 피드 URL 추출
                self.korean_feeds = [feed['url'] for feed in feeds_data.get('korean_feeds', [])]
                
                # 해외 피드 URL 추출
                self.global_feeds = [feed['url'] for feed in feeds_data.get('global_feeds', [])]
                
                # 모든 피드 URL
                self.feed_urls = self.korean_feeds + self.global_feeds
                
                print(f"피드 목록을 로드했습니다: 국내 {len(self.korean_feeds)}개, 해외 {len(self.global_feeds)}개")
            else:
                print(f"피드 파일 '{self.feeds_file}'을 찾을 수 없습니다.")
                self._create_default_feeds()
        except Exception as e:
            print(f"피드 파일 로드 오류: {e}")
            self._create_default_feeds()
    
    def _create_default_feeds(self):
        """기본 피드 목록 생성"""
        self.korean_feeds = [
            'https://techblog.woowahan.com/feed',
            'https://engineering.toss.im/feed.xml'
        ]
        self.global_feeds = [
            'https://ai.googleblog.com/feeds/posts/default',
            'https://aws.amazon.com/blogs/aws/feed'
        ]
        self.feed_urls = self.korean_feeds + self.global_feeds
    
    def save_to_db(self, posts: List[Dict], feed_type: str):
        """posts 테이블에 대량 저장 (execute_values 사용)"""
        if not self.db_config or not posts:
            return
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            values_to_insert = [
                (
                    post.get('title'),
                    post.get('link'),
                    post.get('summary'),
                    post.get('published'),
                    post.get('author'),
                    post.get('source'),
                    feed_type,
                )
                for post in posts
            ]
            execute_values(
                cursor,
                """
                INSERT INTO posts (title, link, summary, published_at, author, source, feed_type)
                VALUES %s
                ON CONFLICT (link) DO NOTHING
                """,
                values_to_insert,
            )
            conn.commit()
        except Exception as e:
            print(f"대량 저장 오류: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def collect_and_save_all_feeds(self) -> None:
        """모든 RSS 피드를 수집하여 DB에 저장 (콘솔용 필터링/반환 없음)"""
        korean_posts: List[Dict] = []
        for url in self.korean_feeds:
            korean_posts.extend(self.parse_feed(url))
        print(f"국내 피드 {len(korean_posts)}개 저장 시도")
        self.save_to_db(korean_posts, "korean")

        global_posts: List[Dict] = []
        for url in self.global_feeds:
            global_posts.extend(self.parse_feed(url))
        print(f"해외 피드 {len(global_posts)}개 저장 시도")
        self.save_to_db(global_posts, "global")
    
    def add_feed(self, name: str, url: str, feed_type: str = "korean"):
        """새 피드 추가"""
        try:
            if os.path.exists(self.feeds_file):
                with open(self.feeds_file, 'r', encoding='utf-8') as f:
                    feeds_data = json.load(f)
            else:
                feeds_data = {"korean_feeds": [], "global_feeds": []}
            
            new_feed = {"name": name, "url": url}
            
            if feed_type == "korean":
                feeds_data["korean_feeds"].append(new_feed)
            else:
                feeds_data["global_feeds"].append(new_feed)
            
            with open(self.feeds_file, 'w', encoding='utf-8') as f:
                json.dump(feeds_data, f, ensure_ascii=False, indent=2)
            
            # 메모리에서도 업데이트
            self.load_feeds()
            print(f"피드가 추가되었습니다: {name}")
            
        except Exception as e:
            print(f"피드 추가 오류: {e}")
    
    def list_feeds(self):
        """현재 피드 목록 출력"""
        print("\n=== 국내 기술 블로그 ===")
        for i, url in enumerate(self.korean_feeds, 1):
            print(f"{i:2d}. {url}")
        
        print("\n=== 해외 기술 블로그 ===")
        for i, url in enumerate(self.global_feeds, 1):
            print(f"{i:2d}. {url}")
    
    def clean_text(self, text: str) -> str:
        """HTML 태그 제거 및 텍스트 정리"""
        if not text:
            return ""
        
        # HTML 태그 제거
        soup = BeautifulSoup(text, 'html.parser')
        clean_text = soup.get_text()
        
        # 불필요한 공백 제거
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        
        # Windows 콘솔에서 출력 가능한 문자만 유지
        try:
            clean_text.encode('cp949')
            return clean_text
        except UnicodeEncodeError:
            # 유니코드 문자를 제거하거나 대체
            clean_text = re.sub(r'[^\x00-\x7F\uAC00-\uD7AF\u3131-\u318E]', '', clean_text)
            return clean_text
    
    def extract_summary(self, entry) -> str:
        """요약 텍스트 추출 (여러 필드에서 시도)"""
        # summary 필드 시도
        if hasattr(entry, 'summary') and entry.summary:
            return self.clean_text(entry.summary)
        
        # description 필드 시도
        if hasattr(entry, 'description') and entry.description:
            return self.clean_text(entry.description)
        
        # content 필드 시도
        if hasattr(entry, 'content') and entry.content:
            if isinstance(entry.content, list) and len(entry.content) > 0:
                return self.clean_text(entry.content[0].value)
            elif isinstance(entry.content, str):
                return self.clean_text(entry.content)
        
        return "요약 정보가 없습니다."
    
    def format_date(self, date_str: str) -> str:
        """날짜 포맷팅"""
        try:
            # feedparser가 파싱한 시간을 사용
            if hasattr(date_str, 'timetuple'):
                return date_str.strftime('%Y-%m-%d %H:%M')
            else:
                # 문자열인 경우 파싱 시도
                dt = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %Z')
                return dt.strftime('%Y-%m-%d %H:%M')
        except:
            return date_str if date_str else "날짜 정보 없음"
    
    def parse_feed(self, url: str) -> List[Dict]:
        """RSS 피드 파싱 및 데이터 추출"""
        try:
            print(f"피드 파싱 중: {url}")
            feed = feedparser.parse(url)
            
            if feed.bozo:
                print(f"피드 파싱 경고: {url}")
            
            posts = []
            
            for entry in feed.entries[:10]:  # 최신 10개만 가져오기
                post_data = {
                    'title': self.clean_text(entry.title) if hasattr(entry, 'title') else "제목 없음",
                    'link': entry.link if hasattr(entry, 'link') else "",
                    'summary': self.extract_summary(entry),
                    'published': self.format_date(entry.published) if hasattr(entry, 'published') else "날짜 없음",
                    'author': self.clean_text(entry.author) if hasattr(entry, 'author') else "작성자 없음",
                    'source': feed.feed.title if hasattr(feed.feed, 'title') else url
                }
                posts.append(post_data)
            
            return posts
            
        except Exception as e:
            print(f"피드 파싱 오류 ({url}): {e}")
            return []
    
    def get_all_feeds(self, keywords: str = None, feed_type: str = "all") -> List[Dict]:
        """RSS 피드 수집 및 (옵션) DB 저장"""
        all_posts: List[Dict] = []
        
        if feed_type == "korean":
            urls = self.korean_feeds
            posts: List[Dict] = []
            for url in urls:
                posts.extend(self.parse_feed(url))
            self.save_to_db(posts, "korean")
            all_posts = posts
        elif feed_type == "global":
            urls = self.global_feeds
            posts = []
            for url in urls:
                posts.extend(self.parse_feed(url))
            self.save_to_db(posts, "global")
            all_posts = posts
        else:  # all
            korean_posts: List[Dict] = []
            for url in self.korean_feeds:
                korean_posts.extend(self.parse_feed(url))
            self.save_to_db(korean_posts, "korean")
            
            global_posts: List[Dict] = []
            for url in self.global_feeds:
                global_posts.extend(self.parse_feed(url))
            self.save_to_db(global_posts, "global")
            
            all_posts = korean_posts + global_posts
        
        if keywords and keywords.strip():
            all_posts = self.filter_by_keywords(all_posts, keywords.strip())
        
        all_posts.sort(key=lambda x: x['published'], reverse=True)
        return all_posts
    
    def filter_by_keywords(self, posts: List[Dict], keywords: str) -> List[Dict]:
        """키워드로 게시물 필터링 (단어 단위 정확 매칭) + 일치 키워드 반환"""
        keyword_list = [kw.strip().lower() for kw in keywords.split(',') if kw.strip()]
        filtered_posts: List[Dict] = []
        
        for post in posts:
            search_text = f"{post['title']} {post['summary']} {post['source']}".lower()
            words = re.findall(r'\b\w+\b|[가-힣]+', search_text)
            
            matched = [kw for kw in keyword_list if any(kw == w for w in words)]
            if matched:
                post_with_match = dict(post)
                post_with_match['matched_keywords'] = matched
                filtered_posts.append(post_with_match)
        
        return filtered_posts
    
    def display_posts(self, posts: List[Dict]):
        """게시물을 카드 형태로 출력"""
        print("\n" + "="*80)
        print("[RSS] 기술 블로그 피드 모음")
        print("="*80)
        
        for i, post in enumerate(posts, 1):
            print(f"\n[카드 #{i}]")
            print("-" * 60)
            print(f"[제목] {post['title']}")
            print(f"[요약] {post['summary'][:200]}{'...' if len(post['summary']) > 200 else ''}")
            print(f"[작성일] {post['published']}")
            print(f"[작성자] {post['author']}")
            print(f"[출처] {post['source']}")
            print(f"[원문] {post['link']}")
            if 'matched_keywords' in post and post['matched_keywords']:
                print(f"[키워드] {', '.join(post['matched_keywords'])}")
            print("-" * 60)

def main():
    """메인 실행 함수"""
    reader = RSSFeedReader()
    
    print("="*60)
    print("RSS 기술 블로그 피드 리더")
    print("="*60)
    print("메뉴를 선택하세요:")
    print("1. RSS 피드 읽기")
    print("2. 피드 목록 보기")
    print("3. 새 피드 추가")
    
    while True:
        menu_choice = input("선택 (1-3): ").strip()
        if menu_choice in ["1", "2", "3"]:
            break
        else:
            print("1, 2, 3 중에서 선택해주세요.")
    
    if menu_choice == "1":
        # RSS 피드 읽기
        print("\n피드 타입을 선택하세요:")
        print("1. 국내 기술 블로그")
        print("2. 해외 기술 블로그")
        print("3. 전체")
        
        while True:
            choice = input("선택 (1-3): ").strip()
            if choice == "1":
                feed_type = "korean"
                break
            elif choice == "2":
                feed_type = "global"
                break
            elif choice == "3":
                feed_type = "all"
                break
            else:
                print("1, 2, 3 중에서 선택해주세요.")
        
        print(f"\n{feed_type} 피드에서 데이터를 수집합니다...")
        print("키워드를 입력하세요 (쉼표로 구분, 엔터만 누르면 최신순):")
        keywords = input("키워드: ").strip()
        
        if keywords:
            print(f"'{keywords}' 키워드로 검색 중...")
        else:
            print("키워드 없이 최신순으로 표시합니다...")
        
        all_posts = reader.get_all_feeds(keywords, feed_type)
        
        if all_posts:
            reader.display_posts(all_posts)
            if keywords:
                print(f"\n'{keywords}' 키워드로 {len(all_posts)}개의 게시물을 찾았습니다.")
            else:
                print(f"\n총 {len(all_posts)}개의 게시물을 수집했습니다.")
        else:
            if keywords:
                print(f"'{keywords}' 키워드와 일치하는 게시물이 없습니다.")
            else:
                print("수집된 게시물이 없습니다.")
    
    elif menu_choice == "2":
        # 피드 목록 보기
        reader.list_feeds()
    
    elif menu_choice == "3":
        # 새 피드 추가
        name = input("피드 이름: ").strip()
        url = input("RSS URL: ").strip()
        feed_type = input("타입 (korean/global): ").strip().lower()
        
        if feed_type not in ["korean", "global"]:
            feed_type = "korean"
        
        reader.add_feed(name, url, feed_type)

if __name__ == "__main__":
    main()