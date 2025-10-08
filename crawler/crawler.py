import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import json
import time
import re
import os
from collections import deque
from typing import List, Optional, Dict, Any, Set, Tuple
from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure, DuplicateKeyError
from urllib.robotparser import RobotFileParser

# --- NEW IMPORTS FOR WEB SERVER ---
import threading
from flask import Flask, jsonify
# ----------------------------------

# Use float('inf') to signify effectively unlimited crawling
UNLIMITED = float('inf')

# --- Hardcoded Configuration Defaults ---
DEFAULT_CONFIG = {
    "SEED_URLS": ["https://en.wikipedia.org/wiki/Main_Page", "https://github.com"],
    "MAX_PAGES": UNLIMITED,
    "DEPTH_LIMIT": 2, 
    "MAX_PAGES_PER_DOMAIN": 300, 
    "BATCH_SIZE": 50,
    "COOLDOWN_TIME": 10.0,
    "ALLOWED_DOMAINS": [], 
    "BLACKLISTED_DOMAINS": [],
    "ENABLE_BREADCRUMBS": True,
    "RESUME_CRAWL": True,
    "DB_NAME": "greysearch_db",
    "STRICT_DOMAIN_MODE": False,
    "CRAWL_EXTERNAL_BUT_DONT_SAVE": True,
    "ROBOTS_ENABLED": True,
    "MAX_QUEUE_MEMORY_SIZE": 10000  # Limit in-memory queue size
}
# -----------------------------------------------------------------


class UnrestrictedWebSpider:
    """
    An UNRESTRICTED, persistent web spider using MongoDB for persistence.
    Now includes respect for robots.txt with improved efficiency and error handling.
    """

    def __init__(self,
                 start_urls: List[str],
                 max_pages: float = UNLIMITED,
                 depth_limit: float = UNLIMITED,
                 max_pages_per_domain: float = UNLIMITED,
                 resume: bool = False,
                 crawl_batch_size: int = 50,
                 cooldown_seconds: float = 5.0,
                 allowed_domains: Optional[List[str]] = None,
                 blacklisted_domains: Optional[List[str]] = None,
                 save_breadcrumbs: bool = True,
                 mongo_uri: str = None,
                 db_name: str = "greysearch_db",
                 results_collection: str = "pages",
                 state_collection: str = "crawler_state",
                 requests_collection: str = "crawl_requests",
                 queue_collection: str = "crawler_queue",
                 strict_domain_mode: bool = False,
                 crawl_external_but_dont_save: bool = False,
                 robots_enabled: bool = True,
                 max_queue_memory_size: int = 10000):

        self.max_pages = max_pages
        self.depth_limit = depth_limit
        self.max_pages_per_domain = max_pages_per_domain
        self.resume = resume
        self.crawl_batch_size = crawl_batch_size
        self.cooldown_seconds = cooldown_seconds
        self.save_breadcrumbs = save_breadcrumbs
        self.max_queue_memory_size = max_queue_memory_size
        
        # Set user agent BEFORE any robots.txt operations
        self.user_agent = 'UnrestrictedWebSpiderBot/1.0 (Persistent Internet Mapping - Use with extreme caution)'
        self.headers = {
            'User-Agent': self.user_agent
        }
        
        # --- NEW TOGGLE ASSIGNMENTS ---
        self.strict_domain_mode = strict_domain_mode
        self.crawl_external_but_dont_save = crawl_external_but_dont_save
        self.robots_enabled = robots_enabled

        # MongoDB configuration
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI")
        self.db_name = db_name
        self.results_collection_name = results_collection
        self.state_collection_name = state_collection
        self.requests_collection_name = requests_collection
        self.queue_collection_name = queue_collection 

        if not self.mongo_uri:
            # Note: This will raise an error if not run in the local testing block,
            # but for deployment, the environment variable must be set.
            raise ValueError("MongoDB URI must be provided via argument or MONGO_URI environment variable.")

        self.client: Optional[MongoClient] = None
        self.db = None
        self.results_collection = None
        self.state_collection = None
        self.requests_collection = None
        self.queue_collection = None 
        self._connect_db()

        # Domain filtering setup
        self.blacklisted_domains = set(blacklisted_domains) if blacklisted_domains else set()
        
        if self.strict_domain_mode and not allowed_domains:
            derived_domains = {self._get_domain(url) for url in start_urls}
            self.allowed_domains = derived_domains
            print(f"[CONFIG] Strict Domain Mode enabled. Allowed domains derived from seeds: {self.allowed_domains}")
        elif allowed_domains:
            self.allowed_domains = set(allowed_domains)
        else:
            self.allowed_domains = None
            
        if self.crawl_external_but_dont_save and self.strict_domain_mode:
            print("[WARNING] Both strict_domain_mode and crawl_external_but_dont_save are True. Strict mode takes precedence for link queuing.")

        # Queue stores tuples of (url, depth, parent_url_if_known)
        self.to_visit: deque[Tuple[str, int, Optional[str]]] = deque()
        self.results_buffer: List[Dict[str, Any]] = []
        self.indexed_count = 0
        self.skipped_count = 0
        
        # Track indexed pages per domain
        self.domain_counts: Dict[str, int] = {}
        
        # Robot exclusion protocol cache with LRU limit
        self.robots_parsers: Dict[str, RobotFileParser] = {}
        self.robots_cache_max_size = 1000
        self.robots_access_order: deque[str] = deque()

        # Initialize or Load State
        if self.resume and self._load_state_db():
            print(f"Successfully loaded state from DB. Total indexed: {self.indexed_count}")
        else:
            self._initialize_seeds(start_urls)

        # Process user requests immediately after initialization
        self._process_crawl_requests()

        self.delay = 0.5
        self.batch_counter = 0

    # --- Robots.txt Methods ---

    def _get_robot_parser(self, domain: str) -> Optional[RobotFileParser]:
        """
        Fetches, parses, and caches the robots.txt file for a given domain.
        Tries HTTPS first, falls back to HTTP. Implements LRU cache eviction.
        """
        if domain in self.robots_parsers:
            # Update access order for LRU
            self.robots_access_order.remove(domain)
            self.robots_access_order.append(domain)
            return self.robots_parsers[domain]
        
        parser = RobotFileParser()
        
        # Attempt 1: HTTPS
        robots_url_https = f"https://{domain}/robots.txt"
        
        try:
            response = requests.get(robots_url_https, timeout=10, headers=self.headers)
            
            if response.status_code == 200:
                parser.parse(response.text.splitlines())
            else:
                # Try HTTP fallback
                robots_url_http = f"http://{domain}/robots.txt"
                try:
                    response = requests.get(robots_url_http, timeout=10, headers=self.headers)
                    if response.status_code == 200:
                        parser.parse(response.text.splitlines())
                except Exception:
                    pass  # If both fail, parser remains empty (allows all)

        except Exception as e:
            print(f"[ROBOTS] Error fetching robots.txt for {domain}: {e}")
            # On error, parser remains empty, defaulting to allow all
        
        # Cache the parser with LRU eviction
        if len(self.robots_parsers) >= self.robots_cache_max_size:
            oldest_domain = self.robots_access_order.popleft()
            del self.robots_parsers[oldest_domain]
        
        self.robots_parsers[domain] = parser
        self.robots_access_order.append(domain)
        return parser

    def _is_url_allowed_by_robots(self, url: str) -> bool:
        """Checks if the given URL is allowed to be fetched by the configured User-Agent."""
        if not self.robots_enabled:
            return True
            
        domain = self._get_domain(url)
        parser = self._get_robot_parser(domain)
        
        if parser is None:
            return True
            
        # Check permission using the configured User-Agent
        allowed = parser.can_fetch(self.user_agent, url)
        
        if not allowed:
            print(f"   -> [ROBOTS] Disallowed by robots.txt: {url}")
            
        return allowed
        
    # --- Database Methods ---

    def _connect_db(self):
        """Establishes connection to MongoDB and sets up necessary indexes."""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')

            self.db = self.client[self.db_name]
            self.results_collection = self.db[self.results_collection_name]
            self.state_collection = self.db[self.state_collection_name]
            self.requests_collection = self.db[self.requests_collection_name]
            self.queue_collection = self.db[self.queue_collection_name] 

            # Ensure indexes for efficient searching and uniqueness
            self.results_collection.create_index([("url", ASCENDING)], unique=True)
            self.results_collection.create_index([("domain", ASCENDING)])
            
            # Create text index only if it doesn't exist
            try:
                self.results_collection.create_index(
                    [("title", "text"), ("content_snippet", "text")], 
                    name="text_search_index"
                )
            except OperationFailure:
                pass  # Index already exists
            
            # Index for the queue collection
            self.queue_collection.create_index([("status", ASCENDING)])
            self.queue_collection.create_index([("depth", ASCENDING)])
            self.queue_collection.create_index([("status", ASCENDING), ("depth", ASCENDING)])

            print(f"[DB] Successfully connected to MongoDB database: {self.db_name}")

        except Exception as e:
            print(f"[DB ERROR] Could not connect to MongoDB: {e}")
            raise

    def _is_url_known(self, url: str) -> bool:
        """Checks if the URL exists in the queue collection (optimized with index)."""
        try:
            return self.queue_collection.find_one({"_id": url}, projection={"_id": 1}) is not None
        except Exception as e:
            print(f"[DB ERROR] Error checking if URL is known: {e}")
            return False

    def _add_to_queue_db(self, url: str, depth: int, parent_url: Optional[str] = None):
        """Adds a new URL to the database queue and the in-memory queue (atomic operation)."""
        
        queue_doc = {
            "_id": url,
            "status": "pending",
            "depth": depth,
            "parent": parent_url,
            "timestamp": time.time()
        }
        
        try:
            # Use insert_one which will fail on duplicate (atomic check + insert)
            self.queue_collection.insert_one(queue_doc)
            
            # Only add to memory queue if we successfully inserted to DB
            if len(self.to_visit) < self.max_queue_memory_size:
                self.to_visit.append((url, depth, parent_url))
            # If memory queue is full, items will be loaded from DB later
            
        except DuplicateKeyError:
            # URL already exists, skip silently
            pass
        except Exception as e:
            print(f"[DB ERROR] Error adding URL to queue: {e}")

    def _load_next_batch_from_queue(self, batch_size: int = 100):
        """Loads the next batch of pending URLs from the database into memory."""
        try:
            pending_urls = self.queue_collection.find(
                {"status": "pending"}
            ).sort([("depth", ASCENDING)]).limit(batch_size)
            
            count = 0
            for doc in pending_urls:
                if len(self.to_visit) < self.max_queue_memory_size:
                    self.to_visit.append((doc['_id'], doc['depth'], doc.get('parent')))
                    count += 1
                else:
                    break
            
            if count > 0:
                print(f"[QUEUE] Loaded {count} URLs from database into memory queue")
                
        except Exception as e:
            print(f"[DB ERROR] Error loading batch from queue: {e}")

    def _process_crawl_requests(self):
        """Fetches pending crawl requests from the DB and adds them to the queue."""

        try:
            requests_to_process = list(self.requests_collection.find({"status": "pending"}))
            count = 0

            if not requests_to_process:
                return

            print(f"[REQUESTS] Processing {len(requests_to_process)} user crawl requests.")

            requests_to_delete = []

            for request in requests_to_process:
                raw_url = request.get('url', '')
                normalized_url = self._normalize_url(raw_url)

                if normalized_url:
                    if not self._is_url_known(normalized_url):
                        self._add_to_queue_db(normalized_url, 0, parent_url="USER_REQUEST")
                        count += 1
                        print(f"   -> Added requested URL to queue: {raw_url}")
                    else:
                        print(f"   -> Skipped requested URL (already known): {raw_url}")

                requests_to_delete.append(request['_id'])

            # Clean up processed requests
            if requests_to_delete:
                self.requests_collection.delete_many({"_id": {"$in": requests_to_delete}})
                print(f"[REQUESTS] Successfully added {count} new seed URLs and cleared {len(requests_to_delete)} requests.")
                
        except Exception as e:
            print(f"[REQUESTS ERROR] Error processing crawl requests: {e}")

    def _save_state_db(self):
        """Saves the current crawl metadata to MongoDB."""
        print(f"\n[STATE SAVE] Saving current metadata to DB...")

        state_document = {
            "_id": "current_state",
            "indexed_count": self.indexed_count,
            "skipped_count": self.skipped_count,
            "domain_counts": self.domain_counts,
            "allowed_domains": list(self.allowed_domains) if self.allowed_domains else [],
            "blacklisted_domains": list(self.blacklisted_domains),
            "strict_domain_mode": self.strict_domain_mode,
            "crawl_external_but_dont_save": self.crawl_external_but_dont_save,
            "robots_enabled": self.robots_enabled,
            "timestamp": time.time()
        }

        try:
            self.state_collection.replace_one(
                {"_id": "current_state"},
                state_document,
                upsert=True
            )
            print("[STATE SAVE] Metadata saved successfully.")
        except Exception as e:
            print(f"[STATE SAVE ERROR] Error saving metadata state to DB: {e}")

    def _load_state_db(self) -> bool:
        """Loads the crawl state from MongoDB, rebuilding the queue from the dedicated collection."""
        try:
            state = self.state_collection.find_one({"_id": "current_state"})

            if not state:
                print("No previous metadata state found in DB. Starting fresh.")
                return False

            # Load Metadata
            self.indexed_count = state.get("indexed_count", 0)
            self.skipped_count = state.get("skipped_count", 0)
            self.domain_counts = state.get("domain_counts", {})
            self.strict_domain_mode = state.get("strict_domain_mode", False)
            self.crawl_external_but_dont_save = state.get("crawl_external_but_dont_save", False)
            self.robots_enabled = state.get("robots_enabled", True)

            # Load domain filtering settings
            loaded_allowed = state.get("allowed_domains")
            self.allowed_domains = set(loaded_allowed) if loaded_allowed else None
            self.blacklisted_domains = set(state.get("blacklisted_domains", []))

            # Rebuild in-memory queue from the persistent queue collection (limited batch)
            print("[DB LOAD] Rebuilding in-memory queue from persistent storage...")
            self._load_next_batch_from_queue(self.max_queue_memory_size)

            print(f"[DB LOAD] State loaded. Initial queue size: {len(self.to_visit)}")
            return True

        except Exception as e:
            print(f"[DB LOAD ERROR] Error loading state from DB: {e}. Starting fresh.")
            return False

    def _save_incremental_results_db(self):
        """Inserts new results from the buffer into the MongoDB results collection."""
        if not self.results_buffer:
            return

        print(f"[DATA SYNC] Inserting {len(self.results_buffer)} new documents...")

        documents_to_insert = self.results_buffer
        self.results_buffer = []

        try:
            if len(documents_to_insert) == 1:
                try:
                    self.results_collection.insert_one(documents_to_insert[0])
                    print(f"[DATA SYNC] Successfully inserted 1 document.")
                except DuplicateKeyError:
                    print(f"[DATA SYNC] Skipped 1 duplicate document.")
            else:
                try:
                    result = self.results_collection.insert_many(documents_to_insert, ordered=False)
                    inserted_count = len(result.inserted_ids)
                    print(f"[DATA SYNC] Synchronization complete. Inserted {inserted_count} documents.")
                except Exception as e:
                    if "duplicate key error" in str(e).lower():
                        print("[DATA SYNC] Warning: Some duplicate key errors detected. Attempting individual inserts.")
                        successful_inserts = 0
                        for doc in documents_to_insert:
                            try:
                                self.results_collection.insert_one(doc)
                                successful_inserts += 1
                            except DuplicateKeyError:
                                pass
                            except Exception as inner_e:
                                print(f"[DATA SYNC ERROR] Error during individual insert: {inner_e}")
                        print(f"[DATA SYNC] Successfully inserted {successful_inserts} documents after retry.")
                    else:
                        raise

        except Exception as e:
            print(f"[DATA SYNC ERROR] Critical error saving incremental results to DB: {e}")

    # --- Domain Filtering Methods ---
    def _matches_domain_pattern(self, url: str, pattern: str) -> bool:
        domain = self._get_domain(url)
        if domain == pattern: 
            return True
        if pattern.startswith('*.'):
            suffix = pattern[1:]
            return domain.endswith(suffix)
        if pattern.endswith('.*'):
            prefix = pattern[:-2]
            return domain.startswith(prefix)
        return False

    def _is_domain_allowed(self, url: str) -> bool:
        """
        Checks if the URL's domain is allowed based on the configuration.
        This determines if a page is *saved*.
        """
        domain = self._get_domain(url)
        
        # Check blacklist first (always enforced)
        for blacklisted in self.blacklisted_domains:
            if self._matches_domain_pattern(url, blacklisted): 
                return False
            
        # Check allowed list
        if self.allowed_domains is None: 
            return True
            
        for allowed in self.allowed_domains:
            if self._matches_domain_pattern(url, allowed): 
                return True
            
        return False

    def _should_queue_link(self, url: str) -> bool:
        """
        Determines if a newly discovered link should be added to the queue for crawling.
        This respects both strict mode and blacklisting.
        """
        # 1. Check blacklisting (always enforced)
        domain = self._get_domain(url)
        for blacklisted in self.blacklisted_domains:
            if self._matches_domain_pattern(url, blacklisted): 
                return False

        # 2. Check Strict Mode (Toggle 1)
        if self.strict_domain_mode:
            return self._is_domain_allowed(url)

        # 3. Check Crawl External Mode (Toggle 2)
        if self.crawl_external_but_dont_save:
            return True
            
        # 4. Standard Mode
        return self._is_domain_allowed(url)

    # --- Utility Methods ---

    def _initialize_seeds(self, start_urls: List[str]):
        print("Initializing seeds...")
        for url in start_urls:
            normalized_url = self._normalize_url(url)
            if normalized_url:
                self._add_to_queue_db(normalized_url, 0, parent_url=None)

    def _normalize_url(self, url: str) -> str:
        if not url:
            return ""
        url = url.strip()
        if not urlparse(url).scheme:
            url = "http://" + url
        # Remove fragment
        url = url.split('#')[0]
        return url

    def _get_domain(self, url: str) -> str:
        return urlparse(url).netloc

    def _is_valid_link(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in ['http', 'https']: 
            return False
        
        # Filter out common file extensions
        if re.search(r'\.(pdf|jpg|jpeg|png|gif|zip|rar|exe|svg|css|js|xml|txt|mp4|mp3|avi|mov|wmv|flv|doc|docx|xls|xlsx|ppt|pptx)$', parsed.path.lower()): 
            return False
        
        return True

    def _fetch_page(self, url: str, current_depth: int) -> Optional[str]:
        try:
            domain_info = f"[{self._get_domain(url)}]"
            print(f"D:{current_depth} | Indexed: {self.indexed_count} | Skipped: {self.skipped_count} | Queue: {len(self.to_visit)} | {domain_info} {url}")
            time.sleep(self.delay)
            response = requests.get(url, headers=self.headers, timeout=20, allow_redirects=True)
            response.raise_for_status()
            
            content_type = response.headers.get('Content-Type', '').lower()
            if 'text/html' in content_type:
                response.encoding = response.apparent_encoding
                return response.text
            else:
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"   -> [FETCH ERROR] Failed to fetch {url}: {e}")
            return None
        except Exception as e:
            print(f"   -> [FETCH ERROR] Unexpected error fetching {url}: {e}")
            return None

    def _find_favicon_url(self, soup: BeautifulSoup, current_url: str) -> str:
        icon_tags = soup.find_all('link', rel=re.compile(r'(icon|shortcut icon|apple-touch-icon)', re.I))
        if icon_tags and icon_tags[0].get('href'):
            return urljoin(current_url, icon_tags[0].get('href'))
        base_url = f"{urlparse(current_url).scheme}://{urlparse(current_url).netloc}"
        return urljoin(base_url, '/favicon.ico')

    def _extract_data(self, html_content: str, current_url: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html_content, 'html.parser')
        title = soup.title.string.strip() if soup.title and soup.title.string else "No Title Found"
        favicon_url = self._find_favicon_url(soup, current_url)

        # Remove common non-content elements
        for element in soup(['script', 'style', 'header', 'footer', 'nav', 'form', 'aside', 'iframe', 'noscript']):
            element.decompose()

        main_content = soup.find('main') or soup.find('article') or soup.find('div', class_=re.compile(r'content|main', re.I)) or soup.body or soup
        text_content = main_content.get_text(separator=' ', strip=True)
        text_content = re.sub(r'\s+', ' ', text_content).strip()

        content_snippet = text_content
        if len(text_content) > 500:
            truncated = text_content[:500]
            last_period = max(truncated.rfind('. '), truncated.rfind('! '), truncated.rfind('? '))
            if last_period > 200:
                content_snippet = truncated[:last_period + 1] + ".."
            else:
                last_space = truncated.rfind(' ')
                content_snippet = truncated[:last_space] + "..." if last_space > 0 else truncated + "..."

        found_links = set()
        for link_tag in soup.find_all('a', href=True):
            href = link_tag['href']
            absolute_url = self._normalize_url(urljoin(current_url, href))
            if self._is_valid_link(absolute_url):
                found_links.add(absolute_url)

        return {
            "title": title,
            "content_snippet": content_snippet,
            "links_found": list(found_links),
            "favicon_url": favicon_url
        }

    def _get_breadcrumb_path(self, url: str) -> List[str]:
        """Reconstructs the path by querying the queue collection iteratively."""
        path = []
        current = url
        seen = set()
        max_depth = 100  # Prevent infinite loops
        
        try:
            while current and current not in seen and len(path) < max_depth:
                path.insert(0, current)
                seen.add(current)
                
                doc = self.queue_collection.find_one({"_id": current}, projection={"parent": 1})
                if doc and doc.get('parent') and doc.get('parent') != "USER_REQUEST":
                    current = doc['parent']
                else:
                    current = None
                    
        except Exception as e:
            print(f"[BREADCRUMB ERROR] Error building breadcrumb: {e}")
            
        return path

    # --- Main Crawl Loop ---

    def crawl(self):
        """Main crawling loop (BFS) with filtering, synchronization and cooldown."""

        print(f"\nStarting UNRESTRICTED, PERSISTENT crawl using MongoDB.")
        print(f"Configuration:")
        print(f"  - Max Pages: {'Unlimited' if self.max_pages == UNLIMITED else self.max_pages}")
        print(f"  - Depth Limit: {'Unlimited' if self.depth_limit == UNLIMITED else self.depth_limit}")
        print(f"  - Max Pages Per Domain: {'Unlimited' if self.max_pages_per_domain == UNLIMITED else self.max_pages_per_domain}")
        print(f"  - Strict Domain Mode: {self.strict_domain_mode}")
        print(f"  - Crawl External But Don't Save: {self.crawl_external_but_dont_save}")
        print(f"  - Robots.txt Enforcement: {'ENABLED' if self.robots_enabled else 'DISABLED'}")
        if self.robots_enabled:
            print(f"  - User-Agent: {self.user_agent}")
        print()

        try:
            while (self.indexed_count < self.max_pages or self.max_pages == UNLIMITED):
                
                # If in-memory queue is empty or low, try to load more from DB
                if len(self.to_visit) < 100:
                    self._load_next_batch_from_queue(500)
                
                # Check if we truly have no more URLs
                if not self.to_visit:
                    # Double-check database for any pending URLs we might have missed
                    pending_count = self.queue_collection.count_documents({"status": "pending"})
                    if pending_count > 0:
                        print(f"[QUEUE] Found {pending_count} pending URLs in database, loading more...")
                        self._load_next_batch_from_queue(500)
                    
                    if not self.to_visit:
                        print("[CRAWL] No more URLs to visit. Crawl complete.")
                        break

                # Check for new user requests periodically
                if self.batch_counter == 0 and self.indexed_count > 0:
                    self._process_crawl_requests()

                current_url, current_depth, parent_url = self.to_visit.popleft()

                # Check depth limit
                if current_depth >= self.depth_limit:
                    try:
                        self.queue_collection.update_one(
                            {"_id": current_url}, 
                            {"$set": {"status": "skipped_depth"}}
                        )
                    except Exception as e:
                        print(f"[DB ERROR] Error updating queue status: {e}")
                    self.skipped_count += 1
                    continue

                # Check if URL was already processed
                try:
                    queue_status = self.queue_collection.find_one(
                        {"_id": current_url}, 
                        projection={"status": 1}
                    )
                    if queue_status and queue_status.get('status') != 'pending':
                        continue
                except Exception as e:
                    print(f"[DB ERROR] Error checking queue status: {e}")
                    continue
                    
                # Robots.txt Check
                if not self._is_url_allowed_by_robots(current_url):
                    try:
                        self.queue_collection.update_one(
                            {"_id": current_url}, 
                            {"$set": {"status": "disallowed_robots"}}
                        )
                    except Exception as e:
                        print(f"[DB ERROR] Error updating queue status: {e}")
                    self.skipped_count += 1
                    continue

                # Fetch the page
                html_content = self._fetch_page(current_url, current_depth)

                if html_content:
                    extracted_data = self._extract_data(html_content, current_url)
                    domain = self._get_domain(current_url)
                    
                    # Decide if we save the content
                    if self._is_domain_allowed(current_url):
                        
                        # Domain Limit Check before indexing
                        current_domain_count = self.domain_counts.get(domain, 0)
                        if self.max_pages_per_domain != UNLIMITED and current_domain_count >= self.max_pages_per_domain:
                            print(f"   -> Domain limit reached for {domain} ({current_domain_count}/{int(self.max_pages_per_domain)}). Skipping indexing.")
                            self.skipped_count += 1
                            try:
                                self.queue_collection.update_one(
                                    {"_id": current_url}, 
                                    {"$set": {"status": "skipped_domain_limit"}}
                                )
                            except Exception as e:
                                print(f"[DB ERROR] Error updating queue status: {e}")
                        else:
                            # Index the page and update counts
                            document = {
                                "url": current_url,
                                "domain": domain,
                                "title": extracted_data["title"],
                                "content_snippet": extracted_data["content_snippet"],
                                "favicon_url": extracted_data["favicon_url"],
                                "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                                "depth": current_depth
                            }

                            if self.save_breadcrumbs:
                                document["breadcrumb_path"] = self._get_breadcrumb_path(current_url)
                                document["parent_url"] = parent_url

                            self.results_buffer.append(document)
                            self.indexed_count += 1
                            self.domain_counts[domain] = current_domain_count + 1
                            
                            try:
                                self.queue_collection.update_one(
                                    {"_id": current_url}, 
                                    {"$set": {"status": "indexed"}}
                                )
                            except Exception as e:
                                print(f"[DB ERROR] Error updating queue status: {e}")

                    else:
                        # Page was crawled but not saved
                        self.skipped_count += 1
                        try:
                            self.queue_collection.update_one(
                                {"_id": current_url}, 
                                {"$set": {"status": "processed_not_saved"}}
                            )
                        except Exception as e:
                            print(f"[DB ERROR] Error updating queue status: {e}")
                        print(f"   -> Skipped saving content (External/Disallowed Domain): {current_url}")

                    self.batch_counter += 1

                    # Queue discovered links
                    next_depth = current_depth + 1
                    for link in extracted_data["links_found"]:
                        
                        if not self._should_queue_link(link):
                            continue

                        link_domain = self._get_domain(link)

                        # Pre-check domain limit before queuing
                        if self.max_pages_per_domain != UNLIMITED:
                            if self.domain_counts.get(link_domain, 0) >= self.max_pages_per_domain:
                                continue

                        # Queue the link (atomic operation)
                        self._add_to_queue_db(link, next_depth, parent_url=current_url)

                    # Synchronization and Rate Limiting Check
                    if self.batch_counter >= self.crawl_batch_size:
                        self._save_incremental_results_db()
                        self._save_state_db()

                        print(f"\n[COOLDOWN] Crawled {self.crawl_batch_size} pages. Resting for {self.cooldown_seconds} seconds...")
                        time.sleep(self.cooldown_seconds)
                        print("[COOLDOWN] Resuming crawl.\n")

                        self.batch_counter = 0
                
                else:
                    # If fetch failed, mark as skipped/failed
                    try:
                        self.queue_collection.update_one(
                            {"_id": current_url}, 
                            {"$set": {"status": "failed_fetch"}}
                        )
                    except Exception as e:
                        print(f"[DB ERROR] Error updating queue status: {e}")
                    self.skipped_count += 1

        except KeyboardInterrupt:
            print("\n!!! Crawl interrupted by user (Ctrl+C) !!!")

        except Exception as e:
            print(f"\n!!! Critical error encountered: {e} !!!")
            import traceback
            traceback.print_exc()

        finally:
            self._save_incremental_results_db()
            self._save_state_db()
            if self.client:
                self.client.close()
            print(f"\nCrawling process halted.")
            print(f"Total indexed pages: {self.indexed_count}")
            print(f"Total skipped pages: {self.skipped_count}")


# --- Web Server Functions ---

def run_crawler_thread(crawler_instance: UnrestrictedWebSpider):
    """Function to run the main crawling loop in a background thread."""
    print("[THREADING] Starting main crawler loop...")
    crawler_instance.crawl()

def run_web_server(crawler_instance: UnrestrictedWebSpider):
    """Initializes and runs a minimal Flask server for health checks."""
    app = Flask(__name__)
    
    # Render provides the PORT environment variable
    port = int(os.environ.get("PORT", 5000))

    @app.route('/status')
    def status():
        """Provides a simple status endpoint for health checks and monitoring."""
        return jsonify({
            "status": "CRAWLER_RUNNING",
            "indexed_pages": crawler_instance.indexed_count,
            "queue_size": len(crawler_instance.to_visit),
            "depth_limit": crawler_instance.depth_limit,
            "last_check": time.strftime("%Y-%m-%d %H:%M:%S")
        })

    print(f"[WEB] Starting Flask health server on port {port}...")
    # Host must be '0.0.0.0' for deployment environments like Render
    app.run(host='0.0.0.0', port=port)


# --- Execution Block (Modified) ---

if __name__ == "__main__":

    # Read MONGO_URI from environment (required)
    MONGO_URI = os.getenv("MONGO_URI")

    # All other configurations are pulled from the hardcoded defaults above
    SEED_URLS = DEFAULT_CONFIG["SEED_URLS"]
    MAX_PAGES = DEFAULT_CONFIG["MAX_PAGES"]
    DEPTH_LIMIT = DEFAULT_CONFIG["DEPTH_LIMIT"]
    MAX_PAGES_PER_DOMAIN = DEFAULT_CONFIG["MAX_PAGES_PER_DOMAIN"]
    BATCH_SIZE = DEFAULT_CONFIG["BATCH_SIZE"]
    COOLDOWN_TIME = DEFAULT_CONFIG["COOLDOWN_TIME"]
    ALLOWED_DOMAINS = DEFAULT_CONFIG["ALLOWED_DOMAINS"]
    BLACKLISTED_DOMAINS = DEFAULT_CONFIG["BLACKLISTED_DOMAINS"]
    ENABLE_BREADCRUMBS = DEFAULT_CONFIG["ENABLE_BREADCRUMBS"]
    RESUME_CRAWL = DEFAULT_CONFIG["RESUME_CRAWL"]
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", DEFAULT_CONFIG["DB_NAME"])
    
    # --- TOGGLES ---
    STRICT_DOMAIN_MODE = DEFAULT_CONFIG["STRICT_DOMAIN_MODE"]
    CRAWL_EXTERNAL_BUT_DONT_SAVE = DEFAULT_CONFIG["CRAWL_EXTERNAL_BUT_DONT_SAVE"]
    ROBOTS_ENABLED = DEFAULT_CONFIG["ROBOTS_ENABLED"]
    MAX_QUEUE_MEMORY_SIZE = DEFAULT_CONFIG["MAX_QUEUE_MEMORY_SIZE"]

    if not MONGO_URI:
        print("Error: MONGO_URI environment variable is required to run the crawler.")
    else:
        try:
            # 1. Initialize the Crawler Instance
            crawler = UnrestrictedWebSpider(
                start_urls=SEED_URLS,
                max_pages=MAX_PAGES,
                depth_limit=DEPTH_LIMIT,
                max_pages_per_domain=MAX_PAGES_PER_DOMAIN,
                resume=RESUME_CRAWL,
                crawl_batch_size=BATCH_SIZE,
                cooldown_seconds=COOLDOWN_TIME,
                allowed_domains=ALLOWED_DOMAINS,
                blacklisted_domains=BLACKLISTED_DOMAINS,
                save_breadcrumbs=ENABLE_BREADCRUMBS,
                mongo_uri=MONGO_URI,
                db_name=MONGO_DB_NAME,
                strict_domain_mode=STRICT_DOMAIN_MODE,
                crawl_external_but_dont_save=CRAWL_EXTERNAL_BUT_DONT_SAVE,
                robots_enabled=ROBOTS_ENABLED,
                max_queue_memory_size=MAX_QUEUE_MEMORY_SIZE
            )
            
            # 2. Start the Crawler in a separate thread
            crawler_thread = threading.Thread(target=run_crawler_thread, args=(crawler,), daemon=True)
            crawler_thread.start()
            
            # 3. Start the Web Server in the main thread (blocking)
            # This ensures the process binds to the required port and stays alive.
            run_web_server(crawler)

        except Exception as e:
            print(f"Crawler failed to initialize or run: {e}")
            import traceback
            traceback.print_exc()
