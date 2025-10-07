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

# Use float('inf') to signify effectively unlimited crawling
UNLIMITED = float('inf')

# --- Hardcoded Configuration Defaults ---
DEFAULT_CONFIG = {
    "SEED_URLS": ["https://en.wikipedia.org/wiki/"],
    "MAX_PAGES": UNLIMITED,
    "DEPTH_LIMIT": 2, #can set to unlimited
    "MAX_PAGES_PER_DOMAIN": 300, # Limit pages indexed per domain
    "BATCH_SIZE": 50,
    "COOLDOWN_TIME": 10.0,
    "ALLOWED_DOMAINS": [], 
    "BLACKLISTED_DOMAINS": [],
    "ENABLE_BREADCRUMBS": True,
    "RESUME_CRAWL": False,
    "DB_NAME": "greysearch_db"
}
# -----------------------------------------------------------------


class UnrestrictedWebSpider:
    """
    An UNRESTRICTED, persistent web spider using MongoDB for persistence.
    Fixes the 'document too large' error by distributing URL state across a dedicated collection.
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
                 queue_collection: str = "crawler_queue"): # New collection for URL state

        self.max_pages = max_pages
        self.depth_limit = depth_limit
        self.max_pages_per_domain = max_pages_per_domain
        self.resume = resume
        self.crawl_batch_size = crawl_batch_size
        self.cooldown_seconds = cooldown_seconds
        self.save_breadcrumbs = save_breadcrumbs

        # MongoDB configuration
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI")
        self.db_name = db_name
        self.results_collection_name = results_collection
        self.state_collection_name = state_collection
        self.requests_collection_name = requests_collection
        self.queue_collection_name = queue_collection # New

        if not self.mongo_uri:
            raise ValueError("MongoDB URI must be provided via argument or MONGO_URI environment variable.")

        self.client: Optional[MongoClient] = None
        self.db = None
        self.results_collection = None
        self.state_collection = None
        self.requests_collection = None
        self.queue_collection = None # New
        self._connect_db()

        # Domain filtering
        self.allowed_domains = set(allowed_domains) if allowed_domains else None
        self.blacklisted_domains = set(blacklisted_domains) if blacklisted_domains else set()

        # Queue stores tuples of (url, depth, parent_url_if_known)
        self.to_visit: deque[Tuple[str, int, Optional[str]]] = deque()
        self.results_buffer: List[Dict[str, Any]] = []
        self.indexed_count = 0
        self.skipped_count = 0
        
        # Track indexed pages per domain
        self.domain_counts: Dict[str, int] = {}

        # Initialize or Load State
        if self.resume and self._load_state_db():
            print(f"Successfully loaded state from DB. Total indexed: {self.indexed_count}")
        else:
            self._initialize_seeds(start_urls)

        # Process user requests immediately after initialization
        self._process_crawl_requests()

        self.headers = {
            'User-Agent': 'UnrestrictedWebSpiderBot/1.0 (Persistent Internet Mapping - Use with extreme caution)'
        }

        self.delay = 0.5
        self.batch_counter = 0

    # --- Database Methods ---

    def _connect_db(self):
        """Establishes connection to MongoDB and sets up necessary indexes."""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping') # Test connection

            self.db = self.client[self.db_name]
            self.results_collection = self.db[self.results_collection_name]
            self.state_collection = self.db[self.state_collection_name]
            self.requests_collection = self.db[self.requests_collection_name]
            self.queue_collection = self.db[self.queue_collection_name] # New Queue Collection

            # Ensure indexes for efficient searching and uniqueness
            self.results_collection.create_index([("url", ASCENDING)], unique=True)
            self.results_collection.create_index([("domain", ASCENDING)])
            self.results_collection.create_index([("title", "text"), ("content_snippet", "text")], name="text_search_index")
            
            # Index for the new queue collection
            self.queue_collection.create_index([("status", ASCENDING)])
            self.queue_collection.create_index([("depth", ASCENDING)])
            # The _id field (URL) is implicitly unique

            print(f"[DB] Successfully connected to MongoDB database: {self.db_name}")

        except ConnectionError as e:
            print(f"[DB ERROR] Could not connect to MongoDB. Check MONGO_URI: {e}")
            raise
        except OperationFailure as e:
            print(f"[DB ERROR] MongoDB operation failed (check credentials/permissions): {e}")
            raise

    def _is_url_known(self, url: str) -> bool:
        """Checks if the URL exists in the queue collection (i.e., has been visited, indexed, or is pending)."""
        return self.queue_collection.find_one({"_id": url}, projection={"_id": 1}) is not None

    def _add_to_queue_db(self, url: str, depth: int, parent_url: Optional[str] = None):
        """Adds a new URL to the database queue and the in-memory queue."""
        
        # Check if already known (replaces self.visited check)
        if self._is_url_known(url):
            return

        queue_doc = {
            "_id": url,
            "status": "pending",
            "depth": depth,
            "parent": parent_url,
            "timestamp": time.time()
        }
        
        try:
            self.queue_collection.insert_one(queue_doc)
            self.to_visit.append((url, depth, parent_url))
        except DuplicateKeyError:
            # Handle race condition where another process/thread added it
            pass


    def _process_crawl_requests(self):
        """Fetches pending crawl requests from the DB and adds them to the queue."""

        requests_to_process = list(self.requests_collection.find({"status": "pending"}))
        count = 0

        if not requests_to_process:
            print("[REQUESTS] No pending user crawl requests found.")
            return

        print(f"[REQUESTS] Processing {len(requests_to_process)} user crawl requests.")

        requests_to_delete = []

        for request in requests_to_process:
            raw_url = request['url']
            normalized_url = self._normalize_url(raw_url)

            if normalized_url:
                # Use the new centralized queuing function
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


    def _save_state_db(self):
        """Saves the current crawl metadata (excluding large lists/sets) to MongoDB."""
        print(f"\n[STATE SAVE] Saving current metadata to DB...")

        # Only save small, critical metadata
        state_document = {
            "_id": "current_state",
            "indexed_count": self.indexed_count,
            "skipped_count": self.skipped_count,
            "domain_counts": self.domain_counts,
            "allowed_domains": list(self.allowed_domains) if self.allowed_domains else [],
            "blacklisted_domains": list(self.blacklisted_domains),
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
            # This error should now be highly unlikely unless domain_counts is huge
            print(f"[STATE SAVE] Error saving metadata state to DB: {e}")

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

            # Load domain filtering settings
            loaded_allowed = state.get("allowed_domains")
            self.allowed_domains = set(loaded_allowed) if loaded_allowed else None
            self.blacklisted_domains = set(state.get("blacklisted_domains", []))

            # Rebuild in-memory queue from the queue collection
            print("[DB LOAD] Rebuilding in-memory queue from persistent storage...")
            pending_urls = self.queue_collection.find({"status": "pending"}).sort([("depth", ASCENDING)])
            
            queue_count = 0
            for doc in pending_urls:
                # (url, depth, parent_url)
                self.to_visit.append((doc['_id'], doc['depth'], doc.get('parent')))
                queue_count += 1

            print(f"[DB LOAD] State loaded. Queue size rebuilt: {queue_count}")
            return True

        except Exception as e:
            print(f"Error loading state from DB: {e}. Starting fresh.")
            return False

    def _save_incremental_results_db(self):
        """Inserts new results from the buffer into the MongoDB results collection."""
        if not self.results_buffer:
            return

        print(f"[DATA SYNC] Inserting {len(self.results_buffer)} new documents...")

        documents_to_insert = self.results_buffer
        self.results_buffer = [] # Clear buffer immediately

        try:
            # Use insert_many for efficiency
            self.results_collection.insert_many(documents_to_insert, ordered=False)

            inserted_count = len(documents_to_insert)
            print(f"[DATA SYNC] Synchronization complete. Inserted {inserted_count} documents.")

        except Exception as e:
            # Handle duplicate key errors gracefully
            if "duplicate key error" in str(e):
                print("[DATA SYNC] Warning: Detected potential duplicate key errors. Retrying individual inserts.")
                successful_inserts = 0
                for doc in documents_to_insert:
                    try:
                        self.results_collection.insert_one(doc)
                        successful_inserts += 1
                    except DuplicateKeyError:
                        pass # Skip duplicates
                    except Exception as inner_e:
                        print(f"Error during individual insert: {inner_e}")
                        
                print(f"[DATA SYNC] Retried batch insert. Successfully inserted {successful_inserts} documents.")
            else:
                print(f"[DATA SYNC] Critical error saving incremental results to DB: {e}")

    # --- Domain Filtering Methods ---
    def _matches_domain_pattern(self, url: str, pattern: str) -> bool:
        domain = self._get_domain(url)
        if domain == pattern: return True
        if pattern.startswith('*.'):
            suffix = pattern[1:]
            return domain.endswith(suffix)
        if pattern.endswith('.*'):
            prefix = pattern[:-2]
            return domain.startswith(prefix)
        return False

    def _is_domain_allowed(self, url: str) -> bool:
        domain = self._get_domain(url)
        
        # Check blacklist first
        for blacklisted in self.blacklisted_domains:
            if self._matches_domain_pattern(url, blacklisted): return False
            
        # Check allowed list
        if self.allowed_domains is None: 
            return True
            
        for allowed in self.allowed_domains:
            if self._matches_domain_pattern(url, allowed): return True
            
        return False

    def _should_save_page(self, url: str) -> bool:
        return self._is_domain_allowed(url)

    # --- Utility Methods ---

    def _initialize_seeds(self, start_urls: List[str]):
        print("Initializing seeds...")
        for url in start_urls:
            normalized_url = self._normalize_url(url)
            if normalized_url:
                # Use the new centralized queuing function
                self._add_to_queue_db(normalized_url, 0, parent_url=None)


    def _normalize_url(self, url: str) -> str:
        url = url.strip()
        if not urlparse(url).scheme:
            url = "http://" + url
        return url.split('#')[0]

    def _get_domain(self, url: str) -> str:
        return urlparse(url).netloc

    def _is_valid_link(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in ['http', 'https']: return False
        
        # Filter out common file extensions
        if re.search(r'\.(pdf|jpg|jpeg|png|gif|zip|rar|exe|svg|css|js|xml|txt)$', parsed.path.lower()): return False
        
        return True

    def _fetch_page(self, url: str, current_depth: int) -> Optional[str]:
        try:
            domain_info = f"[{self._get_domain(url)}]"
            print(f"D:{current_depth} | Indexed: {self.indexed_count} | Skipped: {self.skipped_count} | Queue: {len(self.to_visit)} | {domain_info} {url}")
            time.sleep(self.delay)
            response = requests.get(url, headers=self.headers, timeout=20)
            response.raise_for_status()
            if 'text/html' in response.headers.get('Content-Type', ''):
                response.encoding = response.apparent_encoding
                return response.text
            else:
                return None
        except requests.exceptions.RequestException:
            return None

    def _find_favicon_url(self, soup: BeautifulSoup, current_url: str) -> str:
        icon_tags = soup.find_all('link', rel=re.compile(r'(icon|shortcut icon|apple-touch-icon)', re.I))
        if icon_tags and icon_tags[0].get('href'):
            return urljoin(current_url, icon_tags[0].get('href'))
        base_url = urlparse(current_url).scheme + "://" + urlparse(current_url).netloc
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
        
        # We must query the DB for the parent chain
        while current and current not in seen:
            path.insert(0, current)
            seen.add(current)
            
            doc = self.queue_collection.find_one({"_id": current}, projection={"parent": 1})
            if doc and doc.get('parent'):
                current = doc['parent']
            else:
                current = None
        return path


    # --- Main Crawl Loop ---

    def crawl(self):
        """Main crawling loop (BFS) with filtering, synchronization and cooldown."""

        print(f"Starting UNRESTRICTED, PERSISTENT crawl using MongoDB.")
        if self.max_pages_per_domain != UNLIMITED:
            print(f"Domain limit set to {int(self.max_pages_per_domain)} pages per domain.")

        try:
            while self.to_visit and (self.indexed_count < self.max_pages or self.max_pages == UNLIMITED):

                # Check for new user requests periodically
                if self.batch_counter == 0 and self.indexed_count > 0:
                    self._process_crawl_requests()

                # to_visit now contains (url, depth, parent_url)
                current_url, current_depth, parent_url = self.to_visit.popleft()

                if current_depth >= self.depth_limit:
                    # Update status in DB to 'skipped' due to depth limit
                    self.queue_collection.update_one({"_id": current_url}, {"$set": {"status": "skipped"}})
                    self.skipped_count += 1
                    continue

                # Check if URL was already indexed/skipped (should be handled by _load_state_db, but good safety check)
                queue_status = self.queue_collection.find_one({"_id": current_url}, projection={"status": 1})
                if queue_status and queue_status.get('status') != 'pending':
                    continue

                html_content = self._fetch_page(current_url, current_depth)

                if html_content:
                    extracted_data = self._extract_data(html_content, current_url)
                    domain = self._get_domain(current_url)
                    
                    if self._should_save_page(current_url):
                        
                        # Domain Limit Check before indexing
                        current_domain_count = self.domain_counts.get(domain, 0)
                        if self.max_pages_per_domain != UNLIMITED and current_domain_count >= self.max_pages_per_domain:
                            print(f"   -> Domain limit reached for {domain} ({current_domain_count}/{int(self.max_pages_per_domain)}). Skipping indexing.")
                            self.skipped_count += 1
                            # Update status in DB
                            self.queue_collection.update_one({"_id": current_url}, {"$set": {"status": "skipped"}})
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
                                # Breadcrumb path is dynamically generated using parent links stored in the queue collection
                                document["breadcrumb_path"] = self._get_breadcrumb_path(current_url)
                                document["parent_url"] = parent_url

                            self.results_buffer.append(document)
                            self.indexed_count += 1
                            self.domain_counts[domain] = current_domain_count + 1 # Increment domain count
                            
                            # Update status in DB to 'indexed'
                            self.queue_collection.update_one({"_id": current_url}, {"$set": {"status": "indexed"}})

                    else:
                        self.skipped_count += 1
                        # Update status in DB to 'skipped'
                        self.queue_collection.update_one({"_id": current_url}, {"$set": {"status": "skipped"}})


                    self.batch_counter += 1

                    # Queue discovered links
                    next_depth = current_depth + 1
                    for link in extracted_data["links_found"]:
                        link_domain = self._get_domain(link)

                        # Pre-check domain limit before queuing
                        if self.max_pages_per_domain != UNLIMITED:
                            if self.domain_counts.get(link_domain, 0) >= self.max_pages_per_domain:
                                continue

                        # Use the centralized queuing function
                        self._add_to_queue_db(link, next_depth, parent_url=current_url)

                    # Synchronization and Rate Limiting Check
                    if self.batch_counter >= self.crawl_batch_size:
                        self._save_incremental_results_db()
                        self._save_state_db() # Save metadata state

                        print(f"\n[COOLDOWN] Crawled {self.crawl_batch_size} pages. Resting for {self.cooldown_seconds} seconds...")
                        time.sleep(self.cooldown_seconds)
                        print("[COOLDOWN] Resuming crawl.")

                        self.batch_counter = 0
                
                else:
                    # If fetch failed, mark as skipped/failed in queue collection
                    self.queue_collection.update_one({"_id": current_url}, {"$set": {"status": "skipped"}})
                    self.skipped_count += 1


        except KeyboardInterrupt:
            print("\n!!! Crawl interrupted by user (Ctrl+C) !!!")

        except Exception as e:
            print(f"\n!!! Critical error encountered: {e} !!!")

        finally:
            self._save_incremental_results_db()
            self._save_state_db()
            if self.client:
                self.client.close()
            print(f"\nCrawling process halted.")
            print(f"Total indexed pages: {self.indexed_count}")
            print(f"Total skipped pages: {self.skipped_count}")


# --- Execution Block ---

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


    if not SEED_URLS and not RESUME_CRAWL:
        print("Warning: No initial SEED_URLS provided. Crawler will rely only on resumed state or user requests.")

    if not MONGO_URI:
        print("Error: MONGO_URI environment variable is required to run the crawler.")
    else:
        try:
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
                db_name=MONGO_DB_NAME
            )
            crawler.crawl()
        except Exception as e:
            print(f"Crawler failed to initialize or run: {e}")

