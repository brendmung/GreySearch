# GreySearch

A simple full-stack search engine pipeline, from crawling and indexing to serving search results. It uses a Node.js API and a Python worker, relying heavily on MongoDB for persistence and communication.

Disclaimer: This code is a functional prototype. It is simplified, contains monolithic components, and lacks robust production-level error handling, security, and scaling features.

## ⚠️ The Unvarnished Look: Limitations and Simplifications

Before diving into the code, it’s important to understand the compromises made:

1.  **Monolithic Crawler:** The Python worker (`crawler/crawler.py`) is huge. It handles everything: fetching, parsing, link extraction, state management, and database synchronization. In a cleaner project, this would be split into separate modules (e.g., a scheduler, a fetcher pool, a parser).
2.  **Rudimentary Rate Limiting:** The politeness mechanism relies on a simple `COOLDOWN_TIME` sleep after a batch. This is global and doesn't respect per-domain `robots.txt` or dynamic server load. It's slow, but it's safe.
3.  **No Authentication/Security:** The API is wide open (`app.use(cors({ origin: '*' }));`). This is fine for a demo but obviously terrible for production.
4.  **Performance Bottlenecks:** There are few spots where i know the design is inherently slow (detailed below).

## 🧱 Architecture and Data Flow

The whole system revolves around five crucial MongoDB collections:

| Collection | Component Responsible | Purpose |
| :--- | :--- | :--- |
| `pages` | Crawler (Write), API (Read) | Stores the indexed content (the actual search results). |
| `crawl_requests` | API (Write), Crawler (Read) | Stores URLs submitted by users via the frontend. |
| `crawler_queue` | Crawler (Read/Write) | Stores every URL ever seen, its depth, parent, and current status (`pending`, `indexed`, `skipped`). **This is the key to persistence.** |
| `crawler_state` | Crawler (Read/Write) | Stores global metadata like total indexed counts and per-domain counts (`domain_counts`) for enforcing limits. |
| `crawler_state` | Crawler (Read/Write) | Stores global metadata like total indexed counts and per-domain counts (`domain_counts`) for enforcing limits. |

### The MongoDB Bottleneck: What Could Have Been Used

Relied on MongoDB because it was fast to set up and flexible. However, for performance, it would need to be swapped out:

**1. Search Performance:**
The API relies on MongoDB's built-in `$text` search feature, which requires the crawler to create the `text_search_index` on the `title` and `content_snippet` fields.

> Look, this relies on MongoDB's `$text` search, but for actual relevance and speed at scale, **dedicated search engines like Elasticsearch or Solr** might be required. MongoDB's text scoring is basic; it's just not what it's optimized for.

**2. Queue Management and Polling:**
The API saves user requests to `crawl_requests`, and the crawler polls this collection periodically (`_process_crawl_requests`).

> Used a polling method for user requests, which is simple but slow. For a proper build, use a **message queue (like RabbitMQ or Kafka)** to instantly notify the crawler when a new request comes in, rather than having it check the database every few minutes.

**3. The Breadcrumb Problem (Major Slowness):**
The `_get_breadcrumb_path` function in the crawler is a known performance killer. To reconstruct the path a page was found through, it queries the `crawler_queue` collection repeatedly, following the `parent` field up the chain.

> This means finding a page at depth 10 requires **10 separate database queries** just to save the breadcrumb path! Might be removed in future.

---

## 🐍 Crawler Deep Dive (`crawler/crawler.py`)

The `UnrestrictedWebSpider` class is where all the logic lives. It's designed for persistence, meaning it can be stopped and restarted without losing its place, thanks to the `crawler_queue` and `crawler_state` collections.

### Core Scraping Variables

These variables define the scope and behavior of the crawl:

| Variable | Default Value | Explanation |
| :--- | :--- | :--- |
| `SEED_URLS` | `["https://en.wikipedia.org/wiki/"]` | Where the crawl starts. If `RESUME_CRAWL` is true, these are ignored if a state exists. |
| `DEPTH_LIMIT` | `2` | **The Big Limiter.** How many clicks deep we go from a seed URL. Keep this low (2-3) unless you want a massive queue quickly. |
| `MAX_PAGES_PER_DOMAIN` | `300` | **Politeness.** Once we index this many pages from a domain (e.g., `wikipedia.org`), we stop indexing more from that domain, even if we find them. |
| `BATCH_SIZE` | `50` | How many pages we crawl before we pause, synchronize the data with MongoDB (`_save_incremental_results_db`), and save the state. |
| `COOLDOWN_TIME` | `10.0` | The mandatory sleep time after every batch sync. This is our simple throttle. |

### Domain Filtering Toggles

These two toggles control what we queue and what we save:

1.  **`STRICT_DOMAIN_MODE` (Default: `True`):**
    *   If `True`, we only queue links that belong to the domains derived from the initial seeds. This keeps the crawl focused.
2.  **`CRAWL_EXTERNAL_BUT_DONT_SAVE` (Default: `False`):**
    *   If `True`, we will fetch pages from external domains, but we will mark them as `processed_not_saved` in the queue and discard the content. This is useful if you want to follow external links just to find internal links pointing back to your allowed domains.

The logic in `_should_queue_link` is complex because it has to respect blacklists, strict mode, and the crawl-external toggle simultaneously.

## 🌐 API Backend (`api/server.js`)

The API is built on Express. It’s mostly just a proxy layer between the frontend and MongoDB.

### Key Functions

*   **`connectDB()`:** Handles the connection and ensures the necessary index is created on the `crawl_requests` collection (`{ url: 1 }`, unique) to prevent duplicate user submissions.
*   **`/api/search`:** Executes the MongoDB aggregation pipeline. It uses `$facet` to handle pagination and total count in a single query, which is a neat trick for efficiency, but again, the core search is limited by MongoDB's text search capabilities.
*   **`/api/request_crawl`:** This is the user interface to the crawler. It validates the URL (must start with `http://` or `https://`) and attempts to insert it into `crawl_requests`. It handles the MongoDB `11000` error code gracefully if the user tries to submit the same URL twice.

---

## 💻 Frontend (`frontend/index.html`)

The frontend is a single-page application (SPA) built purely with HTML, CSS, and vanilla JavaScript.

**Crucial Configuration:**
The JavaScript file contains a single, critical configuration constant:

```javascript
const API_BASE_URL = 'https://greysearch-api.onrender.com/api';
```

If you run this locally, you must change this to point to your local Node.js server (e.g., `http://localhost:3000/api`).

The frontend handles the search flow, pagination, and the submission of new crawl requests, providing immediate feedback (success/error messages) based on the API response.

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
