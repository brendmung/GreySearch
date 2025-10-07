const express = require('express');
const { MongoClient } = require('mongodb');
const dotenv = require('dotenv');
const cors = require('cors');

// Load environment variables from .env file
dotenv.config();

const app = express();
const port = process.env.PORT || 3000;
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'greysearch_db';
const RESULTS_COLLECTION = 'pages';
// NEW: Collection for user-submitted crawl requests
const REQUESTS_COLLECTION = 'crawl_requests';

let db;

// Middleware
// Configure CORS to allow requests from the frontend (adjust origin if needed)
app.use(cors({ origin: '*' }));
app.use(express.json());

// --- Database Connection ---
async function connectDB() {
    if (!MONGO_URI) {
        console.error("FATAL: MONGO_URI is not defined.");
        process.exit(1);
    }
    try {
        const client = new MongoClient(MONGO_URI);
        await client.connect();
        db = client.db(DB_NAME);
        // Ensure index on the request URL for quick checks
        await db.collection(REQUESTS_COLLECTION).createIndex({ url: 1 }, { unique: true });

        console.log(`[API] Connected successfully to MongoDB database: ${DB_NAME}`);
    } catch (error) {
        console.error("[API ERROR] Failed to connect to MongoDB:", error);
        process.exit(1);
    }
}

// --- API Endpoints ---

// 1. Search Endpoint (Existing)
// Uses MongoDB's $text search capability for relevance ranking
app.get('/api/search', async (req, res) => {
    const query = req.query.q;
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const skip = (page - 1) * limit;

    if (!query) {
        return res.status(400).json({ error: "Query parameter 'q' is required." });
    }

    const startTime = process.hrtime();

    try {
        const searchPipeline = [
            {
                $match: {
                    // $text requires the text_search_index created by the Python crawler
                    $text: { $search: query }
                }
            },
            {
                $sort: {
                    score: { $meta: "textScore" }, // Sort by relevance score
                    crawled_at: -1
                }
            },
            {
                $facet: {
                    metadata: [{ $count: "totalResults" }],
                    data: [{ $skip: skip }, { $limit: limit }]
                }
            }
        ];

        const results = await db.collection(RESULTS_COLLECTION).aggregate(searchPipeline).toArray();

        const totalResults = results[0].metadata[0] ? results[0].metadata[0].totalResults : 0;
        const data = results[0].data;

        const hrTime = process.hrtime(startTime);
        const timeTaken = hrTime[0] + hrTime[1] / 1e9; // Time in seconds

        res.json({
            query: query,
            totalResults: totalResults,
            timeTaken: timeTaken.toFixed(3),
                 page: page,
                 limit: limit,
                 results: data
        });

    } catch (error) {
        console.error("Search failed:", error);
        res.status(500).json({ error: "Internal server error during search." });
    }
});

// 2. Stats Endpoint (Existing)
app.get('/api/stats', async (req, res) => {
    try {
        const count = await db.collection(RESULTS_COLLECTION).countDocuments({});
        res.json({ indexedCount: count });
    } catch (error) {
        console.error("Stats failed:", error);
        res.status(500).json({ error: "Internal server error fetching stats." });
    }
});


// 3. NEW: Crawl Request Endpoint
app.post('/api/request_crawl', async (req, res) => {
    const { url } = req.body;

    if (!url) {
        return res.status(400).json({ error: "URL is required." });
    }

    // Basic URL validation (you might want more robust validation)
    if (!url.startsWith('http://') && !url.startsWith('https://')) {
        return res.status(400).json({ error: "URL must start with http:// or https://." });
    }

    try {
        const result = await db.collection(REQUESTS_COLLECTION).insertOne({
            url: url.trim(),
                                                                          requested_at: new Date(),
                                                                          status: 'pending'
        });

        res.status(201).json({
            message: "Crawl request submitted successfully. It will be processed shortly.",
            id: result.insertedId
        });

    } catch (error) {
        if (error.code === 11000) { // MongoDB duplicate key error
            return res.status(409).json({ error: "This URL has already been requested." });
        }
        console.error("Crawl request failed:", error);
        res.status(500).json({ error: "Internal server error during request submission." });
    }
});


// Start the server
connectDB().then(() => {
    app.listen(port, () => {
        console.log(`[API] GreySearch API running on http://localhost:${port}`);
    });
});
