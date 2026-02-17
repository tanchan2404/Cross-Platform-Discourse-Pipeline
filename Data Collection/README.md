This repo contains scripts for **data collection, and analysis** across Reddit and 4chan.  
It includes crawlers, data exploration, and sentiment plot scripts.

### Core Scripts
1. **`reddit_client.py`** – Reddit API client to fetch posts and comments.  
2. **`reddit_crawler.py`** – Manages Reddit data collection jobs.  
3. **`chan_client.py`** – 4chan API client to fetch threads and posts.  
4. **`chan_crawler.py`** – Manages 4chan data collection and storage.

---

### Data Validation Scripts
- **`check_data.py`**  : Checks the total number of posts/comments collected

---

### Data Exploration Scripts

### 1. Keyword Filtering
**`keyword_filter.py`** : Generates CSV files containing posts/comments related to selected geopolitical keywords (random words selected based on current affairs).

**Generated files:**
- `filtered_4chan.csv`  
- `filtered_reddit.csv`  
- `filtered_reddit_comments.csv`

---

### 2. Volume Analysis
**`volume_analysis.py`** : Plots data collection volume over time for both platforms and tracks keyword trends.

**Generated plots:**
- `keyword_trends.png`  
- `data_collection_over_time.png`

---

### 3. Sentiment Analysis
**`sentiment.py`** : Performs basic sentiment analysis using VADER on the filtered CSVs generated earlier.

**Generated plots:**
- `sentiment_analysis.png`

---

## How to Run

```bash
# Activate virtual environment
source .venv/bin/activate

# Run crawlers
python3 reddit_crawler.py
python3 chan_crawler.py

# Check collected data
python3 check_data.py

# Generate filtered keyword CSVs
python3 keyword_filter.py

# Plot collection volume and trends
python3 volume_analysis.py

# Run sentiment analysis
python3 sentiment.py
