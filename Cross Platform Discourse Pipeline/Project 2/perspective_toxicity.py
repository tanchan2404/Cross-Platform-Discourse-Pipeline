#how to run- python3 perspective_toxicity.py all

import os
import time
import psycopg2
from googleapiclient import discovery
import json
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
PERSPECTIVE_API_KEY = os.getenv("PERSPECTIVE_API_KEY")

def get_api_client():
    #connect to Perspective API
    return discovery.build(
        "commentanalyzer", "v1alpha1",
        developerKey=PERSPECTIVE_API_KEY,
        discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
        static_discovery=False,
    )

def get_toxicity_score(text, client):
    #get toxicity scores from Perspective API
    #skip empty or short text
    if not text or len(text.strip()) < 3:
        return None
    
    text = text[:20000]  #api limit
    
    #build request with all 6 toxicity attributes
    request = {
        'comment': {'text': text},
        'requestedAttributes': {
            'TOXICITY': {}, 
            'SEVERE_TOXICITY': {}, 
            'IDENTITY_ATTACK': {},
            'INSULT': {}, 
            'PROFANITY': {}, 
            'THREAT': {}
        },
        'languages': ['en']
    }
    
    try:
        #call API and extract scores
        response = client.comments().analyze(body=request).execute()
        return {attr.lower(): data['summaryScore']['value'] 
                for attr, data in response.get('attributeScores', {}).items()}
    except:
        return None

def process_platform(table, text_field, id_fields, platform_name):
    #process all posts from a platform
    print(f"\n--- {platform_name} ---")
    
    client = get_api_client()
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()
    
    #create toxicity_scores column if it doesn't exist
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS toxicity_scores JSONB")
        conn.commit()
    except:
        conn.rollback()
    
    #count how many posts need processing
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE toxicity_scores IS NULL AND {text_field} IS NOT NULL")
    total = cur.fetchone()[0]
    print(f"Posts to process: {total:,}")
    
    if total == 0:
        print("Nothing to process!")
        cur.close()
        conn.close()
        return
    
    #estimated time (1.05 sec per post due to rate limit)
    print(f"Estimated time: {(total * 1.05 / 3600):.1f} hrs")
    
    processed = 0
    
    try:
        #process in batches until all posts are done
        while True:
            #get next batch of 100 posts
            cur.execute(f"""
                SELECT {', '.join(id_fields)}, {text_field}
                FROM {table}
                WHERE toxicity_scores IS NULL AND {text_field} IS NOT NULL
                LIMIT 100
            """)
            rows = cur.fetchall()
            
            #if no more posts, done
            if not rows:
                break
            
            #process each post in batch
            for row in rows:
                *ids, text = row
                
                #get toxicity scores from API
                scores = get_toxicity_score(text, client)
                
                #save scores to database
                if scores:
                    where_clause = ' AND '.join([f"{field} = %s" for field in id_fields])
                    cur.execute(f"""
                        UPDATE {table} SET toxicity_scores = %s WHERE {where_clause}
                    """, (json.dumps(scores), *ids))
                
                processed += 1
                
                #progress every 100 posts
                if processed % 100 == 0:
                    print(f"  {processed}/{total}")
                
                # Wait 1.05 seconds between requests (API rate limit is 1 per second)
                time.sleep(1.05)
            
            conn.commit()
    
    except KeyboardInterrupt:
        print("\nstopped, run again to continue")
        conn.commit()
    
    cur.close()
    conn.close()
    print(f"Processed {processed:,} posts")

def process_all():
    #process all platforms in sequence
    print("\n" + "-"*25)
    print("PROCESSING ALL PLATFORMS")
    print("-"*25)
    start = time.time()
    
    try:
        #4chan posts
        process_platform('posts', "data->>'com'", 
                        ['board_name', 'thread_number', 'post_number'], '4chan')
        
        #reddit posts
        process_platform('reddit_posts', "COALESCE(data->>'selftext', title, '')",
                        ['subreddit', 'post_id'], 'Reddit Posts')
        
        #reddit comments
        process_platform('reddit_comments', "data->>'body'",
                        ['subreddit', 'post_id', 'comment_id'], 'Reddit Comments')
        
        print(f"\nDONE")
    
    except KeyboardInterrupt:
        #allow stopping with Ctrl+C
        print("\nstopped, run again to continue")

if __name__ == "__main__":
    import sys
    
    #check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python3 perspective_toxicity.py [4chan|reddit|reddit_comments|all]")
        sys.exit(1)
    
    platform = sys.argv[1]
    
    if platform == "all":
        process_all()
    elif platform == "4chan":
        process_platform('posts', "data->>'com'", 
                        ['board_name', 'thread_number', 'post_number'], '4chan')
    elif platform == "reddit":
        process_platform('reddit_posts', "COALESCE(data->>'selftext', title, '')",
                        ['subreddit', 'post_id'], 'Reddit Posts')
    elif platform == "reddit_comments":
        process_platform('reddit_comments', "data->>'body'",
                        ['subreddit', 'post_id', 'comment_id'], 'Reddit Comments')
    else:
        print("Invalid. Use: 4chan, reddit, reddit_comments, or all")