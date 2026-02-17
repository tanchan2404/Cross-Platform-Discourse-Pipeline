import os
import psycopg2
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def plot_collection_over_time(conn):
    #this makes the plot showing how much data we collected over time
    cur = conn.cursor()
    
    cur.execute("""
        SELECT date_trunc('hour', created_at)::timestamp AS hour, COUNT(*) as post_count
        FROM posts
        GROUP BY hour
        ORDER BY hour
    """)
    chan_data = cur.fetchall()
    
    cur.execute("""
        SELECT date_trunc('hour', (created_at AT TIME ZONE 'UTC'))::timestamp AS hour, COUNT(*) as post_count
        FROM reddit_posts
        GROUP BY hour
        ORDER BY hour
    """)
    reddit_posts = cur.fetchall()
    
    cur.execute("""
        SELECT date_trunc('hour', (created_at AT TIME ZONE 'UTC'))::timestamp AS hour, COUNT(*) as comment_count
        FROM reddit_comments
        GROUP BY hour
        ORDER BY hour
    """)
    reddit_comments = cur.fetchall()
    
    cur.close()
    
    # aggregate everything
    chan_by_hour = {}
    for hour, count in chan_data:
        chan_by_hour[hour] = count
    
    reddit_by_hour = {}
    for hour, count in reddit_posts:
        if hour in reddit_by_hour:
            reddit_by_hour[hour] += count
        else:
            reddit_by_hour[hour] = count
    
    for hour, count in reddit_comments:
        if hour in reddit_by_hour:
            reddit_by_hour[hour] += count
        else:
            reddit_by_hour[hour] = count
    

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    #posts per hour
    if chan_by_hour:
        hours = sorted(chan_by_hour.keys())
        counts = [chan_by_hour[h] for h in hours]
        ax1.plot(hours, counts, label='4chan', linewidth=2, marker='o', markersize=3, color='#ff4444')
    
    if reddit_by_hour:
        hours = sorted(reddit_by_hour.keys())
        counts = [reddit_by_hour[h] for h in hours]
        ax1.plot(hours, counts, label='Reddit', linewidth=2, marker='s', markersize=3, color='#4444ff')
    
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Posts/Comments per Hour')
    ax1.set_title('Data Collection Over Time', fontsize=14, fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # plot 2: cumulative
    if chan_by_hour and reddit_by_hour:
        all_hours = sorted(set(chan_by_hour.keys()) | set(reddit_by_hour.keys()))
        
        cumulative_chan = []
        cumulative_reddit = []
        total_c = 0
        total_r = 0
        
        for h in all_hours:
            total_c += chan_by_hour.get(h, 0)
            total_r += reddit_by_hour.get(h, 0)
            cumulative_chan.append(total_c)
            cumulative_reddit.append(total_r)
        
        ax2.plot(all_hours, cumulative_chan, label='4chan', linewidth=2, color='#ff4444')
        ax2.plot(all_hours, cumulative_reddit, label='Reddit', linewidth=2, color='#4444ff')
        
        ax2.set_xlabel('Time')
        ax2.set_ylabel('Cumulative Count')
        ax2.set_title('Cumulative Collection', fontsize=14, fontweight='bold')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig('data_collection_over_time.png', dpi=300)
    print("saved: data_collection_over_time.png")
    plt.close()

def plot_keyword_trends(conn):
    #plot some keywords over time

    cur = conn.cursor()
    
    keywords = ['ukraine', 'gaza', 'taiwan', 'election']
    
    fig, axes = plt.subplots(len(keywords), 1, figsize=(12, 3*len(keywords)))
    if len(keywords) == 1:
        axes = [axes]
    
    for i, keyword in enumerate(keywords):
        # 4chan mentions per day
        cur.execute(f"""
            SELECT date_trunc('day', created_at) as day, COUNT(*) as count
            FROM posts
            WHERE data::text ILIKE '%{keyword}%'
            GROUP BY day
            ORDER BY day
        """)
        chan_data = cur.fetchall()
        
        # reddit mentions per day
        cur.execute(f"""
            SELECT date_trunc('day', created_at) as day, COUNT(*) as count
            FROM reddit_posts
            WHERE data::text ILIKE '%{keyword}%'
            GROUP BY day
            ORDER BY day
        """)
        reddit_data = cur.fetchall()
        
        ax = axes[i]
        
        if chan_data:
            days = [d[0] for d in chan_data]
            counts = [d[1] for d in chan_data]
            ax.plot(days, counts, label='4chan', marker='o', linewidth=2, color='#ff4444')
        
        if reddit_data:
            days = [d[0] for d in reddit_data]
            counts = [d[1] for d in reddit_data]
            ax.plot(days, counts, label='Reddit', marker='s', linewidth=2, color='#4444ff')
        
        ax.set_title(f'"{keyword}" mentions over time', fontweight='bold')
        ax.set_ylabel('Posts per day')
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    axes[-1].set_xlabel('Date')
    plt.tight_layout()
    plt.savefig('keyword_trends.png', dpi=300)
    print("saved: keyword_trends.png")
    plt.close()
    cur.close()

def print_stats(conn):
    #print out stats about what we collected
    cur = conn.cursor()
    
    # counts
    cur.execute("SELECT COUNT(*) FROM posts")
    total_chan = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM reddit_posts")
    total_reddit_posts = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM reddit_comments")
    total_reddit_comments = cur.fetchone()[0]
    
    cur.close()

if __name__ == "__main__":
    DATABASE_URL = os.environ.get("DATABASE_URL")
    conn = psycopg2.connect(dsn=DATABASE_URL)
    
    plot_collection_over_time(conn)
    plot_keyword_trends(conn)
    
    print_stats(conn)
    
    conn.close()
