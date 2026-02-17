#how to run: python3 analysis_figures.py

import os
import psycopg2
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

def get_db():
    #connect to database
    return psycopg2.connect(dsn=DATABASE_URL)


# FIGURE 1: Sentiment Analysis 
def figure1_sentiment_analysis():
    #Simple sentiment comparison across platforms
    from textblob import TextBlob
    import re
    
    conn = get_db()
    
    print("Loading posts for sentiment analysis...")
    
    #4chan posts
    df_4chan = pd.read_sql("""
        SELECT data->>'com' as text
        FROM posts
        WHERE data->>'com' IS NOT NULL
        AND LENGTH(data->>'com') > 20
        LIMIT 2000
    """, conn)
    
    #reddit
    df_reddit = pd.read_sql("""
        SELECT data->>'body' as text
        FROM reddit_comments
        WHERE data->>'body' IS NOT NULL
        AND LENGTH(data->>'body') > 20
        LIMIT 2000
    """, conn)
    
    conn.close()
    
    print(f"Analyzing {len(df_4chan)} 4chan posts and {len(df_reddit)} Reddit posts...")
    
    #calculate sentiment
    def get_sentiment(text):
        try:
            text = re.sub(r'http\S+', '', str(text))
            blob = TextBlob(text)
            return blob.sentiment.polarity  
        except:
            return None
    
    df_4chan['sentiment'] = df_4chan['text'].apply(get_sentiment)
    df_reddit['sentiment'] = df_reddit['text'].apply(get_sentiment)
    
    df_4chan = df_4chan.dropna(subset=['sentiment'])
    df_reddit = df_reddit.dropna(subset=['sentiment'])
    
    print(f"Valid: 4chan={len(df_4chan)}, Reddit={len(df_reddit)}")
    
    #categorize
    def categorize(sentiment):
        if sentiment < -0.1:
            return 'Negative'
        elif sentiment > 0.1:
            return 'Positive'
        else:
            return 'Neutral'
    
    df_4chan['category'] = df_4chan['sentiment'].apply(categorize)
    df_reddit['category'] = df_reddit['sentiment'].apply(categorize)
    
    categories = ['Negative', 'Neutral', 'Positive']
    
    chan_counts = df_4chan['category'].value_counts()
    reddit_counts = df_reddit['category'].value_counts()
    
    chan_pcts = [(chan_counts.get(cat, 0) / len(df_4chan) * 100) for cat in categories]
    reddit_pcts = [(reddit_counts.get(cat, 0) / len(df_reddit) * 100) for cat in categories]
    
    #plot
    x = np.arange(len(categories))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.bar(x - width/2, chan_pcts, width, 
           label=f'4chan (n={len(df_4chan):,})', 
           color='#FF6B6B', alpha=0.8, edgecolor='black')
    ax.bar(x + width/2, reddit_pcts, width, 
           label=f'Reddit (n={len(df_reddit):,})', 
           color='#4ECDC4', alpha=0.8, edgecolor='black')
    
    ax.set_xlabel('Sentiment Category', fontsize=12, fontweight='bold')
    ax.set_ylabel('Percentage of Posts (%)', fontsize=12, fontweight='bold')
    ax.set_title('Sentiment Polarity Distribution - Cross-Platform Comparison', 
                 fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, axis='y')
    
    for i, (c_pct, r_pct) in enumerate(zip(chan_pcts, reddit_pcts)):
        ax.text(i - width/2, c_pct + 1, f'{c_pct:.1f}%', 
               ha='center', fontsize=10, fontweight='bold')
        ax.text(i + width/2, r_pct + 1, f'{r_pct:.1f}%', 
               ha='center', fontsize=10, fontweight='bold')
    
    chan_mean = df_4chan['sentiment'].mean()
    reddit_mean = df_reddit['sentiment'].mean()
    
    textstr = f'Mean Sentiment:\n4chan: {chan_mean:.3f}\nReddit: {reddit_mean:.3f}'
    ax.text(0.98, 0.98, textstr, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig('figure1_sentiment_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("Figure 1 done")


# FIGURE 2: Toxicity Distribution
def figure2_toxicity_distribution():
    conn = get_db()
    
    #4chan toxicity
    df_4chan = pd.read_sql("""
        SELECT (toxicity_scores->>'toxicity')::float as toxicity
        FROM posts WHERE toxicity_scores IS NOT NULL
    """, conn)
    
    #reddit toxicity
    df_reddit = pd.read_sql("""
        SELECT (toxicity_scores->>'toxicity')::float as toxicity
        FROM (
            SELECT toxicity_scores FROM reddit_posts WHERE toxicity_scores IS NOT NULL
            UNION ALL
            SELECT toxicity_scores FROM reddit_comments WHERE toxicity_scores IS NOT NULL
        ) AS combined
    """, conn)
    
    conn.close()
    
    #plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    #histogram
    ax.hist(df_4chan['toxicity'], bins=50, alpha=0.6, label=f'4chan (n={len(df_4chan):,})', color='#FF6B6B', edgecolor='black')
    ax.hist(df_reddit['toxicity'], bins=50, alpha=0.6, label=f'Reddit (n={len(df_reddit):,})', color='#4ECDC4', edgecolor='black')
    ax.set_xlabel('Toxicity Score', fontsize=12, fontweight='bold')
    ax.set_ylabel('Frequency', fontsize=12, fontweight='bold')
    ax.set_title('Perspective API Toxicity Distribution', fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('figure2_toxicity_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("Figure 2 done")


# FIGURE 3: Toxicity CDF
def figure3_toxicity_cdf():
    conn = get_db()
    
    #4chan toxicity
    df_4chan = pd.read_sql("""
        SELECT (toxicity_scores->>'toxicity')::float as toxicity
        FROM posts WHERE toxicity_scores IS NOT NULL
    """, conn)
    
    #reddit toxicity
    df_reddit = pd.read_sql("""
        SELECT (toxicity_scores->>'toxicity')::float as toxicity
        FROM (
            SELECT toxicity_scores FROM reddit_posts WHERE toxicity_scores IS NOT NULL
            UNION ALL
            SELECT toxicity_scores FROM reddit_comments WHERE toxicity_scores IS NOT NULL
        ) AS combined
    """, conn)
    
    conn.close()
    
    #sort and calculate CDF
    chan_sorted = np.sort(df_4chan['toxicity'])
    chan_cdf = np.arange(1, len(chan_sorted)+1) / len(chan_sorted)
    
    reddit_sorted = np.sort(df_reddit['toxicity'])
    reddit_cdf = np.arange(1, len(reddit_sorted)+1) / len(reddit_sorted)
    
    #plot
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(chan_sorted, chan_cdf, label='4chan', color='#FF6B6B', linewidth=2.5)
    ax.plot(reddit_sorted, reddit_cdf, label='Reddit', color='#4ECDC4', linewidth=2.5)
    
    #add reference lines
    ax.axvline(x=0.5, color='red', linestyle='--', alpha=0.5, linewidth=1.5, label='Moderate toxicity (0.5)')
    ax.axhline(y=0.5, color='gray', linestyle='--', alpha=0.3, linewidth=1)
    ax.axhline(y=0.75, color='gray', linestyle='--', alpha=0.3, linewidth=1)
    ax.axhline(y=0.25, color='gray', linestyle='--', alpha=0.3, linewidth=1)
    
    ax.set_xlabel('Toxicity Score', fontsize=12, fontweight='bold')
    ax.set_ylabel('Cumulative Probability', fontsize=12, fontweight='bold')
    ax.set_title('Toxicity CDF - Cumulative Distribution', fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    
    plt.tight_layout()
    plt.savefig('figure3_toxicity_cdf.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("Figure 3 done")


# FIGURE 4: Keyword Shift- Compare keyword usage during high vs low toxicity periods 
def figure4_keyword_shifts_during_spikes():
    conn = get_db()
    
    #posts with HIGH toxicity (>0.35)
    df_high = pd.read_sql("""
        SELECT data::text as text
        FROM posts
        WHERE toxicity_scores IS NOT NULL
        AND (toxicity_scores->>'toxicity')::float > 0.35
    """, conn)
    
    #posts with LOW toxicity (<=0.35)
    df_low = pd.read_sql("""
        SELECT data::text as text
        FROM posts
        WHERE toxicity_scores IS NOT NULL
        AND (toxicity_scores->>'toxicity')::float <= 0.35
    """, conn)
    
    conn.close()
    
    keywords = ['ukraine', 'russia', 'gaza', 'israel', 'trump', 
                'election', 'china', 'taiwan', 'iran', 'nato', 
                'jew', 'muslim', 'war', 'genocide']
    
    #count keyword frequencies
    high_counts = {}
    low_counts = {}
    
    for kw in keywords:
        high_counts[kw] = sum(df_high['text'].str.contains(kw, case=False, na=False))
        low_counts[kw] = sum(df_low['text'].str.contains(kw, case=False, na=False))
    
    #normalize by document count
    high_rates = {k: (v / len(df_high) * 100) for k, v in high_counts.items()}
    low_rates = {k: (v / len(df_low) * 100) for k, v in low_counts.items()}
    
    #plot
    x = np.arange(len(keywords))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(14, 6))
    
    ax.bar(x - width/2, [high_rates[k] for k in keywords], width, 
           label=f'High Toxicity (>0.35, n={len(df_high):,})', 
           color='#FF6B6B', alpha=0.8, edgecolor='black')
    ax.bar(x + width/2, [low_rates[k] for k in keywords], width, 
           label=f'Low Toxicity (≤0.35, n={len(df_low):,})', 
           color='#4ECDC4', alpha=0.8, edgecolor='black')
    
    ax.set_xlabel('Keywords', fontsize=12, fontweight='bold')
    ax.set_ylabel('Mention Rate (per 100 posts)', fontsize=12, fontweight='bold')
    ax.set_title('Keyword Usage During High vs Low Toxicity Posts', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(keywords, rotation=45, ha='right')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig('figure4_keyword_shifts_toxicity.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("Figure 4 done")

# FIGURE 5: Compare all 6 Perspective API attributes across platforms
def figure5_multi_attribute_toxicity():
    conn = get_db()
    cur = conn.cursor()
    
    #4chan: average of all toxicity attributes
    cur.execute("""
        SELECT 
            AVG((toxicity_scores->>'toxicity')::float) as toxicity,
            AVG((toxicity_scores->>'severe_toxicity')::float) as severe_toxicity,
            AVG((toxicity_scores->>'identity_attack')::float) as identity_attack,
            AVG((toxicity_scores->>'insult')::float) as insult,
            AVG((toxicity_scores->>'profanity')::float) as profanity,
            AVG((toxicity_scores->>'threat')::float) as threat
        FROM posts WHERE toxicity_scores IS NOT NULL
    """)
    chan_scores = cur.fetchone()
    
    #reddit: average of all toxicity attributes  
    cur.execute("""
        SELECT 
            AVG((toxicity_scores->>'toxicity')::float) as toxicity,
            AVG((toxicity_scores->>'severe_toxicity')::float) as severe_toxicity,
            AVG((toxicity_scores->>'identity_attack')::float) as identity_attack,
            AVG((toxicity_scores->>'insult')::float) as insult,
            AVG((toxicity_scores->>'profanity')::float) as profanity,
            AVG((toxicity_scores->>'threat')::float) as threat
        FROM (
            SELECT toxicity_scores FROM reddit_posts WHERE toxicity_scores IS NOT NULL
            UNION ALL
            SELECT toxicity_scores FROM reddit_comments WHERE toxicity_scores IS NOT NULL
        ) AS combined
    """)
    reddit_scores = cur.fetchone()
    
    cur.close()
    conn.close()
    
    #handle None values
    chan_scores = [s if s is not None else 0 for s in chan_scores]
    reddit_scores = [s if s is not None else 0 for s in reddit_scores]
    
    attributes = ['Toxicity', 'Severe\nToxicity', 'Identity\nAttack', 'Insult', 'Profanity', 'Threat']
    
    #create figure with 2 subplots
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    #left is bar chart comparison
    x = np.arange(len(attributes))
    width = 0.35
    
    axes[0].bar(x - width/2, chan_scores, width, label='4chan', 
                color='#FF6B6B', alpha=0.8, edgecolor='black')
    axes[0].bar(x + width/2, reddit_scores, width, label='Reddit', 
                color='#4ECDC4', alpha=0.8, edgecolor='black')
    
    axes[0].set_xlabel('Toxicity Attributes', fontsize=12, fontweight='bold')
    axes[0].set_ylabel('Average Score', fontsize=12, fontweight='bold')
    axes[0].set_title('Multi-Attribute Toxicity Comparison', fontsize=13, fontweight='bold')
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(attributes)
    axes[0].legend(fontsize=11)
    axes[0].grid(True, alpha=0.3, axis='y')
    axes[0].set_ylim(0, max(max(chan_scores), max(reddit_scores)) * 1.2)
    
    #right is ratio (4chan / Reddit ratio)
    ratios = []
    for c, r in zip(chan_scores, reddit_scores):
        if r > 0:
            ratios.append(c / r)
        else:
            ratios.append(0)
    
    colors_ratio = ['#FF6B6B' if r > 1 else '#4ECDC4' for r in ratios]
    
    axes[1].barh(attributes, ratios, color=colors_ratio, alpha=0.8, edgecolor='black')
    axes[1].axvline(x=1.0, color='black', linestyle='--', linewidth=2, 
                    label='Equal (1.0x)', alpha=0.7)
    axes[1].set_xlabel('4chan/Reddit Ratio', fontsize=12, fontweight='bold')
    axes[1].set_title('Relative Toxicity: How Much Higher is 4chan?', fontsize=13, fontweight='bold')
    axes[1].legend(fontsize=10)
    axes[1].grid(True, alpha=0.3, axis='x')
    
    #add value labels on bars
    for i, (attr, ratio) in enumerate(zip(attributes, ratios)):
        axes[1].text(ratio + 0.05, i, f'{ratio:.2f}x', 
                     va='center', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('figure5_multi_attribute_toxicity.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("Figure 5 done")


# FIGURE 6: Which Platform Reacts First
def figure6_event_response_timeline():
    """
    Compare when platforms discuss major events
    Automatically finds peak dates for keywords
    Tests: Does 4chan spike before Reddit during events?
    """
    conn = get_db()
    
    #find peak dates for each keyword
    print("Finding peak dates for keywords...")
    
    #find top Ukraine date
    ukraine_peak = pd.read_sql("""
        SELECT DATE(created_at) as date, COUNT(*) as mentions
        FROM posts
        WHERE data::text ILIKE '%ukraine%'
        GROUP BY DATE(created_at)
        ORDER BY mentions DESC
        LIMIT 1
    """, conn)
    
    #find top Gaza date
    gaza_peak = pd.read_sql("""
        SELECT DATE(created_at) as date, COUNT(*) as mentions
        FROM posts
        WHERE data::text ILIKE '%gaza%'
        GROUP BY DATE(created_at)
        ORDER BY mentions DESC
        LIMIT 1
    """, conn)
    
    #build events dictionary from actual data
    events = {}
    
    if len(ukraine_peak) > 0:
        events['Ukraine'] = str(ukraine_peak['date'].iloc[0])
        print(f"Ukraine peak: {events['Ukraine']} ({ukraine_peak['mentions'].iloc[0]} mentions)")
    
    if len(gaza_peak) > 0:
        events['Gaza'] = str(gaza_peak['date'].iloc[0])
        print(f"Gaza peak: {events['Gaza']} ({gaza_peak['mentions'].iloc[0]} mentions)")
    
    if len(events) == 0:
        print("WARNING: No keyword peaks found, skipping figure")
        conn.close()
        return
    
    #for each event, look at ±3 days of activity
    fig, axes = plt.subplots(len(events), 1, figsize=(14, 4*len(events)))
    
    if len(events) == 1:
        axes = [axes]
    
    for idx, (keyword, event_date) in enumerate(events.items()):
        #4chan activity around event
        df_4chan = pd.read_sql(f"""
            SELECT 
                DATE(created_at) as date,
                COUNT(*) as post_count
            FROM posts
            WHERE created_at >= '{event_date}'::date - interval '3 days'
            AND created_at <= '{event_date}'::date + interval '3 days'
            AND data::text ILIKE '%{keyword.lower()}%'
            GROUP BY DATE(created_at)
            ORDER BY date
        """, conn)
        
        #reddit activity around event
        df_reddit = pd.read_sql(f"""
            SELECT 
                DATE(created_at) as date,
                COUNT(*) as post_count
            FROM (
                SELECT created_at, data FROM reddit_posts
                WHERE data::text ILIKE '%{keyword.lower()}%'
                UNION ALL
                SELECT created_at, data FROM reddit_comments
                WHERE data::text ILIKE '%{keyword.lower()}%'
            ) AS combined
            WHERE created_at >= '{event_date}'::date - interval '3 days'
            AND created_at <= '{event_date}'::date + interval '3 days'
            GROUP BY DATE(created_at)
            ORDER BY date
        """, conn)
        
        df_4chan['date'] = pd.to_datetime(df_4chan['date'])
        df_reddit['date'] = pd.to_datetime(df_reddit['date'])
        
        print(f"{keyword}: 4chan={len(df_4chan)} days, Reddit={len(df_reddit)} days")
        
        #normalize to percentages for comparison
        if len(df_4chan) > 0:
            df_4chan['pct'] = (df_4chan['post_count'] / df_4chan['post_count'].max() * 100)
        if len(df_reddit) > 0:
            df_reddit['pct'] = (df_reddit['post_count'] / df_reddit['post_count'].max() * 100)
        
        #plot
        if len(df_4chan) > 0:
            axes[idx].plot(df_4chan['date'], df_4chan['pct'], 
                          label='4chan', color='#FF6B6B', linewidth=2.5, marker='o', markersize=8)
        if len(df_reddit) > 0:
            axes[idx].plot(df_reddit['date'], df_reddit['pct'], 
                          label='Reddit', color='#4ECDC4', linewidth=2.5, marker='s', markersize=8)
        
        #mark the event day
        axes[idx].axvline(x=pd.to_datetime(event_date), color='red', linestyle='--', 
                         linewidth=2, alpha=0.7, label='Peak day')
        
        axes[idx].set_ylabel('Activity (% of peak)', fontsize=11, fontweight='bold')
        axes[idx].set_title(f'{keyword} Discussion Around Peak Day ({event_date})', 
                           fontsize=12, fontweight='bold')
        axes[idx].legend(fontsize=10)
        axes[idx].grid(True, alpha=0.3)
        axes[idx].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    
    axes[-1].set_xlabel('Date', fontsize=12, fontweight='bold')
    plt.tight_layout()
    plt.savefig('figure6_event_response_timeline.png', dpi=300, bbox_inches='tight')
    plt.close()
    conn.close()
    print("Figure 6 done")

# FIGURE 7: /pol/ Threads Daily (Nov 1-14)
def figure7_pol_threads_daily():
    conn = get_db()
    
    #count threads where the OP post was made that day
    df = pd.read_sql("""
        SELECT DATE(created_at) as date, 
               COUNT(*) as thread_count
        FROM posts
        WHERE board_name = 'pol'
        AND created_at >= '2025-11-01' AND created_at < '2025-11-15'
        AND (data->>'resto')::text = '0'
        GROUP BY DATE(created_at) 
        ORDER BY date
    """, conn)
    
    df['date'] = pd.to_datetime(df['date'])
    conn.close()
    
    #plot
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar(df['date'], df['thread_count'], color='#FF6B6B', alpha=0.8, edgecolor='black', width=0.8)
    
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Thread Count', fontsize=12, fontweight='bold')
    ax.set_title('/pol/ Daily Threads (Nov 1-14, 2025)', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='y')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    plt.xticks(rotation=45)
    
    #add counts on bars
    for date, count in zip(df['date'], df['thread_count']):
        ax.text(date, count, f'{count:,}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('figure7_pol_threads_daily.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("Figure 7 done")

# FIGURE 8: /pol/ Posts Hourly (Nov 1-14)
def figure8_pol_posts_hourly():
    conn = get_db()
    
    df = pd.read_sql("""
        SELECT DATE_TRUNC('hour', created_at) as hour, COUNT(*) as post_count
        FROM posts
        WHERE board_name = 'pol'
        AND created_at >= '2025-11-01' AND created_at < '2025-11-15'
        GROUP BY hour ORDER BY hour
    """, conn)
    df['hour'] = pd.to_datetime(df['hour'])
    conn.close()
    
    #plot
    fig, ax = plt.subplots(figsize=(16, 6))
    ax.plot(df['hour'], df['post_count'], color='#FF6B6B', linewidth=1.5, marker='o', markersize=3)
    ax.fill_between(df['hour'], df['post_count'], alpha=0.3, color='#FF6B6B')
    
    ax.set_xlabel('Date/Time', fontsize=12, fontweight='bold')
    ax.set_ylabel('Post Count', fontsize=12, fontweight='bold')
    ax.set_title('/pol/ Hourly Posts (Nov 1-14, 2025)', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig('figure8_pol_posts_hourly.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("Figure 8 done")

#generate all figures
def generate_all():
    figure1_sentiment_analysis()
    figure2_toxicity_distribution()
    figure3_toxicity_cdf()  
    figure4_keyword_shifts_during_spikes()
    figure5_multi_attribute_toxicity()
    figure6_event_response_timeline()
    figure7_pol_threads_daily()
    figure8_pol_posts_hourly()
    print("DONE")

if __name__ == "__main__":
    generate_all()
