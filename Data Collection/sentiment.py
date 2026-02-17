import os
import csv
import matplotlib.pyplot as plt
import numpy as np
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def analyze_sentiment(csv_file, platform):
    analyzer = SentimentIntensityAnalyzer()
    scores = []
    keyword_scores = {}

    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            text = ''
            if 'text' in row and row['text']:
                text = row['text']
            elif 'body' in row and row['body']:
                text = row['body']
            elif 'title' in row:
                text = row.get('title', '') + ' ' + row.get('body', '')

            if not text or len(text.strip()) < 10:
                continue

            result = analyzer.polarity_scores(text)
            compound = result['compound']
            scores.append(compound)

            text_lower = text.lower()
            keywords = ['ukraine', 'gaza', 'israel', 'taiwan', 'election', 'trump', 'russia']
            for kw in keywords:
                if kw in text_lower:
                    keyword_scores.setdefault(kw, []).append(compound)

    return scores, keyword_scores

def categorize(score):
    if score >= 0.05:
        return 'positive'
    elif score <= -0.05:
        return 'negative'
    else:
        return 'neutral'


def make_plots():
    chan_scores, chan_kw = analyze_sentiment('filtered_4chan.csv', '4chan')
    reddit_scores, reddit_kw = analyze_sentiment('filtered_reddit.csv', 'Reddit')

    fig = plt.figure(figsize=(12, 6))

    #sentiment categories
    ax1 = plt.subplot(1, 2, 1)

    chan_cats = [categorize(s) for s in chan_scores]
    reddit_cats = [categorize(s) for s in reddit_scores]

    chan_pos = chan_cats.count('positive') / len(chan_cats) * 100
    chan_neu = chan_cats.count('neutral') / len(chan_cats) * 100
    chan_neg = chan_cats.count('negative') / len(chan_cats) * 100

    reddit_pos = reddit_cats.count('positive') / len(reddit_cats) * 100
    reddit_neu = reddit_cats.count('neutral') / len(reddit_cats) * 100
    reddit_neg = reddit_cats.count('negative') / len(reddit_cats) * 100

    x = np.arange(3)
    width = 0.35

    ax1.bar(x - width/2, [chan_neg, chan_neu, chan_pos], width, label='4chan', color='red', alpha=0.7)
    ax1.bar(x + width/2, [reddit_neg, reddit_neu, reddit_pos], width, label='Reddit', color='blue', alpha=0.7)
    ax1.set_ylabel('Percentage (%)')
    ax1.set_title('Sentiment Categories', fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(['Negative', 'Neutral', 'Positive'])
    ax1.legend()
    ax1.grid(alpha=0.3, axis='y')

    #keyword comparison
    ax2 = plt.subplot(1, 2, 2)

    common_kw = [
        kw for kw in ['ukraine', 'gaza', 'israel', 'taiwan', 'election', 'trump']
        if kw in chan_kw and len(chan_kw[kw]) > 5 and kw in reddit_kw and len(reddit_kw[kw]) > 5
    ]

    if common_kw:
        y_pos = np.arange(len(common_kw))
        width = 0.35
        chan_avgs = [np.mean(chan_kw[kw]) for kw in common_kw]
        reddit_avgs = [np.mean(reddit_kw[kw]) for kw in common_kw]

        ax2.barh(y_pos - width/2, chan_avgs, width, label='4chan', color='red', alpha=0.7)
        ax2.barh(y_pos + width/2, reddit_avgs, width, label='Reddit', color='blue', alpha=0.7)
        ax2.set_yticks(y_pos)
        ax2.set_yticklabels([k.capitalize() for k in common_kw])
        ax2.set_xlabel('Average Sentiment')
        ax2.set_title('Sentiment by Keyword', fontweight='bold')
        ax2.axvline(0, color='black', linestyle='--', alpha=0.3)
        ax2.legend()
        ax2.grid(alpha=0.3, axis='x')

    plt.tight_layout()
    plt.savefig('sentiment_analysis.png', dpi=300)
    print("saved: sentiment_analysis.png")
    plt.close()

    #stats
    print(f"\n4chan (n={len(chan_scores)}):")
    print(f"  positive: {chan_pos:.1f}%")
    print(f"  neutral: {chan_neu:.1f}%")
    print(f"  negative: {chan_neg:.1f}%")

    print(f"\nReddit (n={len(reddit_scores)}):")
    print(f"  positive: {reddit_pos:.1f}%")
    print(f"  neutral: {reddit_neu:.1f}%")
    print(f"  negative: {reddit_neg:.1f}%")

if __name__ == "__main__":
    if not os.path.exists('filtered_4chan.csv'):
        print("error: need filtered_4chan.csv - run keyword_filter.py first")
        exit(1)
    if not os.path.exists('filtered_reddit.csv'):
        print("error: need filtered_reddit.csv - run keyword_filter.py first")
        exit(1)

    make_plots()
