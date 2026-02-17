#Flask Dashboard for Interactive Toxicity Analysis


from flask import Flask, render_template, jsonify, request
from analysis import ToxicityAnalyzer
import os
from dotenv import load_dotenv

load_dotenv()

#create Flask app
app = Flask(__name__)

analyzer = ToxicityAnalyzer()

@app.route('/')
def index():
    #Main dashboard page
    return render_template('index.html')

@app.route('/api/toxicity-distribution')
def toxicity_distribution():
    """
    API endpoint for Analysis 1: Toxicity Distribution
    Returns: JSON with histogram and CDF data for Plotly charts
    """
    # Get user filter choices from the URL parameters
    platform = request.args.get('platform', 'all')  
    start_date = request.args.get('start_date')     
    end_date = request.args.get('end_date')
    
    # Call analyzer to put numbers
    result = analyzer.get_toxicity_distribution(
        platform=platform,
        start_date=start_date,
        end_date=end_date
    )
    
    # Send results back as JSON
    return jsonify(result)

@app.route('/api/keyword-analysis')
def keyword_analysis():
    platform = request.args.get('platform', 'all')
    threshold = float(request.args.get('threshold', 0.35))
    keywords_str = request.args.get('keywords', '')
    
    keywords = [k.strip() for k in keywords_str.split(',') if k.strip()]
    
    result = analyzer.get_keyword_frequency(
        platform=platform,
        threshold=threshold,
        keywords=keywords
    )
    
    return jsonify(result)

@app.route('/api/multi-attribute')
def multi_attribute():
    platform = request.args.get('platform', 'all')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    show_ratio = request.args.get('show_ratio', 'false') == 'true'
    
    result = analyzer.get_multi_attribute_toxicity(
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        show_ratio=show_ratio
    )
    
    return jsonify(result)

@app.route('/api/temporal-analysis')
def temporal_analysis():
    keyword = request.args.get('keyword', 'ukraine')
    window_days = int(request.args.get('window_days', 3))
    platform = request.args.get('platform', 'all')
    metric = request.args.get('metric', 'volume')  # 'volume' or 'toxicity'
    
    result = analyzer.get_temporal_analysis(
        keyword=keyword,
        window_days=window_days,
        platform=platform,
        metric=metric
    )
    
    return jsonify(result)

@app.route('/api/tfidf-analysis')
def tfidf_analysis():
    platform = request.args.get('platform', '4chan')
    threshold = float(request.args.get('threshold', 0.35))
    top_n = int(request.args.get('top_n', 20))
    
    result = analyzer.get_tfidf_toxic_words(
        platform=platform,
        threshold=threshold,
        top_n=top_n
    )
    
    return jsonify(result)

if __name__ == '__main__':
    
    print("\n" + "-"*25)
    print("Starting Toxicity Analysis Dashboard")
    print("-"*25)
    print("\nDashboard at: http://localhost:5000")
    
    app.run(debug=True, host='0.0.0.0', port=5000)