"""
Simple Flask server to create download links for the market cap data files.
"""

from flask import Flask, send_file, render_template_string
import os

app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>T2D Pulse Market Cap Data</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            line-height: 1.6;
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            border: 1px solid #ddd;
            border-radius: 5px;
        }
        h1 {
            color: #333;
        }
        .file-list {
            margin-top: 20px;
        }
        .file-item {
            margin-bottom: 15px;
            padding: 10px;
            background-color: #f9f9f9;
            border-radius: 4px;
        }
        .file-link {
            display: inline-block;
            padding: 8px 16px;
            background-color: #4CAF50;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            margin-top: 5px;
        }
        .file-link:hover {
            background-color: #45a049;
        }
        .file-description {
            margin-top: 5px;
            color: #666;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>T2D Pulse Market Cap Data Files</h1>
        <p>Here are the market cap data files you requested:</p>
        
        <div class="file-list">
            {% for file in files %}
            <div class="file-item">
                <h3>{{ file.name }}</h3>
                <div class="file-description">{{ file.description }}</div>
                <a href="{{ file.url }}" class="file-link" download>Download</a>
            </div>
            {% endfor %}
        </div>
    </div>
</body>
</html>
"""

@app.route('/')
def index():
    """Home page with links to download the files"""
    files = [
        {
            'name': 'Market Cap Table (Text)',
            'description': 'Formatted text file showing sector market caps in billions USD for the past 30 days',
            'url': '/download/30day_sector_marketcap_table.txt'
        },
        {
            'name': 'Market Cap Table (CSV)',
            'description': 'Raw CSV data with complete market cap values for all sectors',
            'url': '/download/sector_marketcap_table.csv'
        },
        {
            'name': 'Market Cap Data (JSON)',
            'description': 'Complete market cap data in JSON format with full precision',
            'url': '/download/complete_market_cap_data.json'
        },
        {
            'name': 'Market Cap Analysis (Excel)',
            'description': 'Excel spreadsheet with sector market cap analysis',
            'url': '/download/30day_sector_marketcap_analysis.xlsx'
        }
    ]
    return render_template_string(HTML_TEMPLATE, files=files)

@app.route('/download/<path:filename>')
def download_file(filename):
    """Serve files for download"""
    return send_file(filename, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)