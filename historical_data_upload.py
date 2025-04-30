#!/usr/bin/env python3
# historical_data_upload.py
# -----------------------------------------------------------
# Simple web interface for uploading historical indicator data
# and generating authentic historical sector sentiment scores

import dash
from dash import dcc, html, callback, Output, Input, State
import dash_bootstrap_components as dbc
import pandas as pd
import base64
import io
import os
from datetime import datetime
import process_historical_indicators

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# App layout
app.layout = dbc.Container([
    html.H1("T2D Pulse Historical Data Generator", className="my-4"),
    
    dbc.Card([
        dbc.CardHeader("Upload Historical Indicator Data"),
        dbc.CardBody([
            html.P("Upload your historical indicator data Excel file with the following sheets:"),
            html.Ul([
                html.Li("NASDAQ (date, value)"),
                html.Li("VIX (date, value)"),
                html.Li("Treasury_Yield (date, value)"),
                html.Li("Fed_Funds_Rate (date, value)"),
                html.Li("CPI (date, value)"),
                html.Li("PCEPI (date, value)"),
                html.Li("GDP (date, value)"),
                html.Li("PCE (date, value)"),
                html.Li("Unemployment (date, value)"),
                html.Li("Job_Postings (date, value)"),
                html.Li("Data_PPI (date, value)"),
                html.Li("Software_PPI (date, value)"),
                html.Li("Consumer_Sentiment (date, value)")
            ]),
            dcc.Upload(
                id='upload-historical-data',
                children=html.Div([
                    'Drag and Drop or ',
                    html.A('Select Excel File')
                ]),
                style={
                    'width': '100%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px'
                },
                multiple=False
            ),
            html.Div(id='upload-output')
        ])
    ], className="mb-4"),
    
    dbc.Card([
        dbc.CardHeader("Generate Historical Sector Scores"),
        dbc.CardBody([
            html.P("Specify the date range for generating historical sector scores:"),
            dbc.Row([
                dbc.Col([
                    html.Label("Start Date:"),
                    dcc.DatePickerSingle(
                        id='start-date',
                        date=datetime.now().replace(day=1).date(),
                        display_format='YYYY-MM-DD'
                    )
                ]),
                dbc.Col([
                    html.Label("End Date:"),
                    dcc.DatePickerSingle(
                        id='end-date',
                        date=datetime.now().date(),
                        display_format='YYYY-MM-DD'
                    )
                ])
            ], className="mb-3"),
            dbc.Button("Generate Historical Scores", id="generate-scores-button", color="primary"),
            html.Div(id='generate-output', className="mt-3")
        ])
    ], className="mb-4"),
    
    dbc.Card([
        dbc.CardHeader("Download Results"),
        dbc.CardBody([
            html.P("Download the generated historical sector scores:"),
            html.Div(id='download-links', style={"display": "none"})
        ])
    ])
], fluid=True)

# Callback for uploading historical data
@app.callback(
    Output('upload-output', 'children'),
    Input('upload-historical-data', 'contents'),
    State('upload-historical-data', 'filename')
)
def upload_historical_data(contents, filename):
    if contents is None:
        return html.Div()
    
    try:
        # Decode the uploaded file
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        
        # Save to file
        os.makedirs('data', exist_ok=True)
        output_path = os.path.join('data', 'Historical_Indicator_Data.xlsx')
        
        with open(output_path, 'wb') as f:
            f.write(decoded)
        
        return html.Div([
            html.H5("Upload Successful!"),
            html.P(f"File '{filename}' has been saved as 'Historical_Indicator_Data.xlsx'.")
        ])
    
    except Exception as e:
        return html.Div([
            html.H5("Upload Failed!"),
            html.P(f"Error: {str(e)}")
        ])

# Callback for generating historical scores
@app.callback(
    [Output('generate-output', 'children'),
     Output('download-links', 'children'),
     Output('download-links', 'style')],
    Input('generate-scores-button', 'n_clicks'),
    State('start-date', 'date'),
    State('end-date', 'date')
)
def generate_historical_scores(n_clicks, start_date, end_date):
    if n_clicks is None:
        return html.Div(), html.Div(), {"display": "none"}
    
    try:
        # Check if the uploaded file exists
        data_file = os.path.join('data', 'Historical_Indicator_Data.xlsx')
        if not os.path.exists(data_file):
            return html.Div([
                html.H5("Error!"),
                html.P("Please upload the historical indicator data file first.")
            ]), html.Div(), {"display": "none"}
        
        # Convert date strings to datetime objects
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # Calculate historical sector scores
        historical_scores = process_historical_indicators.calculate_historical_sector_scores(start_date, end_date)
        
        if historical_scores is None or historical_scores.empty:
            return html.Div([
                html.H5("Error!"),
                html.P("Failed to generate historical sector scores. Check the console for details.")
            ]), html.Div(), {"display": "none"}
        
        # Export to CSV and Excel
        csv_path = "data/authentic_historical_scores.csv"
        excel_path = "data/authentic_historical_scores.xlsx"
        
        process_historical_indicators.export_historical_scores_to_csv(historical_scores, csv_path)
        process_historical_indicators.export_historical_scores_to_excel(historical_scores, excel_path)
        
        # Create download links
        download_links = html.Div([
            dbc.Button("Download CSV", id="download-csv", color="success", className="me-2"),
            dbc.Button("Download Excel", id="download-excel", color="success"),
            html.Div(id="download-notification", className="mt-2")
        ])
        
        return html.Div([
            html.H5("Success!"),
            html.P(f"Historical sector scores have been generated for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}."),
            html.P(f"Processed {len(historical_scores)} dates with scores for {len(historical_scores.columns) - 1} sectors.")
        ]), download_links, {"display": "block"}
    
    except Exception as e:
        return html.Div([
            html.H5("Error!"),
            html.P(f"Failed to generate historical sector scores: {str(e)}")
        ]), html.Div(), {"display": "none"}

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=5001, debug=True)