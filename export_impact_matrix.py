import pandas as pd

# Define the sectors
sectors = [
    "SMB SaaS", "Enterprise SaaS", "Cloud Infrastructure", "AdTech", 
    "Fintech", "Consumer Internet", "eCommerce", "Cybersecurity", 
    "Dev Tools / Analytics", "Semiconductors", "AI Infrastructure", 
    "Vertical SaaS", "IT Services / Legacy Tech", "Hardware / Devices"
]

# Define the indicators
indicators = [
    "10Y_Treasury_Yield_%", "VIX", "NASDAQ_20d_gap_%", "Fed_Funds_Rate_%",
    "CPI_YoY_%", "PCEPI_YoY_%", "Real_GDP_Growth_%_SAAR", "Real_PCE_YoY_%",
    "Unemployment_%", "Software_Dev_Job_Postings_YoY_%", "PPI_Data_Processing_YoY_%",
    "PPI_Software_Publishers_YoY_%", "Consumer_Sentiment"
]

# Create the impact matrix
impact_matrix = {
    "10Y_Treasury_Yield_%": [3, 3, 3, 2, 2, 2, 2, 2, 3, 2, 3, 3, 1, 2],
    "VIX": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
    "NASDAQ_20d_gap_%": [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2],
    "Fed_Funds_Rate_%": [3, 3, 3, 2, 3, 2, 2, 2, 3, 2, 3, 3, 1, 2],
    "CPI_YoY_%": [2, 2, 2, 3, 2, 3, 3, 2, 2, 3, 2, 2, 2, 3],
    "PCEPI_YoY_%": [2, 2, 2, 3, 2, 3, 3, 2, 2, 3, 2, 2, 2, 3],
    "Real_GDP_Growth_%_SAAR": [2, 2, 2, 3, 2, 3, 3, 2, 2, 3, 2, 2, 2, 3],
    "Real_PCE_YoY_%": [2, 2, 2, 3, 2, 3, 3, 2, 2, 3, 2, 2, 2, 3],
    "Unemployment_%": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
    "Software_Dev_Job_Postings_YoY_%": [3, 2, 3, 1, 1, 1, 1, 3, 3, 1, 3, 3, 1, 1],
    "PPI_Data_Processing_YoY_%": [1, 1, 3, 1, 1, 1, 1, 1, 1, 1, 3, 1, 1, 1],
    "PPI_Software_Publishers_YoY_%": [1, 1, 3, 1, 1, 1, 1, 1, 1, 1, 3, 1, 1, 1],
    "Consumer_Sentiment": [2, 1, 1, 3, 2, 3, 3, 1, 1, 2, 1, 1, 1, 2]
}

# Create a dictionary to hold importance values
importance = {
    "NASDAQ_20d_gap_%": 3,
    "10Y_Treasury_Yield_%": 3,
    "VIX": 3,
    "Consumer_Sentiment": 3
}

# Default importance is 1
default_importance = 1

# Calculate the impact x importance matrix
weight_matrix = {}
for indicator in indicators:
    # Get the importance (default is 1 if not specified)
    imp = importance.get(indicator, default_importance)
    # Calculate weights
    weight_matrix[indicator] = [impact * imp for impact in impact_matrix[indicator]]

# Create DataFrames
df_impact = pd.DataFrame(impact_matrix, index=sectors)
df_weight = pd.DataFrame(weight_matrix, index=sectors)

# Create a dictionary to hold the importance values for the spreadsheet
importance_row = [importance.get(indicator, default_importance) for indicator in indicators]
df_importance = pd.DataFrame([importance_row], columns=indicators, index=["Importance"])

# Create Excel writer
with pd.ExcelWriter('sector_impact_matrix.xlsx') as writer:
    # Write the impact values with a descriptive sheet name
    df_impact.T.to_excel(writer, sheet_name='Impact Values (1-3)')
    
    # Write the importance values
    df_importance.to_excel(writer, sheet_name='Importance Values')
    
    # Write the weight values (impact x importance)
    df_weight.T.to_excel(writer, sheet_name='Weights (Impact x Importance)')

print("Excel file 'sector_impact_matrix.xlsx' has been created successfully.")