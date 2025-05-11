"""
Create a comparison between our calculated market caps and the user's provided values
"""

import pandas as pd
import matplotlib.pyplot as plt

def main():
    # Load our corrected data
    our_data = pd.read_csv("corrected_sector_market_caps.csv")
    
    # Load user's data
    user_data = pd.read_csv("user_provided_sector_market_caps.csv")
    
    # Merge the datasets
    comparison = pd.merge(our_data, user_data, on="Sector", suffixes=("_Calculated", "_User"))
    
    # Calculate the differences
    comparison["Difference"] = comparison["Market Cap (Billions USD)_User"] - comparison["Market Cap (Billions USD)_Calculated"]
    comparison["Percentage_Difference"] = (comparison["Difference"] / comparison["Market Cap (Billions USD)_User"]) * 100
    
    # Sort by user's market cap values (descending)
    comparison = comparison.sort_values("Market Cap (Billions USD)_User", ascending=False)
    
    # Save the comparison
    comparison.to_csv("market_cap_comparison.csv", index=False)
    
    print("Market Cap Comparison (Billions USD):")
    print("-" * 80)
    print(f"{'Sector':<25} {'Our Calc.':<15} {'User Value':<15} {'Diff.':<15} {'% Diff.':<15}")
    print("-" * 80)
    
    total_calculated = 0
    total_user = 0
    
    for _, row in comparison.iterrows():
        sector = row["Sector"]
        calc_value = row["Market Cap (Billions USD)_Calculated"]
        user_value = row["Market Cap (Billions USD)_User"]
        diff = row["Difference"]
        pct_diff = row["Percentage_Difference"]
        
        total_calculated += calc_value
        total_user += user_value
        
        print(f"{sector:<25} ${calc_value:<14.2f} ${user_value:<14.2f} ${diff:<14.2f} {pct_diff:<14.2f}%")
    
    total_diff = total_user - total_calculated
    total_pct_diff = (total_diff / total_user) * 100
    
    print("-" * 80)
    print(f"{'TOTAL':<25} ${total_calculated:<14.2f} ${total_user:<14.2f} ${total_diff:<14.2f} {total_pct_diff:<14.2f}%")
    
    # Create a text file with formatted comparison
    with open("market_cap_comparison.txt", "w") as f:
        f.write("Sector Market Capitalization Comparison (Billions USD)\n\n")
        f.write(f"{'Sector':<25} {'Our Calculation':<20} {'User Value':<15} {'Difference':<15} {'% Difference':<15}\n")
        f.write("-" * 85 + "\n")
        
        for _, row in comparison.iterrows():
            sector = row["Sector"]
            calc_value = row["Market Cap (Billions USD)_Calculated"]
            user_value = row["Market Cap (Billions USD)_User"]
            diff = row["Difference"]
            pct_diff = row["Percentage_Difference"]
            
            f.write(f"{sector:<25} ${calc_value:<19.2f} ${user_value:<14.2f} ${diff:<14.2f} {pct_diff:<14.2f}%\n")
        
        f.write("-" * 85 + "\n")
        f.write(f"{'TOTAL':<25} ${total_calculated:<19.2f} ${total_user:<14.2f} ${total_diff:<14.2f} {total_pct_diff:<14.2f}%\n")
    
    print(f"\nComparison saved to market_cap_comparison.csv and market_cap_comparison.txt")
    
    # Create the final user-approved version
    with open("final_sector_market_caps.csv", "w") as f:
        f.write("Sector,Market Cap (Billions USD)\n")
        for _, row in comparison.iterrows():
            sector = row["Sector"]
            value = row["Market Cap (Billions USD)_User"]
            f.write(f"{sector},{value}\n")
    
    print(f"Final user-approved market caps saved to final_sector_market_caps.csv")

if __name__ == "__main__":
    main()