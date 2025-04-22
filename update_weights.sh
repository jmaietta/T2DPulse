#!/bin/bash

# Update CPI weight
sed -i 's/id="cpi-weight",\n                            min=0,\n                            max=30,\n                            step=0.1,\n                            value=6.0,/id="cpi-weight",\n                            min=0,\n                            max=30,\n                            step=0.01,\n                            value=6.36,/g' app.py

# Update PCEPI weight
sed -i 's/id="pcepi-weight",\n                            min=0,\n                            max=30,\n                            step=0.1,\n                            value=7.0,/id="pcepi-weight",\n                            min=0,\n                            max=30,\n                            step=0.01,\n                            value=6.36,/g' app.py

# Update NASDAQ weight
sed -i 's/id="nasdaq-weight",\n                            min=0,\n                            max=30,\n                            step=0.1,\n                            value=16.0,/id="nasdaq-weight",\n                            min=0,\n                            max=30,\n                            step=0.01,\n                            value=15.45,/g' app.py

# Update Data PPI weight
sed -i 's/id="data-ppi-weight",\n                            min=0,\n                            max=30,\n                            step=0.1,\n                            value=6.0,/id="data-ppi-weight",\n                            min=0,\n                            max=30,\n                            step=0.01,\n                            value=6.36,/g' app.py

# Update Software PPI weight
sed -i 's/id="software-ppi-weight",\n                            min=0,\n                            max=30,\n                            step=0.1,\n                            value=6.0,/id="software-ppi-weight",\n                            min=0,\n                            max=30,\n                            step=0.01,\n                            value=6.36,/g' app.py

# Update Interest Rate weight
sed -i 's/id="interest-rate-weight",\n                            min=0,\n                            max=30,\n                            step=0.1,\n                            value=6.0,/id="interest-rate-weight",\n                            min=0,\n                            max=30,\n                            step=0.01,\n                            value=6.36,/g' app.py

# Update Treasury Yield weight
sed -i 's/id="treasury-yield-weight",\n                            min=0,\n                            max=30,\n                            step=0.1,\n                            value=9.0,/id="treasury-yield-weight",\n                            min=0,\n                            max=30,\n                            step=0.01,\n                            value=9.09,/g' app.py

# Update VIX weight
sed -i 's/id="vix-weight",\n                            min=0,\n                            max=30,\n                            step=0.1,\n                            value=9.0,/id="vix-weight",\n                            min=0,\n                            max=30,\n                            step=0.01,\n                            value=9.09,/g' app.py

# Update Consumer Sentiment weight
sed -i 's/id="consumer-sentiment-weight",\n                            min=0,\n                            max=30,\n                            step=0.1,\n                            value=9.0,/id="consumer-sentiment-weight",\n                            min=0,\n                            max=30,\n                            step=0.01,\n                            value=9.09,/g' app.py
