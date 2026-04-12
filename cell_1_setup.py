# ============================================================
# CELL 1 — Installs, Imports, API Connection
# ============================================================
# BankNifty Gap Fill + Trailing Stop Strategy
# Broker: Groww (free plan)  |  Exchange: NSE  |  Segment: CASH
# ============================================================

# !pip install growwapi scikit-learn yfinance nselib xlrd -q

from growwapi import GrowwAPI
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from datetime import time as dtime
import time
import warnings
import matplotlib.pyplot as plt
from scipy.stats import norm
from math import log, sqrt, exp

warnings.filterwarnings('ignore')

# ── API Auth ──────────────────────────────────────────────────────────────
# Groww access token resets daily at 6 AM — regenerate each morning
API_AUTH_TOKEN = "PASTE_YOUR_TOKEN_HERE"
groww = GrowwAPI(API_AUTH_TOKEN)

print("✅ Setup complete")
