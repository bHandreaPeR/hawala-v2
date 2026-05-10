#!/usr/bin/env python3
"""SENSEX-only expiry spread sweep — quick runner."""
import sys
sys.path.insert(0, '.')

# Monkey-patch INSTRUMENTS_TO_RUN before importing main module
import run_expiry_spread_backtest as _m
_m.INSTRUMENTS_TO_RUN = ['SENSEX']

from run_expiry_spread_backtest import main
main()
