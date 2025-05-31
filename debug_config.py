#!/usr/bin/env python3

import sys
import os
from pathlib import Path

# Add src to path
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Load .env.test like the tests do
from dotenv import load_dotenv
env_path = Path(__file__).parent / '.env.test'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
    print(f"Loaded test environment from: {env_path}")
else:
    print(f"Warning: .env.test file not found at {env_path}")

# Now load settings
from config import get_settings

settings = get_settings()
print(f"Settings: {settings}")
print(f"CCXT Config: {settings.ccxt}")
print(f"Default Exchange: {settings.ccxt.default_exchange}")
print(f"Exchange ID: {settings.ccxt.exchange_id}")

# Also check environment variables directly
print("\nEnvironment variables:")
for key, value in os.environ.items():
    if 'CCXT' in key.upper():
        print(f"{key} = {value}")
