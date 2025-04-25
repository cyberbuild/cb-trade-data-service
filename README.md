# cb-trade-data-service

Microservice for collecting and storing cryptocurrency order book data.

## Setup

1. Create and activate the Conda environment:
   ```powershell
   conda env create -f environment.yml
   conda activate cb-trade
   ```
2. Install any additional dependencies:
   ```powershell
   pip install -r requirements.txt  # (if you add one later)
   ```
3. Run tests:
   ```powershell
   pytest
   ```

## Project Structure

- `environment.yml` — Conda environment definition
- `.gitignore`       — Excludes build artifacts and env files
- `README.md`        — This document
- `src/`             — Python package for the data service
- `tests/`           — Unit and integration tests
