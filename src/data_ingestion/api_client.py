import requests
import pandas as pd
import json
import os
from typing import Dict, List, Optional
import logging
from datetime import datetime
import time
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class CryptoAPIClient:
    """
    Professional-grade API client for cryptocurrency data ingestion
    Demonstrates API automation skills required for ETL QA roles
    """

    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self, rate_limit_delay: float = 1.0):
        """
        Initialize API client with professional configurations

        Args:
            rate_limit_delay: Delay between API calls to respect rate limits
        """
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Automation-Demo/1.0',
            'Accept': 'application/json'
        })
        self.rate_limit_delay = rate_limit_delay
        self.logger = logging.getLogger(__name__)

        # Create data directories
        self.data_dir = Path("data")
        self.raw_dir = self.data_dir / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """
        Make HTTP request with error handling and retry logic
        Essential for production ETL systems
        """
        url = f"{self.BASE_URL}/{endpoint}"
        max_retries = 3

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Making API request to: {endpoint}")
                response = self.session.get(url, params=params, timeout=30)

                # Handle rate limiting
                if response.status_code == 429:
                    wait_time = 60  # Wait 1 minute for rate limit reset
                    self.logger.warning(f"Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()

                # Rate limiting - be respectful to the API
                time.sleep(self.rate_limit_delay)

                return response.json()

            except requests.exceptions.RequestException as e:
                self.logger.error(f"API request failed (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

    def fetch_market_data(self, limit: int = 100) -> List[Dict]:
        """
        Fetch current market data for top cryptocurrencies

        Args:
            limit: Number of coins to fetch (max 250)

        Returns:
            List of cryptocurrency market data dictionaries
        """
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': min(limit, 250),  # API limit
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '1h,24h,7d,30d'
        }

        data = self._make_request('coins/markets', params)
        self.logger.info(f"Successfully fetched {len(data)} market records")
        return data

    def fetch_coin_details(self, coin_id: str) -> Dict:
        """
        Fetch detailed information for a specific coin

        Args:
            coin_id: CoinGecko coin identifier

        Returns:
            Detailed coin information dictionary
        """
        endpoint = f'coins/{coin_id}'
        params = {
            'localization': False,
            'tickers': False,
            'market_data': True,
            'community_data': True,
            'developer_data': True,
            'sparkline': False
        }

        data = self._make_request(endpoint, params)
        self.logger.info(f"Successfully fetched details for {coin_id}")
        return data

    def fetch_historical_data(self, coin_id: str, days: int = 30) -> Dict:
        """
        Fetch historical price data for a coin

        Args:
            coin_id: CoinGecko coin identifier
            days: Number of days of historical data

        Returns:
            Historical price data dictionary
        """
        endpoint = f'coins/{coin_id}/market_chart'
        params = {
            'vs_currency': 'usd',
            'days': days,
            'interval': 'daily' if days > 1 else 'hourly'
        }

        data = self._make_request(endpoint, params)
        self.logger.info(f"Successfully fetched {days} days of historical data for {coin_id}")
        return data

    def save_raw_data(self, data: List[Dict], filename: str, format_type: str = 'json') -> str:
        """
        Save raw data to local storage in specified format
        Demonstrates handling of multiple data formats (CSV, JSON, Parquet)

        Args:
            data: Data to save
            filename: Base filename (without extension)
            format_type: Format to save ('json', 'csv', 'parquet')

        Returns:
            Full path to saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if format_type == 'json':
            filepath = self.raw_dir / f"{filename}_{timestamp}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)

        elif format_type == 'csv':
            filepath = self.raw_dir / f"{filename}_{timestamp}.csv"
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False, encoding='utf-8')

        elif format_type == 'parquet':
            filepath = self.raw_dir / f"{filename}_{timestamp}.parquet"
            df = pd.DataFrame(data)
            df.to_parquet(filepath, index=False, engine='pyarrow')

        else:
            raise ValueError(f"Unsupported format: {format_type}")

        self.logger.info(f"Saved {len(data)} records to {filepath}")
        return str(filepath)

    def validate_api_response(self, data: List[Dict]) -> Dict:
        """
        Validate API response data quality
        Essential for ETL Quality Engineering

        Returns:
            Validation results dictionary
        """
        if not data:
            return {'status': 'FAIL', 'error': 'Empty response'}

        validation_results = {
            'status': 'PASS',
            'total_records': len(data),
            'validation_timestamp': datetime.now().isoformat(),
            'checks': {}
        }

        # Check required fields
        required_fields = ['id', 'symbol', 'name', 'current_price', 'market_cap']
        sample_record = data[0]

        for field in required_fields:
            field_present = field in sample_record
            validation_results['checks'][f'{field}_present'] = field_present
            if not field_present:
                validation_results['status'] = 'FAIL'

        # Check data types and ranges
        validation_results['checks']['price_positive'] = sample_record.get('current_price', 0) > 0
        validation_results['checks']['market_cap_positive'] = sample_record.get('market_cap', 0) > 0

        # Check for null values in critical fields
        null_prices = sum(1 for record in data if record.get('current_price') is None)
        validation_results['checks']['null_price_count'] = null_prices
        validation_results['checks']['price_completeness'] = (len(data) - null_prices) / len(data) * 100

        self.logger.info(f"API validation completed: {validation_results['status']}")
        return validation_results