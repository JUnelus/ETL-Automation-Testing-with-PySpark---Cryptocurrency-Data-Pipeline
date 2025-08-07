import logging
from datetime import datetime
from typing import Dict, List
from src.data_ingestion.api_client import CryptoAPIClient


class CryptoDataFetcher:
    """
    High-level data fetching orchestrator
    Demonstrates ETL pipeline organization skills
    """

    def __init__(self):
        self.api_client = CryptoAPIClient()
        self.logger = logging.getLogger(__name__)

    def fetch_daily_market_data(self, top_n: int = 100, save_formats: List[str] = None) -> Dict:
        """
        Fetch daily market data with multiple format support

        Args:
            top_n: Number of top coins to fetch
            save_formats: List of formats to save ['json', 'csv', 'parquet']

        Returns:
            Dictionary with file paths and validation results
        """
        if save_formats is None:
            save_formats = ['json', 'csv']

        self.logger.info(f"Starting daily market data fetch for top {top_n} coins")

        # Fetch market data
        market_data = self.api_client.fetch_market_data(limit=top_n)

        # Validate data quality
        validation_results = self.api_client.validate_api_response(market_data)

        # Save in multiple formats
        saved_files = {}
        for format_type in save_formats:
            try:
                filepath = self.api_client.save_raw_data(
                    market_data,
                    'market_data',
                    format_type
                )
                saved_files[format_type] = filepath
            except Exception as e:
                self.logger.error(f"Failed to save in {format_type} format: {e}")

        results = {
            'fetch_timestamp': datetime.now().isoformat(),
            'records_fetched': len(market_data),
            'validation_results': validation_results,
            'saved_files': saved_files,
            'data_preview': market_data[:3] if market_data else []  # First 3 records for preview
        }

        self.logger.info(f"Daily fetch completed: {len(market_data)} records")
        return results

    def fetch_historical_batch(self, coin_list: List[str], days: int = 30) -> Dict:
        """
        Fetch historical data for multiple coins
        Demonstrates batch processing capabilities

        Args:
            coin_list: List of coin IDs to fetch
            days: Days of historical data

        Returns:
            Dictionary with results for each coin
        """
        results = {}
        failed_fetches = []

        self.logger.info(f"Starting historical batch fetch for {len(coin_list)} coins")

        for coin_id in coin_list:
            try:
                historical_data = self.api_client.fetch_historical_data(coin_id, days)

                # Save historical data
                filepath = self.api_client.save_raw_data(
                    [historical_data],
                    f'historical_{coin_id}',
                    'json'
                )

                results[coin_id] = {
                    'status': 'SUCCESS',
                    'data_points': len(historical_data.get('prices', [])),
                    'file_path': filepath
                }

            except Exception as e:
                self.logger.error(f"Failed to fetch historical data for {coin_id}: {e}")
                failed_fetches.append(coin_id)
                results[coin_id] = {
                    'status': 'FAILED',
                    'error': str(e)
                }

        summary = {
            'batch_timestamp': datetime.now().isoformat(),
            'total_coins': len(coin_list),
            'successful_fetches': len(coin_list) - len(failed_fetches),
            'failed_fetches': len(failed_fetches),
            'failed_coins': failed_fetches,
            'results': results
        }

        self.logger.info(
            f"Historical batch completed: {summary['successful_fetches']}/{summary['total_coins']} successful")
        return summary