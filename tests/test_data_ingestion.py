from src.data_ingestion.api_client import CryptoAPIClient
from src.data_ingestion.data_fetcher import CryptoDataFetcher


def test_data_ingestion():
    """
    Quick test to verify data ingestion is working
    Run this to validate your setup
    """
    print("🚀 Testing Data Ingestion Layer...")

    try:
        # Test API client
        print("\n1. Testing API Client...")
        client = CryptoAPIClient()

        # Fetch a small sample
        market_data = client.fetch_market_data(limit=5)
        print(f"✅ Fetched {len(market_data)} market records")

        # Validate response
        validation = client.validate_api_response(market_data)
        print(f"✅ API validation: {validation['status']}")

        # Test data fetcher
        print("\n2. Testing Data Fetcher...")
        fetcher = CryptoDataFetcher()

        # Fetch and save data
        results = fetcher.fetch_daily_market_data(
            top_n=10,
            save_formats=['json', 'csv']
        )

        print(f"✅ Saved files: {list(results['saved_files'].keys())}")
        print(f"✅ Records processed: {results['records_fetched']}")

        # Show preview
        print("\n3. Data Preview:")
        for i, record in enumerate(results['data_preview'], 1):
            print(f"   {i}. {record['name']} ({record['symbol'].upper()}): ${record['current_price']:,.2f}")

        print("\n🎉 Data Ingestion Layer Setup Complete!")
        print(f"📁 Check the 'data/raw/' directory for saved files")

        return True

    except Exception as e:
        print(f"❌ Error during testing: {e}")
        return False


if __name__ == "__main__":
    # Run the test
    test_data_ingestion()