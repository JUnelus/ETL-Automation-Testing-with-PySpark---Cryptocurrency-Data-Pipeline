import logging
import tempfile
from pathlib import Path

import pandas as pd

from src.etl_pipeline.integrated_pipeline import QualityEnhancedETLPipeline


def _create_test_market_data_csv(temp_dir: str) -> str:
    """Create a deterministic raw market data file for integration testing."""
    df = pd.DataFrame([
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 65000.0,
            "market_cap": 1_250_000_000_000,
            "market_cap_rank": 1,
            "total_volume": 42_000_000_000,
            "price_change_percentage_24h": 2.1,
            "price_change_percentage_7d": 4.3,
            "price_change_percentage_30d": 7.5,
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "current_price": 3200.0,
            "market_cap": 385_000_000_000,
            "market_cap_rank": 2,
            "total_volume": 20_500_000_000,
            "price_change_percentage_24h": 1.4,
            "price_change_percentage_7d": 3.2,
            "price_change_percentage_30d": 6.1,
        },
        {
            "id": "solana",
            "symbol": "sol",
            "name": "Solana",
            "current_price": 155.0,
            "market_cap": 72_000_000_000,
            "market_cap_rank": 3,
            "total_volume": 3_900_000_000,
            "price_change_percentage_24h": 3.8,
            "price_change_percentage_7d": 5.1,
            "price_change_percentage_30d": 11.2,
        },
    ])

    output_path = Path(temp_dir) / "market_data_test.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


def test_integrated_etl_pipeline():
    """
    Test the complete Quality-Enhanced ETL Pipeline
    This demonstrates the full ETL + Quality validation workflow
    """
    print("🚀 Testing Quality-Enhanced ETL Pipeline...")
    print("This combines ETL processing with comprehensive data quality validation!")

    temp_data_dir = tempfile.mkdtemp(prefix="integration_raw_")
    try:
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Initialize the quality-enhanced pipeline
        pipeline = QualityEnhancedETLPipeline("Integration-Test")

        input_file = _create_test_market_data_csv(temp_data_dir)

        # Run the complete pipeline with quality gates
        results = pipeline.run_etl_with_quality_gates(input_file=input_file, quality_threshold=80.0)

        if results['status'] == 'SUCCESS':
            print(f"\n🎉 INTEGRATION TEST SUCCESSFUL!")
            print(f"✅ All quality gates passed")
            print(f"✅ Data processing completed")
            print(f"✅ Quality reports generated")
            print(f"✅ Production-ready outputs created")
            qa = results.get('quality_assessment', {})
            assert qa.get('raw_data_quality_score', 0) >= 80.0
            assert qa.get('processed_data_quality_score', 0) >= 80.0
        else:
            print(f"\n❌ INTEGRATION TEST FAILED")
            print(f"Failed at stage: {results.get('stage', 'Unknown')}")
            print(f"Error: {results.get('error', 'Unknown error')}")
            assert False, f"Integration pipeline failed: {results.get('error', 'Unknown error')}"

    except Exception as e:
        print(f"❌ Error during integration testing: {e}")
        import traceback
        traceback.print_exc()
        raise AssertionError(f"Error during integration testing: {e}") from e
    finally:
        import shutil
        shutil.rmtree(temp_data_dir, ignore_errors=True)


if __name__ == "__main__":
    test_integrated_etl_pipeline()
