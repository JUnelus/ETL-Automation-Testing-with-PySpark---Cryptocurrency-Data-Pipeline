import sys
import logging

from pathlib import Path

# Add src to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

# Configure logging before imports
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from src.etl_pipeline.pipeline_runner import CryptoETLPipeline


def test_etl_pipeline():
    """
    Test the complete ETL pipeline
    This function can be run directly or via pytest
    """
    print("🚀 Testing PySpark ETL Pipeline...")

    try:
        # Initialize pipeline
        pipeline = CryptoETLPipeline("ETL-Test")

        # Run the pipeline
        results = pipeline.run_full_pipeline()

        if results['status'] == 'SUCCESS':
            print(f"\n✅ Pipeline executed successfully!")
            print(f"⏱️  Execution time: {results['execution_time_seconds']:.2f} seconds")
            print(f"📊 Records processed: {results['records_processed']}")
            print(f"📁 Output files created:")
            for file_type, path in results['output_files'].items():
                print(f"   - {file_type}: {path}")

            # For pytest compatibility
            assert results['status'] == 'SUCCESS'
            assert results['records_processed'] > 0
            assert len(results['output_files']) == 3

            return True
        else:
            print(f"❌ Pipeline failed: {results['error']}")
            # For pytest compatibility
            assert False, f"Pipeline failed: {results['error']}"

    except Exception as e:
        print(f"❌ Error during pipeline testing: {e}")
        import traceback
        traceback.print_exc()
        # For pytest compatibility
        assert False, f"Test failed with exception: {e}"


if __name__ == "__main__":
    # Run directly (not through pytest)
    test_etl_pipeline()