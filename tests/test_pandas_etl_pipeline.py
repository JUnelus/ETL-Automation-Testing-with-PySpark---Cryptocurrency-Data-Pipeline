import logging

from src.etl_pipeline.pandas_pipeline import PandasETLPipeline


# Test script for Pandas ETL
def test_pandas_etl_pipeline():
    """
    Test the Pandas ETL pipeline (no Java required!)
    """
    print("🚀 Testing Pandas ETL Pipeline (No Java Required)...")

    try:
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Initialize pipeline
        pipeline = PandasETLPipeline("Pandas-ETL-Test")

        # Run the pipeline
        results = pipeline.run_full_pipeline()

        if results['status'] == 'SUCCESS':
            print(f"\n✅ Pipeline executed successfully!")
            print(f"⏱️  Execution time: {results['execution_time_seconds']:.2f} seconds")
            print(f"📊 Records processed: {results['records_processed']}")
            print(f"📁 Output files created:")
            for file_type, path in results['output_files'].items():
                print(f"   - {file_type}: {path}")

            print(f"\n🎉 Pandas ETL Pipeline Complete!")
            print(f"📁 Check the 'data/processed/' directory for output files")
            assert results['records_processed'] > 0
            assert len(results['output_files']) == 3
        else:
            print(f"❌ Pipeline failed: {results['error']}")
            assert False, f"Pandas pipeline failed: {results['error']}"

    except Exception as e:
        print(f"❌ Error during pipeline testing: {e}")
        import traceback
        traceback.print_exc()
        raise AssertionError(f"Error during pandas pipeline testing: {e}") from e


if __name__ == "__main__":
    test_pandas_etl_pipeline()