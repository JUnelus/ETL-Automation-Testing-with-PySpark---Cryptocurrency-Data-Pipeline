import logging

from src.etl_pipeline.integrated_pipeline import QualityEnhancedETLPipeline


def test_integrated_etl_pipeline():
    """
    Test the complete Quality-Enhanced ETL Pipeline
    This demonstrates the full ETL + Quality validation workflow
    """
    print("🚀 Testing Quality-Enhanced ETL Pipeline...")
    print("This combines ETL processing with comprehensive data quality validation!")

    try:
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Initialize the quality-enhanced pipeline
        pipeline = QualityEnhancedETLPipeline("Integration-Test")

        # Run the complete pipeline with quality gates
        results = pipeline.run_etl_with_quality_gates(quality_threshold=80.0)

        if results['status'] == 'SUCCESS':
            print(f"\n🎉 INTEGRATION TEST SUCCESSFUL!")
            print(f"✅ All quality gates passed")
            print(f"✅ Data processing completed")
            print(f"✅ Quality reports generated")
            print(f"✅ Production-ready outputs created")

            return True
        else:
            print(f"\n❌ INTEGRATION TEST FAILED")
            print(f"Failed at stage: {results.get('stage', 'Unknown')}")
            print(f"Error: {results.get('error', 'Unknown error')}")

            return False

    except Exception as e:
        print(f"❌ Error during integration testing: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_integrated_etl_pipeline()
