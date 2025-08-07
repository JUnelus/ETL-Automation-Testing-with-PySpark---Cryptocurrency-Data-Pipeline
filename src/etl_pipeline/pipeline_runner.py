import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional  # Added missing imports
import glob

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.spark_session import create_spark_session
from src.etl_pipeline.transformations import CryptoDataTransformations
from pyspark.sql.functions import col  # Added missing import


class CryptoETLPipeline:
    """
    Main ETL pipeline orchestrator
    Demonstrates end-to-end ETL pipeline management
    """

    def __init__(self, app_name: str = "CryptoETL"):
        self.spark = create_spark_session(app_name)
        self.transformer = CryptoDataTransformations(self.spark)
        self.logger = logging.getLogger(__name__)

        # Setup directories
        self.base_dir = Path(__file__).parent.parent.parent
        self.data_dir = self.base_dir / "data"
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"

        # Create directories if they don't exist
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def get_latest_raw_file(self, pattern: str = "*market_data*.csv") -> str:
        """
        Get the most recent raw data file
        """
        files = glob.glob(str(self.raw_dir / pattern))
        if not files:
            raise FileNotFoundError(f"No files found matching pattern: {pattern}")

        # Return the most recent file
        latest_file = max(files, key=os.path.getctime)
        self.logger.info(f"Using latest raw file: {latest_file}")
        return latest_file

    def run_full_pipeline(self, input_file: str = None) -> Dict:
        """
        Execute the complete ETL pipeline

        Args:
            input_file: Optional specific input file path

        Returns:
            Dictionary with pipeline execution results
        """
        pipeline_start = datetime.now()
        self.logger.info("🚀 Starting ETL Pipeline Execution")

        try:
            # Step 1: Load raw data
            if input_file is None:
                input_file = self.get_latest_raw_file()

            self.logger.info(f"Loading data from: {input_file}")
            raw_df = self.transformer.load_raw_data(input_file, "csv")

            # Step 2: Clean and standardize data
            self.logger.info("Cleaning and standardizing data")
            cleaned_df = self.transformer.clean_market_data(raw_df)

            # Cache cleaned data for multiple operations
            cleaned_df.cache()
            cleaned_count = cleaned_df.count()

            # Step 3: Calculate advanced metrics
            self.logger.info("Calculating advanced metrics")
            metrics_df = self.transformer.calculate_advanced_metrics(cleaned_df)

            # Step 4: Create market summary
            self.logger.info("Creating market summary")
            summary_df = self.transformer.create_market_summary(metrics_df)

            # Step 5: Create performance rankings
            self.logger.info("Creating performance rankings")
            rankings_df = self.transformer.create_performance_rankings(metrics_df)

            # Step 6: Save processed data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Save main processed dataset
            main_output = self.processed_dir / f"crypto_processed_{timestamp}.parquet"
            self.transformer.save_processed_data(
                metrics_df,
                str(main_output),
                "parquet",
                ["market_cap_category"]
            )

            # Save summary data
            summary_output = self.processed_dir / f"market_summary_{timestamp}.parquet"
            self.transformer.save_processed_data(summary_df, str(summary_output), "parquet")

            # Save rankings
            rankings_output = self.processed_dir / f"performance_rankings_{timestamp}.parquet"
            self.transformer.save_processed_data(rankings_df, str(rankings_output), "parquet")

            # Pipeline completion
            pipeline_end = datetime.now()
            execution_time = (pipeline_end - pipeline_start).total_seconds()

            results = {
                'status': 'SUCCESS',
                'execution_time_seconds': execution_time,
                'input_file': input_file,
                'records_processed': cleaned_count,
                'output_files': {
                    'main_dataset': str(main_output),
                    'market_summary': str(summary_output),
                    'performance_rankings': str(rankings_output)
                },
                'pipeline_timestamp': pipeline_end.isoformat()
            }

            self.logger.info(f"✅ ETL Pipeline completed successfully in {execution_time:.2f} seconds")
            self.logger.info(f"📊 Processed {cleaned_count} records")

            # Display sample results
            self._display_sample_results(summary_df, rankings_df)

            return results

        except Exception as e:
            self.logger.error(f"❌ Pipeline execution failed: {e}")
            return {
                'status': 'FAILED',
                'error': str(e),
                'execution_time_seconds': (datetime.now() - pipeline_start).total_seconds()
            }

        finally:
            # Clean up Spark session
            self.spark.stop()

    def _display_sample_results(self, summary_df, rankings_df):
        """Display sample results for verification"""
        print("\n" + "=" * 60)
        print("📊 ETL PIPELINE RESULTS PREVIEW")
        print("=" * 60)

        print("\n🏆 TOP 5 PERFORMERS (24H):")
        rankings_df.select(
            "coin_name", "coin_symbol", "price_usd",
            "price_change_24h_pct", "rank_24h"
        ).filter(
            col("rank_24h") <= 5
        ).orderBy("rank_24h").show(5, truncate=False)

        print("\n📈 MARKET SUMMARY BY CATEGORY:")
        summary_df.show(10, truncate=False)