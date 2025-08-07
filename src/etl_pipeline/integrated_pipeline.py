import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import glob
import json
import sys

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from src.etl_pipeline.pandas_pipeline import PandasETLPipeline
from src.data_quality.quality_validator import DataQualityValidator


class QualityEnhancedETLPipeline:
    """
    Production-Ready ETL Pipeline with Integrated Data Quality Validation
    This demonstrates the complete skillset for ETL Quality Engineering roles
    """

    def __init__(self, app_name: str = "QualityETL"):
        self.app_name = app_name
        self.logger = logging.getLogger(__name__)
        self.etl_pipeline = PandasETLPipeline(app_name)
        self.quality_validator = DataQualityValidator()

        # Setup directories
        self.base_dir = Path(__file__).parent.parent.parent
        self.data_dir = self.base_dir / "data"
        self.quality_dir = self.data_dir / "quality_reports"
        self.quality_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Initialized Quality-Enhanced ETL Pipeline: {app_name}")

    def run_etl_with_quality_gates(self, input_file: str = None,
                                   quality_threshold: float = 80.0) -> Dict:
        """
        Execute ETL pipeline with integrated quality gates
        This is the main production method that combines ETL + Quality validation

        Args:
            input_file: Optional input file path
            quality_threshold: Minimum quality score required to proceed (0-100)

        Returns:
            Complete pipeline results with quality assessment
        """
        pipeline_start = datetime.now()
        self.logger.info("🚀 Starting Quality-Enhanced ETL Pipeline")

        try:
            # ============= STAGE 1: DATA INGESTION & INITIAL QUALITY CHECK =============
            self.logger.info("STAGE 1: Data Ingestion & Initial Quality Assessment")

            if input_file is None:
                input_file = self.etl_pipeline.get_latest_raw_file()

            # Load raw data
            raw_df = self.etl_pipeline.load_raw_data(input_file)

            # Initial quality check on raw data
            raw_quality_config = {
                'critical_columns': ['id', 'symbol', 'name', 'current_price'],
                'completeness_threshold': 0.90,  # Lower threshold for raw data
                'unique_columns': ['id'],
                'generate_summary': True
            }

            raw_quality_report = self.quality_validator.generate_comprehensive_quality_report(
                raw_df, raw_quality_config
            )

            raw_quality_score = raw_quality_report['overall_quality_assessment']['quality_score_percentage']
            self.logger.info(f"Raw data quality score: {raw_quality_score}%")

            # Quality Gate 1: Raw data must meet minimum standards
            if raw_quality_score < 70:  # Lower threshold for raw data
                return {
                    'status': 'FAILED',
                    'stage': 'RAW_DATA_QUALITY_GATE',
                    'error': f'Raw data quality too low: {raw_quality_score}%',
                    'raw_quality_report': raw_quality_report,
                    'execution_time_seconds': (datetime.now() - pipeline_start).total_seconds()
                }

            # ============= STAGE 2: ETL PROCESSING =============
            self.logger.info("STAGE 2: ETL Processing & Transformations")

            # Clean and standardize data
            cleaned_df = self.etl_pipeline.clean_market_data(raw_df)

            # Calculate advanced metrics
            metrics_df = self.etl_pipeline.calculate_advanced_metrics(cleaned_df)

            # Create aggregations
            summary_df = self.etl_pipeline.create_market_summary(metrics_df)
            rankings_df = self.etl_pipeline.create_performance_rankings(metrics_df)

            # ============= STAGE 3: POST-PROCESSING QUALITY VALIDATION =============
            self.logger.info("STAGE 3: Post-Processing Quality Validation")

            # Comprehensive quality check on processed data
            processed_quality_config = {
                'critical_columns': ['coin_id', 'coin_symbol', 'coin_name', 'price_usd', 'market_cap_usd'],
                'completeness_threshold': 0.95,  # Higher threshold for processed data
                'unique_columns': ['coin_id'],
                'business_rules': {
                    'positive_prices': {
                        'condition': metrics_df['price_usd'] > 0,
                        'description': 'All prices must be positive'
                    },
                    'positive_market_caps': {
                        'condition': metrics_df['market_cap_usd'] > 0,
                        'description': 'All market caps must be positive'
                    },
                    'valid_volatility_scores': {
                        'condition': metrics_df.get('volatility_score', pd.Series([1])).between(0, 100),
                        'description': 'Volatility scores must be between 0 and 100'
                    },
                    'reasonable_volume_ratios': {
                        'condition': metrics_df.get('volume_to_market_cap_ratio', pd.Series([0.1])).between(0, 10),
                        'description': 'Volume to market cap ratios should be reasonable (0-10)'
                    }
                },
                'generate_summary': True
            }

            processed_quality_report = self.quality_validator.generate_comprehensive_quality_report(
                metrics_df, processed_quality_config
            )

            processed_quality_score = processed_quality_report['overall_quality_assessment']['quality_score_percentage']
            self.logger.info(f"Processed data quality score: {processed_quality_score}%")

            # Quality Gate 2: Processed data must meet production standards
            if processed_quality_score < quality_threshold:
                return {
                    'status': 'FAILED',
                    'stage': 'PROCESSED_DATA_QUALITY_GATE',
                    'error': f'Processed data quality below threshold: {processed_quality_score}% < {quality_threshold}%',
                    'raw_quality_report': raw_quality_report,
                    'processed_quality_report': processed_quality_report,
                    'execution_time_seconds': (datetime.now() - pipeline_start).total_seconds()
                }

            # ============= STAGE 4: AGGREGATION QUALITY VALIDATION =============
            self.logger.info("STAGE 4: Aggregation Quality Validation")

            # Validate aggregated data
            aggregation_quality_checks = {
                'summary_completeness': len(summary_df) > 0,
                'rankings_completeness': len(rankings_df) > 0,
                'ranking_integrity': self._validate_ranking_integrity(rankings_df),
                'summary_accuracy': self._validate_summary_accuracy(summary_df, metrics_df)
            }

            failed_aggregation_checks = [check for check, passed in aggregation_quality_checks.items() if not passed]

            if failed_aggregation_checks:
                return {
                    'status': 'FAILED',
                    'stage': 'AGGREGATION_QUALITY_GATE',
                    'error': f'Aggregation quality checks failed: {failed_aggregation_checks}',
                    'raw_quality_report': raw_quality_report,
                    'processed_quality_report': processed_quality_report,
                    'failed_aggregation_checks': failed_aggregation_checks,
                    'execution_time_seconds': (datetime.now() - pipeline_start).total_seconds()
                }

            # ============= STAGE 5: DATA PERSISTENCE WITH QUALITY METADATA =============
            self.logger.info("STAGE 5: Data Persistence with Quality Metadata")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Save processed data with quality metadata
            main_output = self.data_dir / "processed" / f"crypto_quality_validated_{timestamp}.parquet"

            # Add quality metadata to the main dataset
            enriched_df = metrics_df.copy()
            enriched_df['quality_score'] = processed_quality_score
            enriched_df['quality_validation_timestamp'] = datetime.now()
            enriched_df['pipeline_run_id'] = timestamp

            self.etl_pipeline.save_processed_data(enriched_df, str(main_output), "parquet")

            # Save aggregations
            summary_output = self.data_dir / "processed" / f"market_summary_quality_{timestamp}.csv"
            rankings_output = self.data_dir / "processed" / f"performance_rankings_quality_{timestamp}.json"

            self.etl_pipeline.save_processed_data(summary_df, str(summary_output), "csv")
            self.etl_pipeline.save_processed_data(rankings_df, str(rankings_output), "json")

            # Save quality reports
            raw_quality_file = self.quality_dir / f"raw_quality_report_{timestamp}.json"
            processed_quality_file = self.quality_dir / f"processed_quality_report_{timestamp}.json"

            self.quality_validator.save_quality_report(raw_quality_report, raw_quality_file)
            self.quality_validator.save_quality_report(processed_quality_report, processed_quality_file)

            # ============= PIPELINE COMPLETION =============
            pipeline_end = datetime.now()
            execution_time = (pipeline_end - pipeline_start).total_seconds()

            results = {
                'status': 'SUCCESS',
                'execution_time_seconds': execution_time,
                'input_file': input_file,
                'records_processed': len(metrics_df),
                'quality_assessment': {
                    'raw_data_quality_score': raw_quality_score,
                    'processed_data_quality_score': processed_quality_score,
                    'quality_threshold': quality_threshold,
                    'quality_gates_passed': 2,
                    'overall_quality_status': 'EXCELLENT' if processed_quality_score >= 95 else 'GOOD' if processed_quality_score >= 85 else 'ACCEPTABLE'
                },
                'output_files': {
                    'main_dataset': str(main_output),
                    'market_summary': str(summary_output),
                    'performance_rankings': str(rankings_output),
                    'raw_quality_report': str(raw_quality_file),
                    'processed_quality_report': str(processed_quality_file)
                },
                'pipeline_timestamp': pipeline_end.isoformat()
            }

            self.logger.info(f"✅ Quality-Enhanced ETL Pipeline completed successfully in {execution_time:.2f} seconds")
            self.logger.info(f"📊 Final quality score: {processed_quality_score}%")

            # Display comprehensive results
            self._display_quality_enhanced_results(results, summary_df, rankings_df)

            return results

        except Exception as e:
            self.logger.error(f"❌ Pipeline execution failed: {e}")
            import traceback
            traceback.print_exc()
            return {
                'status': 'FAILED',
                'stage': 'EXECUTION_ERROR',
                'error': str(e),
                'execution_time_seconds': (datetime.now() - pipeline_start).total_seconds()
            }

    def _validate_ranking_integrity(self, rankings_df: pd.DataFrame) -> bool:
        """Validate that rankings are properly calculated"""
        try:
            ranking_columns = [col for col in rankings_df.columns if 'rank' in col.lower()]

            for rank_col in ranking_columns:
                if rank_col in rankings_df.columns:
                    ranks = rankings_df[rank_col].dropna()
                    if len(ranks) > 0:
                        # Check if rankings are sequential starting from 1
                        expected_max_rank = len(ranks)
                        actual_max_rank = ranks.max()
                        actual_min_rank = ranks.min()

                        if actual_min_rank != 1 or actual_max_rank > expected_max_rank:
                            self.logger.warning(
                                f"Ranking integrity issue in {rank_col}: min={actual_min_rank}, max={actual_max_rank}, expected_max={expected_max_rank}")
                            return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating ranking integrity: {e}")
            return False

    def _validate_summary_accuracy(self, summary_df: pd.DataFrame, source_df: pd.DataFrame) -> bool:
        """Validate that summary aggregations are accurate"""
        try:
            # Basic validation - summary should not be empty if source has data
            if len(source_df) > 0 and len(summary_df) == 0:
                return False

            # If summary has coin counts, validate they don't exceed source
            if 'coin_count' in summary_df.columns:
                total_summary_coins = summary_df['coin_count'].sum()
                actual_coins = len(source_df)

                if total_summary_coins > actual_coins:
                    self.logger.warning(
                        f"Summary coin count ({total_summary_coins}) exceeds actual count ({actual_coins})")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating summary accuracy: {e}")
            return False

    def _display_quality_enhanced_results(self, results: Dict, summary_df: pd.DataFrame, rankings_df: pd.DataFrame):
        """Display comprehensive pipeline results with quality metrics"""
        print("\n" + "=" * 80)
        print("🏆 QUALITY-ENHANCED ETL PIPELINE RESULTS")
        print("=" * 80)

        print(f"\n📊 EXECUTION SUMMARY:")
        print(f"  Status: {results['status']}")
        print(f"  Execution Time: {results['execution_time_seconds']:.2f} seconds")
        print(f"  Records Processed: {results['records_processed']}")

        print(f"\n🔍 QUALITY ASSESSMENT:")
        qa = results['quality_assessment']
        print(f"  Raw Data Quality: {qa['raw_data_quality_score']:.1f}%")
        print(f"  Processed Data Quality: {qa['processed_data_quality_score']:.1f}%")
        print(f"  Quality Gates Passed: {qa['quality_gates_passed']}/2")
        print(f"  Overall Quality Status: {qa['overall_quality_status']}")

        print(f"\n🏆 TOP 3 PERFORMERS (24H):")
        if 'rank_24h' in rankings_df.columns:
            top_3 = rankings_df.nsmallest(3, 'rank_24h')
            for i, (_, row) in enumerate(top_3.iterrows(), 1):
                symbol = row.get('coin_symbol', 'N/A')
                name = row.get('coin_name', 'N/A')
                price = row.get('price_usd', 0)
                change = row.get('price_change_24h_pct', 0)
                print(f"  {i}. {name} ({symbol}): ${price:,.2f} ({change:+.2f}%)")

        print(f"\n📈 MARKET CATEGORIES:")
        if not summary_df.empty:
            category_summary = summary_df.head(3)  # Show top 3 categories
            for _, row in category_summary.iterrows():
                category = row.get('market_cap_category', 'Unknown')
                count = row.get('coin_count', 0)
                if pd.notna(category) and count > 0:
                    print(f"  {category}: {count} coins")

        print(f"\n📁 OUTPUT FILES:")
        for file_type, path in results['output_files'].items():
            print(f"  {file_type}: {Path(path).name}")
