from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import glob
import json
import sys

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.spark_session import create_spark_session


class PySparkQualityETLPipeline:
    """
    Production PySpark ETL Pipeline with Integrated Data Quality Validation
    Demonstrates enterprise-scale distributed computing for ETL Quality Engineering
    """

    def __init__(self, app_name: str = "PySpark-Quality-ETL"):
        self.app_name = app_name
        self.spark = create_spark_session(app_name)
        self.logger = logging.getLogger(__name__)

        # Setup directories
        self.base_dir = Path(__file__).parent.parent.parent
        self.data_dir = self.base_dir / "data"
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.quality_dir = self.data_dir / "quality_reports"

        # Create directories
        for directory in [self.processed_dir, self.quality_dir]:
            directory.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Initialized PySpark Quality ETL Pipeline: {app_name}")
        self.logger.info(f"Spark version: {self.spark.version}")

    def get_latest_raw_file(self, pattern: str = "*market_data*.csv") -> str:
        """Get the most recent raw data file"""
        files = glob.glob(str(self.raw_dir / pattern))
        if not files:
            raise FileNotFoundError(f"No files found matching pattern: {pattern}")

        latest_file = max(files, key=lambda f: Path(f).stat().st_ctime)
        self.logger.info(f"Using latest raw file: {latest_file}")
        return latest_file

    def load_raw_data(self, file_path: str) -> DataFrame:
        """Load raw CSV data with PySpark optimizations"""
        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("multiline", "true") \
                .option("escape", '"') \
                .csv(file_path)

            record_count = df.count()
            self.logger.info(f"Loaded {record_count} records from {file_path}")
            return df

        except Exception as e:
            self.logger.error(f"Failed to load data from {file_path}: {e}")
            raise

    def validate_data_quality_spark(self, df: DataFrame, stage: str = "raw") -> Dict:
        """
        PySpark-native data quality validation
        Demonstrates advanced Spark SQL and DataFrame operations for quality checking
        """
        self.logger.info(f"Running PySpark data quality validation for {stage} data")

        total_records = df.count()
        validation_results = {
            'stage': stage,
            'total_records': total_records,
            'validation_timestamp': datetime.now().isoformat(),
            'spark_validations': {}
        }

        try:
            # 1. Completeness Check using Spark aggregations
            completeness_results = {}
            critical_columns = ['id', 'symbol', 'name', 'current_price', 'market_cap'] if stage == 'raw' else \
                ['coin_id', 'coin_symbol', 'coin_name', 'price_usd', 'market_cap_usd']

            for col_name in critical_columns:
                if col_name in df.columns:
                    null_count = df.filter(col(col_name).isNull() | (col(col_name) == "")).count()
                    completeness_pct = (
                                (total_records - null_count) / total_records * 100) if total_records > 0 else 100

                    completeness_results[col_name] = {
                        'completeness_percentage': round(completeness_pct, 2),
                        'null_count': null_count,
                        'status': 'PASS' if completeness_pct >= 95 else 'FAIL'
                    }

            validation_results['spark_validations']['completeness'] = completeness_results

            # 2. Accuracy Check using Spark SQL
            accuracy_results = {}

            if stage == 'raw':
                price_col, market_cap_col = 'current_price', 'market_cap'
            else:
                price_col, market_cap_col = 'price_usd', 'market_cap_usd'

            if price_col in df.columns:
                # Check for positive prices
                negative_prices = df.filter(col(price_col) <= 0).count()
                accuracy_results['positive_prices'] = {
                    'invalid_count': negative_prices,
                    'status': 'PASS' if negative_prices == 0 else 'FAIL'
                }

            if market_cap_col in df.columns:
                # Check for positive market caps
                negative_market_caps = df.filter(col(market_cap_col) <= 0).count()
                accuracy_results['positive_market_caps'] = {
                    'invalid_count': negative_market_caps,
                    'status': 'PASS' if negative_market_caps == 0 else 'FAIL'
                }

            validation_results['spark_validations']['accuracy'] = accuracy_results

            # 3. Consistency Check using Spark string functions
            consistency_results = {}

            symbol_col = 'symbol' if stage == 'raw' else 'coin_symbol'
            if symbol_col in df.columns:
                # Check symbol case consistency
                non_uppercase_symbols = df.filter(
                    col(symbol_col) != upper(col(symbol_col))
                ).count()

                consistency_results['uppercase_symbols'] = {
                    'inconsistent_count': non_uppercase_symbols,
                    'status': 'PASS' if non_uppercase_symbols == 0 else 'FAIL'
                }

            validation_results['spark_validations']['consistency'] = consistency_results

            # 4. Uniqueness Check using Spark distinct operations
            uniqueness_results = {}

            id_col = 'id' if stage == 'raw' else 'coin_id'
            if id_col in df.columns:
                total_ids = df.select(id_col).filter(col(id_col).isNotNull()).count()
                unique_ids = df.select(id_col).filter(col(id_col).isNotNull()).distinct().count()
                duplicate_count = total_ids - unique_ids

                uniqueness_results['unique_ids'] = {
                    'duplicate_count': duplicate_count,
                    'status': 'PASS' if duplicate_count == 0 else 'FAIL'
                }

            validation_results['spark_validations']['uniqueness'] = uniqueness_results

            # Calculate overall quality score
            all_checks = []
            for category in validation_results['spark_validations'].values():
                for check in category.values():
                    all_checks.append(1 if check['status'] == 'PASS' else 0)

            quality_score = (sum(all_checks) / len(all_checks) * 100) if all_checks else 0
            validation_results['quality_score'] = round(quality_score, 2)
            validation_results['overall_status'] = 'PASS' if quality_score >= 80 else 'FAIL'

            self.logger.info(f"PySpark quality validation completed: {quality_score}% score")
            return validation_results

        except Exception as e:
            self.logger.error(f"PySpark quality validation failed: {e}")
            return {
                'stage': stage,
                'total_records': total_records,
                'validation_timestamp': datetime.now().isoformat(),
                'error': str(e),
                'quality_score': 0,
                'overall_status': 'ERROR'
            }

    def clean_and_transform_data(self, df: DataFrame) -> DataFrame:
        """
        Advanced PySpark transformations with quality-aware processing
        """
        self.logger.info("Starting PySpark data cleaning and transformation")

        # Schema transformation with data quality filtering
        cleaned_df = df.select(
            col("id").alias("coin_id"),
            col("symbol").alias("coin_symbol"),
            col("name").alias("coin_name"),
            col("current_price").cast("double").alias("price_usd"),
            col("market_cap").cast("bigint").alias("market_cap_usd"),
            coalesce(col("market_cap_rank"), lit(999)).cast("int").alias("market_rank"),
            coalesce(col("total_volume"), lit(0)).cast("bigint").alias("volume_24h_usd"),
            coalesce(col("price_change_percentage_24h"), lit(0.0)).cast("double").alias("price_change_24h_pct"),
            coalesce(col("price_change_percentage_7d"), lit(0.0)).cast("double").alias("price_change_7d_pct"),
            current_timestamp().alias("extraction_timestamp"),
            date_format(current_timestamp(), "yyyy-MM-dd").alias("extraction_date")
        ).filter(
            # Quality filters using Spark SQL
            col("price_usd").isNotNull() &
            col("market_cap_usd").isNotNull() &
            col("price_usd") > 0 &
            col("market_cap_usd") > 0
        ).withColumn(
            # Standardize symbols to uppercase
            "coin_symbol", upper(col("coin_symbol"))
        )

        # Cache for multiple operations
        cleaned_df.cache()

        record_count = cleaned_df.count()
        self.logger.info(f"Data cleaning completed: {record_count} records retained")

        return cleaned_df

    def calculate_advanced_metrics(self, df: DataFrame) -> DataFrame:
        """
        Calculate advanced financial metrics using PySpark window functions
        """
        self.logger.info("Calculating advanced metrics with PySpark")

        # Window specifications for advanced analytics
        rank_window = Window.orderBy(col("market_cap_usd").desc())

        metrics_df = df.withColumn(
            # Volume to Market Cap Ratio
            "volume_to_market_cap_ratio",
            round(col("volume_24h_usd").cast("double") / col("market_cap_usd").cast("double"), 6)
        ).withColumn(
            # Volatility Score using advanced Spark functions
            "volatility_score",
            round(
                sqrt(
                    pow(coalesce(col("price_change_24h_pct"), lit(0)), 2) +
                    pow(coalesce(col("price_change_7d_pct"), lit(0)), 2)
                ) / 2,
                2
            )
        ).withColumn(
            # Market Cap Categories using advanced when/otherwise
            "market_cap_category",
            when(col("market_cap_usd") >= 100e9, "Mega Cap")
            .when(col("market_cap_usd") >= 10e9, "Large Cap")
            .when(col("market_cap_usd") >= 1e9, "Mid Cap")
            .when(col("market_cap_usd") >= 100e6, "Small Cap")
            .otherwise("Micro Cap")
        ).withColumn(
            # Volatility Categories
            "volatility_category",
            when(col("volatility_score") >= 15, "Very High")
            .when(col("volatility_score") >= 10, "High")
            .when(col("volatility_score") >= 5, "Medium")
            .when(col("volatility_score") >= 2, "Low")
            .otherwise("Very Low")
        ).withColumn(
            # Trading Activity Level
            "trading_activity",
            when(col("volume_to_market_cap_ratio") >= 0.5, "Very Active")
            .when(col("volume_to_market_cap_ratio") >= 0.1, "Active")
            .when(col("volume_to_market_cap_ratio") >= 0.05, "Moderate")
            .otherwise("Low Activity")
        ).withColumn(
            # Market Cap Rank using window functions
            "calculated_rank",
            rank().over(rank_window)
        ).withColumn(
            # Performance Rankings
            "rank_24h",
            rank().over(Window.orderBy(col("price_change_24h_pct").desc()))
        ).withColumn(
            "rank_7d",
            rank().over(Window.orderBy(col("price_change_7d_pct").desc()))
        )

        self.logger.info("Advanced metrics calculation completed")
        return metrics_df

    def create_spark_aggregations(self, df: DataFrame) -> DataFrame:
        """
        Create comprehensive market summary using advanced Spark aggregations
        """
        self.logger.info("Creating Spark aggregations")

        summary_df = df.groupBy(
            "market_cap_category",
            "volatility_category"
        ).agg(
            count("*").alias("coin_count"),
            round(avg("price_usd"), 2).alias("avg_price_usd"),
            round(sum("market_cap_usd") / 1e9, 2).alias("total_market_cap_billions"),
            round(sum("volume_24h_usd") / 1e9, 2).alias("total_volume_billions"),
            round(avg("volume_to_market_cap_ratio"), 4).alias("avg_volume_ratio"),
            round(avg("volatility_score"), 2).alias("avg_volatility_score"),
            round(avg("price_change_24h_pct"), 2).alias("avg_24h_change_pct"),
            min("price_usd").alias("min_price"),
            max("price_usd").alias("max_price"),
            collect_list("coin_symbol").alias("coins_in_category")
        ).orderBy(
            "market_cap_category",
            "volatility_category"
        )

        self.logger.info("Spark aggregations completed")
        return summary_df

    def save_with_quality_metadata(self, df: DataFrame, output_path: str,
                                   quality_score: float, format_type: str = "parquet") -> None:
        """
        Save data with quality metadata using PySpark optimizations
        """
        try:
            # Add quality metadata columns
            enriched_df = df.withColumn("quality_score", lit(quality_score)) \
                .withColumn("quality_validation_timestamp", current_timestamp()) \
                .withColumn("pipeline_type", lit("pyspark")) \
                .withColumn("spark_version", lit(self.spark.version))

            # Optimize partitioning for better performance
            writer = enriched_df.coalesce(2)  # Optimize file count

            if format_type.lower() == "parquet":
                writer.write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(output_path)

            elif format_type.lower() == "csv":
                writer.write \
                    .mode("overwrite") \
                    .option("header", "true") \
                    .csv(output_path)

            self.logger.info(f"Successfully saved data with quality metadata to {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to save data with quality metadata: {e}")
            raise

    def run_pyspark_quality_pipeline(self, input_file: str = None,
                                     quality_threshold: float = 85.0) -> Dict:
        """
        Execute complete PySpark ETL pipeline with integrated quality validation
        """
        pipeline_start = datetime.now()
        self.logger.info("🚀 Starting PySpark Quality ETL Pipeline")

        try:
            # Stage 1: Data Ingestion
            self.logger.info("STAGE 1: PySpark Data Ingestion")
            if input_file is None:
                input_file = self.get_latest_raw_file()

            raw_df = self.load_raw_data(input_file)

            # Stage 2: Raw Data Quality Validation
            self.logger.info("STAGE 2: Raw Data Quality Validation with PySpark")
            raw_quality_results = self.validate_data_quality_spark(raw_df, "raw")
            raw_quality_score = raw_quality_results['quality_score']

            if raw_quality_score < 70:
                return {
                    'status': 'FAILED',
                    'stage': 'RAW_DATA_QUALITY_GATE',
                    'error': f'Raw data quality too low: {raw_quality_score}%',
                    'raw_quality_results': raw_quality_results,
                    'execution_time_seconds': (datetime.now() - pipeline_start).total_seconds()
                }

            # Stage 3: ETL Processing with PySpark
            self.logger.info("STAGE 3: PySpark ETL Processing")
            cleaned_df = self.clean_and_transform_data(raw_df)
            metrics_df = self.calculate_advanced_metrics(cleaned_df)

            # Stage 4: Processed Data Quality Validation
            self.logger.info("STAGE 4: Processed Data Quality Validation")
            processed_quality_results = self.validate_data_quality_spark(metrics_df, "processed")
            processed_quality_score = processed_quality_results['quality_score']

            if processed_quality_score < quality_threshold:
                return {
                    'status': 'FAILED',
                    'stage': 'PROCESSED_DATA_QUALITY_GATE',
                    'error': f'Processed data quality below threshold: {processed_quality_score}% < {quality_threshold}%',
                    'raw_quality_results': raw_quality_results,
                    'processed_quality_results': processed_quality_results,
                    'execution_time_seconds': (datetime.now() - pipeline_start).total_seconds()
                }

            # Stage 5: Advanced Aggregations
            self.logger.info("STAGE 5: Advanced PySpark Aggregations")
            summary_df = self.create_spark_aggregations(metrics_df)

            # Stage 6: Data Persistence with PySpark Optimizations
            self.logger.info("STAGE 6: Data Persistence with PySpark Optimizations")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Save main dataset
            main_output = self.processed_dir / f"crypto_pyspark_quality_{timestamp}.parquet"
            self.save_with_quality_metadata(
                metrics_df,
                str(main_output),
                processed_quality_score,
                "parquet"
            )

            # Save aggregations
            summary_output = self.processed_dir / f"market_summary_pyspark_{timestamp}.parquet"
            summary_df.write.mode("overwrite").parquet(str(summary_output))

            # Save quality reports
            raw_quality_file = self.quality_dir / f"pyspark_raw_quality_{timestamp}.json"
            processed_quality_file = self.quality_dir / f"pyspark_processed_quality_{timestamp}.json"

            with open(raw_quality_file, 'w') as f:
                json.dump(raw_quality_results, f, indent=2, default=str)
            with open(processed_quality_file, 'w') as f:
                json.dump(processed_quality_results, f, indent=2, default=str)

            # Pipeline completion
            pipeline_end = datetime.now()
            execution_time = (pipeline_end - pipeline_start).total_seconds()

            results = {
                'status': 'SUCCESS',
                'execution_time_seconds': execution_time,
                'input_file': input_file,
                'records_processed': metrics_df.count(),
                'quality_assessment': {
                    'raw_data_quality_score': raw_quality_score,
                    'processed_data_quality_score': processed_quality_score,
                    'quality_threshold': quality_threshold,
                    'pipeline_type': 'pyspark',
                    'spark_version': self.spark.version
                },
                'output_files': {
                    'main_dataset': str(main_output),
                    'market_summary': str(summary_output),
                    'raw_quality_report': str(raw_quality_file),
                    'processed_quality_report': str(processed_quality_file)
                }
            }

            self.logger.info(f"✅ PySpark Quality ETL Pipeline completed in {execution_time:.2f} seconds")
            self.logger.info(f"📊 Final quality score: {processed_quality_score}%")

            # Display results
            self._display_pyspark_results(results, summary_df, metrics_df)

            return results

        except Exception as e:
            self.logger.error(f"❌ PySpark pipeline execution failed")
