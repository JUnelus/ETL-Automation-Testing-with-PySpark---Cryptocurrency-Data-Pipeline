from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from typing import List, Dict, Tuple


class CryptoDataTransformations:
    """
    Advanced PySpark transformations for cryptocurrency data
    Demonstrates distributed data processing skills for ETL roles
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def load_raw_data(self, file_path: str, file_format: str = "csv") -> DataFrame:
        """
        Load raw data with schema inference and validation

        Args:
            file_path: Path to the data file
            file_format: File format (csv, json, parquet)

        Returns:
            PySpark DataFrame
        """
        try:
            if file_format.lower() == "csv":
                df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("multiline", "true") \
                    .csv(file_path)

            elif file_format.lower() == "json":
                df = self.spark.read \
                    .option("multiline", "true") \
                    .json(file_path)

            elif file_format.lower() == "parquet":
                df = self.spark.read.parquet(file_path)

            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            self.logger.info(f"Loaded {df.count()} records from {file_path}")
            return df

        except Exception as e:
            self.logger.error(f"Failed to load data from {file_path}: {e}")
            raise

    def clean_market_data(self, df: DataFrame) -> DataFrame:
        """
        Clean and standardize cryptocurrency market data
        Demonstrates data quality and transformation skills
        """
        self.logger.info("Starting data cleaning transformation")

        # Define schema transformation with proper data types
        cleaned_df = df.select(
            col("id").alias("coin_id"),
            col("symbol").alias("coin_symbol"),
            col("name").alias("coin_name"),
            col("current_price").cast("double").alias("price_usd"),
            col("market_cap").cast("bigint").alias("market_cap_usd"),
            col("market_cap_rank").cast("int").alias("market_rank"),
            col("total_volume").cast("bigint").alias("volume_24h_usd"),
            col("price_change_percentage_24h").cast("double").alias("price_change_24h_pct"),
            col("price_change_percentage_7d").cast("double").alias("price_change_7d_pct"),
            col("price_change_percentage_30d").cast("double").alias("price_change_30d_pct"),
            current_timestamp().alias("extraction_timestamp"),
            date_format(current_timestamp(), "yyyy-MM-dd").alias("extraction_date")
        ).filter(
            # Data quality filters
            col("price_usd").isNotNull() &
            col("market_cap_usd").isNotNull() &
            col("price_usd") > 0 &
            col("market_cap_usd") > 0
        )

        # Standardize symbol to uppercase
        cleaned_df = cleaned_df.withColumn(
            "coin_symbol",
            upper(col("coin_symbol"))
        )

        self.logger.info(f"Data cleaning completed: {cleaned_df.count()} records retained")
        return cleaned_df

    def calculate_advanced_metrics(self, df: DataFrame) -> DataFrame:
        """
        Calculate advanced financial metrics using PySpark
        Demonstrates complex analytical transformations
        """
        self.logger.info("Calculating advanced market metrics")

        # Window specification for ranking
        rank_window = Window.orderBy(col("market_cap_usd").desc())

        # Calculate comprehensive metrics
        metrics_df = df.withColumn(
            # Volume to Market Cap Ratio
            "volume_to_market_cap_ratio",
            round(col("volume_24h_usd") / col("market_cap_usd"), 6)
        ).withColumn(
            # Price Volatility Score (combines 24h, 7d, 30d changes)
            "volatility_score",
            round(
                sqrt(
                    pow(coalesce(col("price_change_24h_pct"), lit(0)), 2) +
                    pow(coalesce(col("price_change_7d_pct"), lit(0)), 2) +
                    pow(coalesce(col("price_change_30d_pct"), lit(0)), 2)
                ) / 3,
                2
            )
        ).withColumn(
            # Market Cap Categories
            "market_cap_category",
            when(col("market_cap_usd") >= 100e9, "Mega Cap")
            .when(col("market_cap_usd") >= 10e9, "Large Cap")
            .when(col("market_cap_usd") >= 1e9, "Mid Cap")
            .when(col("market_cap_usd") >= 100e6, "Small Cap")
            .otherwise("Micro Cap")
        ).withColumn(
            # Price Volatility Categories
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
            # Market Cap Rank (recalculated for consistency)
            "calculated_rank",
            rank().over(rank_window)
        )

        self.logger.info("Advanced metrics calculation completed")
        return metrics_df

    def create_market_summary(self, df: DataFrame) -> DataFrame:
        """
        Create comprehensive market summary using advanced aggregations
        Demonstrates complex SQL and aggregation skills
        """
        self.logger.info("Creating market summary aggregations")

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

        self.logger.info("Market summary creation completed")
        return summary_df

    def create_performance_rankings(self, df: DataFrame) -> DataFrame:
        """
        Create performance rankings across different timeframes
        Demonstrates window functions and ranking logic
        """
        self.logger.info("Creating performance rankings")

        # Window specifications for different rankings
        performance_windows = {
            "24h": Window.orderBy(col("price_change_24h_pct").desc()),
            "7d": Window.orderBy(col("price_change_7d_pct").desc()),
            "30d": Window.orderBy(col("price_change_30d_pct").desc()),
            "market_cap": Window.orderBy(col("market_cap_usd").desc()),
            "volume": Window.orderBy(col("volume_24h_usd").desc())
        }

        rankings_df = df.select("*")

        # Add rankings for each timeframe
        for timeframe, window_spec in performance_windows.items():
            rankings_df = rankings_df.withColumn(
                f"rank_{timeframe}",
                rank().over(window_spec)
            )

        # Calculate composite performance score
        rankings_df = rankings_df.withColumn(
            "composite_performance_score",
            round(
                (coalesce(col("price_change_24h_pct"), lit(0)) * 0.4 +
                 coalesce(col("price_change_7d_pct"), lit(0)) * 0.35 +
                 coalesce(col("price_change_30d_pct"), lit(0)) * 0.25),
                2
            )
        ).withColumn(
            "composite_rank",
            rank().over(Window.orderBy(col("composite_performance_score").desc()))
        )

        self.logger.info("Performance rankings completed")
        return rankings_df

    def save_processed_data(self, df: DataFrame, output_path: str,
                            format_type: str = "parquet", partitions: List[str] = None) -> None:
        """
        Save processed data with optimizations
        Demonstrates data lake best practices
        """
        try:
            writer = df.coalesce(1) if df.count() < 10000 else df  # Optimize small datasets

            if partitions:
                writer = writer.write.partitionBy(*partitions)
            else:
                writer = writer.write

            writer.mode("overwrite").option("compression", "snappy")

            if format_type.lower() == "parquet":
                writer.parquet(output_path)
            elif format_type.lower() == "csv":
                writer.option("header", "true").csv(output_path)
            elif format_type.lower() == "json":
                writer.json(output_path)
            else:
                raise ValueError(f"Unsupported output format: {format_type}")

            self.logger.info(f"Successfully saved data to {output_path} as {format_type}")

        except Exception as e:
            self.logger.error(f"Failed to save data: {e}")
            raise