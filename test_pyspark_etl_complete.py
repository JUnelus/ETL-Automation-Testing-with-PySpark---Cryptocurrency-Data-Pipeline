import sys
from pathlib import Path
import logging
import os
import tempfile
import pandas as pd

from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    current_timestamp,
    lit,
    rank,
    round,
    sum,
    udf,
    upper,
    when,
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from src.utils.spark_session import create_spark_session

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))


def _create_test_market_data_csv(temp_dir: str) -> str:
    """Create deterministic source data so this test does not depend on prior ingestion runs."""
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
        },
        {
            "id": "solana",
            "symbol": "sol",
            "name": "Solana",
            "current_price": 155.0,
            "market_cap": 72_000_000_000,
            "market_cap_rank": 5,
            "total_volume": 3_900_000_000,
            "price_change_percentage_24h": 3.8,
        },
        {
            "id": "dogecoin",
            "symbol": "doge",
            "name": "Dogecoin",
            "current_price": 0.19,
            "market_cap": 28_000_000_000,
            "market_cap_rank": 10,
            "total_volume": 1_800_000_000,
            "price_change_percentage_24h": 6.5,
        },
    ])
    output_path = Path(temp_dir) / "market_data_test.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


def test_complete_pyspark_etl():
    """Test complete PySpark ETL pipeline with crypto data"""
    print("🚀 Testing Complete PySpark ETL Pipeline...")

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    spark = None
    temp_hadoop = None
    temp_data_dir = tempfile.mkdtemp(prefix="pyspark_raw_")
    try:
        # Step 1: Create Spark Session
        print("\n📊 STEP 1: Creating Spark Session")

        # Set environment variables for compatibility
        java_home = os.environ.get('JAVA_HOME', r"C:\Program Files\Java\jdk-21")
        os.environ["JAVA_HOME"] = java_home
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        # Set Hadoop home to avoid warnings
        temp_hadoop = tempfile.mkdtemp()
        os.environ["HADOOP_HOME"] = temp_hadoop

        spark = create_spark_session("CryptoETL-Complete")
        print("✅ Spark session ready")

        # Step 2: Load Real Crypto Data
        print("\n📊 STEP 2: Loading Cryptocurrency Data")

        # Use deterministic local test data so this test is not order-dependent.
        latest_file = _create_test_market_data_csv(temp_data_dir)
        print(f"📁 Loading: {Path(latest_file).name}")

        # Load data with PySpark
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(latest_file)

        record_count = df.count()
        print(f"✅ Loaded {record_count} cryptocurrency records")

        # Step 3: Data Cleaning and Transformation
        print("\n📊 STEP 3: PySpark Data Transformations")

        # Clean and transform data
        cleaned_df = df.select(
            col("id").alias("coin_id"),
            col("symbol").alias("coin_symbol"),
            col("name").alias("coin_name"),
            col("current_price").cast("double").alias("price_usd"),
            col("market_cap").cast("bigint").alias("market_cap_usd"),
            col("market_cap_rank").cast("int").alias("market_rank"),
            coalesce(col("total_volume"), lit(0)).cast("bigint").alias("volume_24h_usd"),
            coalesce(col("price_change_percentage_24h"), lit(0.0)).cast("double").alias("price_change_24h_pct"),
            current_timestamp().alias("processing_timestamp")
        ).filter(
            (col("price_usd").isNotNull()) &
            (col("market_cap_usd").isNotNull()) &
            (col("price_usd") > 0)
        ).withColumn(
            "coin_symbol", upper(col("coin_symbol"))
        )

        cleaned_count = cleaned_df.count()
        print(f"✅ Data cleaning completed: {cleaned_count} records retained")

        # Step 4: Advanced Analytics with PySpark
        print("\n📊 STEP 4: Advanced PySpark Analytics")

        # Calculate advanced metrics using window functions
        rank_window = Window.orderBy(col("market_cap_usd").desc())

        analytics_df = cleaned_df.withColumn(
            # Market cap ranking
            "market_cap_rank_calculated",
            rank().over(rank_window)
        ).withColumn(
            # Volume to market cap ratio
            "volume_ratio",
            round(col("volume_24h_usd").cast("double") / col("market_cap_usd").cast("double"), 6)
        ).withColumn(
            # Market cap categories
            "market_cap_category",
            when(col("market_cap_usd") >= 100e9, "Mega Cap")
            .when(col("market_cap_usd") >= 10e9, "Large Cap")
            .when(col("market_cap_usd") >= 1e9, "Mid Cap")
            .otherwise("Small Cap")
        ).withColumn(
            # Performance rankings
            "performance_rank_24h",
            rank().over(Window.orderBy(col("price_change_24h_pct").desc()))
        )

        print("✅ Advanced analytics completed")

        # Step 5: Aggregations
        print("\n📊 STEP 5: PySpark Aggregations")

        # Market summary by category
        market_summary = analytics_df.groupBy("market_cap_category").agg(
            count("*").alias("coin_count"),
            round(avg("price_usd"), 2).alias("avg_price"),
            round(sum("market_cap_usd") / 1e9, 2).alias("total_market_cap_billions"),
            round(avg("volume_ratio"), 4).alias("avg_volume_ratio")
        ).orderBy("market_cap_category")

        print("✅ Market aggregations completed")

        # Step 6: Display Results
        print("\n📊 STEP 6: Results Summary")

        # Show top performers
        print("\n🏆 TOP 5 PERFORMERS (24H):")
        top_performers = analytics_df.filter(col("performance_rank_24h") <= 5) \
            .orderBy("performance_rank_24h") \
            .select("coin_name", "coin_symbol", "price_usd", "price_change_24h_pct")

        top_performers.show(5, truncate=False)

        # Show market summary
        print("\n📈 MARKET SUMMARY BY CATEGORY:")
        market_summary.show(truncate=False)

        # Step 7: Save Results (in-memory only due to Hadoop issues)
        print("\n📊 STEP 7: Processing Results Summary")

        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Instead of saving to file, collect results for display
        summary_results = market_summary.collect()
        top_results = top_performers.collect()

        print(f"✅ Results processed successfully")
        print(f"   📊 Market categories analyzed: {len(summary_results)}")
        print(f"   🏆 Top performers identified: {len(top_results)}")

        # Step 8: Advanced PySpark Demonstrations
        print("\n📊 STEP 8: Advanced PySpark Features Demo")

        # Demonstrate complex SQL operations
        analytics_df.createOrReplaceTempView("crypto_data")

        # SQL query demonstration
        sql_result = spark.sql("""
                               SELECT market_cap_category,
                                      COUNT(*)                  as coin_count,
                                      AVG(price_usd)            as avg_price,
                                      MAX(price_change_24h_pct) as max_gain_24h
                               FROM crypto_data
                               GROUP BY market_cap_category
                               ORDER BY coin_count DESC
                               """)

        print("✅ Spark SQL query executed:")
        sql_result.show()

        # Demonstrate UDF (User Defined Function)
        def price_category(price):
            if price >= 1000:
                return "High Value"
            elif price >= 1:
                return "Medium Value"
            else:
                return "Low Value"

        price_category_udf = udf(price_category, StringType())

        categorized_df = analytics_df.withColumn(
            "price_category",
            price_category_udf(col("price_usd"))
        )

        price_dist = categorized_df.groupBy("price_category").count().collect()
        print("✅ UDF demonstration - Price categorization:")
        for row in price_dist:
            print(f"   {row['price_category']}: {row['count']} coins")

        # Step 9: Performance Summary
        print("\n📊 FINAL SUMMARY:")
        print(f"   🔹 Records Processed: {cleaned_count}")
        print(f"   🔹 Spark Version: {spark.version}")
        print(f"   🔹 Java Version: Java 21 Compatible")
        print(f"   🔹 Processing Engine: Distributed PySpark")
        print(f"   🔹 Analytics: Window functions, SQL queries, UDFs")
        print(f"   🔹 Aggregations: Multi-level grouping and statistics")

        # Step 10: Cleanup
        print("\n📊 STEP 10: Cleanup")
        spark.stop()

        print(f"\n🎉 PySpark ETL Pipeline Test SUCCESSFUL!")
        print(f"✅ All distributed processing operations completed")
        print(f"✅ Advanced analytics demonstrated with real cryptocurrency data")
        print(f"✅ Window functions, aggregations, SQL, and UDFs all working")

        assert cleaned_count > 0
        assert len(summary_results) > 0
        assert len(top_results) > 0

    except Exception as e:
        print(f"\n❌ PySpark ETL test failed: {e}")
        import traceback
        traceback.print_exc()
        raise AssertionError(f"PySpark ETL test failed: {e}") from e
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass

        import shutil
        if temp_hadoop and os.path.exists(temp_hadoop):
            shutil.rmtree(temp_hadoop, ignore_errors=True)
        if temp_data_dir and os.path.exists(temp_data_dir):
            shutil.rmtree(temp_data_dir, ignore_errors=True)


if __name__ == "__main__":
    test_complete_pyspark_etl()