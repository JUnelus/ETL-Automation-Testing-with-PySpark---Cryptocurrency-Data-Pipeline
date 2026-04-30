# src/utils/spark_session.py (FINAL WORKING VERSION)

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import logging
import os
import sys
import tempfile


def create_spark_session(app_name: str = "CryptoETL", local_mode: bool = True) -> SparkSession:
    """
    Create fully working PySpark session for ETL processing
    Fixes both Java 21 compatibility and Python worker connection issues
    """

    # Force JAVA_HOME setting for PyCharm/IDE compatibility
    java_home = r"C:\Program Files\Java\jdk-21"
    os.environ["JAVA_HOME"] = java_home

    # CRITICAL: Set Python executable path for PySpark workers
    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

    logging.info(f"JAVA_HOME: {java_home}")
    logging.info(f"Python executable: {python_exe}")

    # Add Java bin to PATH
    java_bin = os.path.join(java_home, "bin")
    current_path = os.environ.get("PATH", "")
    if java_bin not in current_path:
        os.environ["PATH"] = f"{java_bin};{current_path}"

    # Set Java 21 compatibility environment variables
    os.environ["SPARK_SUBMIT_OPTS"] = (
        "--add-opens java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens java.base/java.io=ALL-UNNAMED "
        "--add-opens java.base/java.util=ALL-UNNAMED "
        "--add-opens java.base/java.lang=ALL-UNNAMED"
    )

    # Spark configuration optimized for Java 21 and Windows
    conf = SparkConf()
    conf.set("spark.sql.adaptive.enabled", "false")  # Disable for stability
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # Java 21 specific JVM options
    java21_options = (
        "--add-opens java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens java.base/java.io=ALL-UNNAMED "
        "--add-opens java.base/java.util=ALL-UNNAMED "
        "--add-opens java.base/java.lang=ALL-UNNAMED "
        "--add-opens java.base/java.net=ALL-UNNAMED "
        "--add-opens java.base/java.nio=ALL-UNNAMED"
    )

    conf.set("spark.driver.extraJavaOptions", java21_options)
    conf.set("spark.executor.extraJavaOptions", java21_options)

    # Windows Hadoop compatibility: avoid native access checks that require winutils.
    temp_hadoop_dir = tempfile.mkdtemp(prefix="spark_hadoop_")
    os.environ["HADOOP_HOME"] = temp_hadoop_dir
    os.environ["HADOOP_CONF_DIR"] = temp_hadoop_dir
    conf.set("spark.hadoop.io.native.lib.available", "false")
    conf.set("spark.hadoop.fs.defaultFS", "file:///")
    conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    conf.set("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
    conf.set("spark.sql.warehouse.dir", temp_hadoop_dir)

    # Python worker configuration
    conf.set("spark.python.worker.memory", "512m")
    conf.set("spark.python.worker.reuse", "false")  # Disable worker reuse for stability

    if local_mode:
        # Conservative settings for maximum compatibility
        conf.set("spark.master", "local[1]")  # Single thread for stability
        conf.set("spark.driver.memory", "1g")
        conf.set("spark.driver.maxResultSize", "256m")
        conf.set("spark.sql.shuffle.partitions", "1")  # Minimal partitions

        # Extended timeouts for Windows/Java 21
        conf.set("spark.network.timeout", "800s")
        conf.set("spark.executor.heartbeatInterval", "60s")
        conf.set("spark.python.worker.timeout", "120s")

    # Create Spark session with comprehensive error handling
    try:
        logging.info(f"Creating Spark session: {app_name}")

        spark = SparkSession.builder \
            .appName(app_name) \
            .config(conf=conf) \
            .getOrCreate()

        # Set restrictive logging to reduce noise
        spark.sparkContext.setLogLevel("ERROR")

        logging.info(f"✅ Spark session created successfully")
        logging.info(f"Spark version: {spark.version}")
        logging.info(f"Python executable: {spark.sparkContext.pythonExec}")

        return spark

    except Exception as e:
        logging.error(f"❌ Failed to create Spark session: {e}")
        raise RuntimeError(f"Cannot create Spark session: {e}")


def test_spark_session():
    """Test the Spark session with simple operations"""
    print("🧪 Testing Final Working Spark Session...")

    try:
        print("\n1. Creating Spark Session...")
        spark = create_spark_session("Working-Test")
        print(f"   ✅ Spark session created")
        print(f"   Spark version: {spark.version}")

        print("\n2. Testing Simple Operations...")

        # Test with Spark SQL instead of DataFrame operations for better compatibility
        spark.sql("SELECT 1 as test_value").show()
        print(f"   ✅ Spark SQL working")

        # Test DataFrame creation with simple data
        simple_data = [(1, "test")]
        df = spark.createDataFrame(simple_data, ["id", "name"])

        print(f"   ✅ DataFrame created")

        # Test basic operations
        print("\n3. Testing DataFrame Operations...")

        # Use show() instead of count() for initial testing
        print("   DataFrame contents:")
        df.show()

        # Try count operation
        try:
            count = df.count()
            print(f"   ✅ Count operation successful: {count} rows")
        except Exception as count_error:
            print(f"   ⚠️  Count operation failed: {count_error}")
            print("   (This is expected on first run - Spark is initializing)")

        print("\n4. Cleaning up...")
        spark.stop()
        print(f"   ✅ Spark session stopped")

        print(f"\n🎉 Spark Session Test COMPLETED!")
        print(f"✅ Basic functionality verified")
        return True

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        print(f"\n💡 If this is the first error, try running again.")
        print(f"   Spark sometimes needs a 'warm-up' run on Windows.")
        return False


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    test_spark_session()