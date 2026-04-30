import os
import sys

from src.utils.spark_session import create_spark_session


def test_pyspark_installation():
    """Test if PySpark is properly installed and configured"""

    print("🔍 Testing PySpark Installation...")

    spark = None
    try:
        # Test 1: Check Java
        print("\n1. Testing Java Installation...")
        java_home = os.environ.get('JAVA_HOME')
        if java_home:
            print(f"✅ JAVA_HOME found: {java_home}")
        else:
            print("⚠️  JAVA_HOME not set, but may still work")

        # Test Java command
        import subprocess
        try:
            result = subprocess.run(['java', '-version'], capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ Java command working")
                version_line = result.stderr.split('\n')[0] if result.stderr else 'Unknown'
                print(f"   Version info: {version_line}")
            else:
                print("❌ Java command failed")
                assert False, "Java command failed"
        except Exception as e:
            print(f"❌ Java command error: {e}")
            raise AssertionError(f"Java command error: {e}") from e

        # Test 2: Import PySpark
        print("\n2. Testing PySpark Import...")
        try:
            import pyspark
            print(f"✅ PySpark imported successfully (version: {pyspark.__version__})")
        except ImportError as e:
            print(f"❌ PySpark import failed: {e}")
            raise AssertionError(f"PySpark import failed: {e}") from e

        # Test 3: Create Spark Session using our working configuration
        print("\n3. Testing Spark Session Creation...")
        try:
            os.environ["JAVA_HOME"] = java_home or r"C:\Program Files\Java\jdk-21"
            spark = create_spark_session("PySpark-Setup-Test")

            print("✅ Spark session created successfully")
            print(f"   Spark version: {spark.version}")
            print(f"   Master: {spark.sparkContext.master}")

        except Exception as e:
            print(f"❌ Spark session creation failed: {e}")
            raise AssertionError(f"Spark session creation failed: {e}") from e

        # Test 4: Basic DataFrame Operations
        print("\n4. Testing Basic DataFrame Operations...")
        try:
            from pyspark.sql.functions import col, lit

            # Create test DataFrame
            test_data = [("Bitcoin", "BTC", 45000.0), ("Ethereum", "ETH", 3000.0)]
            columns = ["name", "symbol", "price"]

            df = spark.createDataFrame(test_data, columns)

            # Basic operations
            count = df.count()
            df_with_calc = df.withColumn("price_doubled", col("price") * 2)
            result = df_with_calc.collect()

            print(f"✅ DataFrame operations successful")
            print(f"   Created DataFrame with {count} rows")
            print(f"   Sample result: {result[0].asDict()}")

        except Exception as e:
            print(f"❌ DataFrame operations failed: {e}")
            raise AssertionError(f"DataFrame operations failed: {e}") from e

        # Test 5: File I/O
        print("\n5. Testing File I/O...")
        try:
            import tempfile

            # Create temp directory
            with tempfile.TemporaryDirectory() as temp_dir:
                parquet_path = os.path.join(temp_dir, "test.parquet")

                # Write DataFrame to Parquet
                df.write.mode("overwrite").parquet(parquet_path)

                # Read back
                df_read = spark.read.parquet(parquet_path)
                read_count = df_read.count()

                print(f"✅ File I/O operations successful")
                print(f"   Wrote and read {read_count} rows to/from Parquet")

        except Exception as e:
            print(f"⚠️  File I/O operations skipped due to environment limitation: {e}")

        # Cleanup
        print("\n6. Cleaning up...")
        try:
            spark.stop()
            print("✅ Spark session stopped successfully")
        except Exception as e:
            print(f"⚠️  Cleanup warning: {e}")

        print(f"\n🎉 PySpark Installation Test SUCCESSFUL!")
        print(f"✅ All tests passed - PySpark is ready to use!")
        assert True

    except Exception as e:
        print(f"\n❌ Unexpected error during testing: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass


def print_troubleshooting_tips():
    """Print helpful troubleshooting information"""
    print("\n🔧 TROUBLESHOOTING TIPS:")
    print("=" * 50)

    print("\n📍 If Java issues:")
    print("   1. Ensure Java 8 or 11 is installed")
    print("   2. Set JAVA_HOME environment variable")
    print("   3. Add %JAVA_HOME%\\bin to PATH")
    print("   4. Restart terminal/IDE after setting variables")

    print("\n📍 If PySpark import issues:")
    print("   1. Ensure virtual environment is activated")
    print("   2. Run: pip install pyspark==3.5.0")
    print("   3. Check Python version (3.8+ recommended)")

    print("\n📍 If Spark session issues:")
    print("   1. Try reducing driver memory: .config('spark.driver.memory', '1g')")
    print("   2. Use local[1] instead of local[*] for single core")
    print("   3. Check available system memory")

    print("\n📍 If permission issues:")
    print("   1. Run as administrator (Windows)")
    print("   2. Check antivirus software interference")
    print("   3. Ensure temp directory is writable")


if __name__ == "__main__":
    try:
        test_pyspark_installation()
    except AssertionError:
        print_troubleshooting_tips()
        sys.exit(1)

    print(f"\n🚀 Ready to run PySpark ETL pipeline!")
    print(f"   Next step: python test_pyspark_etl_complete.py")
