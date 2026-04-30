import os
import subprocess
import sys

from src.utils.spark_session import create_spark_session


def check_java_version():
    """Check current Java version and compatibility"""
    print("🔍 Checking Java Version Compatibility...")

    try:
        # Get Java version
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            java_output = result.stderr if result.stderr else result.stdout
            print(f"✅ Java found:")
            print(f"   {java_output.split()[0:3]}")

            # Check if Java 21
            if "21.0" in java_output:
                print("⚠️  Java 21 detected - may need compatibility adjustments")
                return "java21"
            elif "11.0" in java_output:
                print("✅ Java 11 detected - optimal for PySpark")
                return "java11"
            elif "1.8" in java_output or "8.0" in java_output:
                print("✅ Java 8 detected - compatible with PySpark")
                return "java8"
            else:
                print("⚠️  Unknown Java version - may have compatibility issues")
                return "unknown"
        else:
            print("❌ Java not found in PATH")
            return "not_found"

    except Exception as e:
        print(f"❌ Error checking Java version: {e}")
        return "error"


def test_pyspark_with_java21():
    """Test PySpark with Java 21 and provide workarounds"""
    print("\n🧪 Testing PySpark with Java 21...")

    spark = None
    try:
        # Try importing PySpark
        import pyspark
        print(f"✅ PySpark imported successfully (version: {pyspark.__version__})")

        # Use project spark-session helper so Python worker config is consistent.
        spark = create_spark_session("Java21-Compatibility-Test")

        print("✅ Spark session created successfully with Java 21")
        print(f"   Spark version: {spark.version}")

        # Test basic operations
        test_data = [("Test", 1), ("Data", 2)]
        df = spark.createDataFrame(test_data, ["name", "value"])
        count = df.count()

        print(f"✅ Basic DataFrame operations successful ({count} rows)")

        spark.stop()
        print("✅ Java 21 compatibility test PASSED")
        assert True

    except Exception as e:
        print(f"❌ Java 21 compatibility test FAILED: {e}")
        raise AssertionError(f"Java 21 compatibility test failed: {e}") from e
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass


def provide_java11_installation_guide():
    """Provide step-by-step Java 11 installation guide"""
    print("\n📝 JAVA 11 INSTALLATION GUIDE:")
    print("=" * 50)

    print("\n1. Download Java 11:")
    print("   🔗 Go to: https://adoptium.net/temurin/releases/")
    print("   📦 Select: OpenJDK 11 (LTS)")
    print("   💻 Platform: Windows x64")
    print("   📄 Package Type: MSI")

    print("\n2. Install Java 11:")
    print("   ▶️  Run the downloaded MSI file")
    print("   ✅ Accept default installation settings")
    print("   📁 Note installation path (usually: C:\\Program Files\\Eclipse Adoptium\\jdk-11.x.x-hotspot\\)")

    print("\n3. Set Environment Variables:")
    print("   🔧 Press Win + R, type 'sysdm.cpl', press Enter")
    print("   🔧 Click 'Environment Variables' button")
    print("   🔧 Under 'System Variables', click 'New'")
    print("   ")
    print("   📝 Variable name: JAVA_HOME")
    print("   📝 Variable value: C:\\Program Files\\Eclipse Adoptium\\jdk-11.x.x-hotspot")
    print("   ")
    print("   🔧 Edit PATH variable, add: %JAVA_HOME%\\bin")

    print("\n4. Verify Installation:")
    print("   💻 Open NEW terminal")
    print("   ▶️  Run: java -version")
    print("   ✅ Should show Java 11.x.x")


def provide_current_java_workaround():
    """Provide workaround for current Java 21 setup"""
    print("\n🔧 JAVA 21 WORKAROUND FOR PYSPARK:")
    print("=" * 50)

    print("\n1. Update your spark_session.py with Java 21 compatibility:")

    spark_session_fix = '''
def create_spark_session(app_name: str = "CryptoETL", local_mode: bool = True) -> SparkSession:
    # Java 21 compatibility settings
    os.environ["SPARK_SUBMIT_OPTS"] = "--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED"

    conf = SparkConf()
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # Java 21 specific configurations
    conf.set("spark.driver.extraJavaOptions", 
             "--add-opens java.base/sun.nio.ch=ALL-UNNAMED " +
             "--add-opens java.base/java.io=ALL-UNNAMED " +
             "--add-opens java.base/java.util=ALL-UNNAMED")

    if local_mode:
        conf.set("spark.master", "local[1]")  # Single core for stability
        conf.set("spark.driver.memory", "1g")  # Reduced memory
        conf.set("spark.sql.shuffle.partitions", "2")

    spark = SparkSession.builder \\
        .appName(app_name) \\
        .config(conf=conf) \\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark
'''

    print("2. Use this updated function in your src/utils/spark_session.py")
    print("\n3. Set environment variable before running:")
    print(
        '   set SPARK_SUBMIT_OPTS=--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED')


def main():
    """Main compatibility check and guidance"""
    print("☕ JAVA PYSPARK COMPATIBILITY CHECKER")
    print("=" * 50)

    # Check Java version
    java_status = check_java_version()

    if java_status == "java21":
        # Test Java 21 compatibility
        try:
            test_pyspark_with_java21()
            print("\n🎉 GOOD NEWS: Java 21 works with PySpark!")
            print("✅ You can proceed with your current setup")
            provide_current_java_workaround()
        except AssertionError:
            print("\n⚠️  Java 21 has compatibility issues")
            provide_java11_installation_guide()

    elif java_status in ["java11", "java8"]:
        print(f"\n✅ Perfect! Your Java version is optimal for PySpark")
        print("🚀 Proceed with: python test_pyspark_setup.py")

    elif java_status == "not_found":
        print("\n❌ Java not found. Please install Java first:")
        provide_java11_installation_guide()

    else:
        print(f"\n⚠️  Unknown Java version detected")
        print("💡 Recommend installing Java 11 for best compatibility:")
        provide_java11_installation_guide()


if __name__ == "__main__":
    main()