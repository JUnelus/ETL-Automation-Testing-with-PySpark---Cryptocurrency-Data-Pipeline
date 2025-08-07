from src.data_quality.quality_validator import DataQualityValidator
import pandas as pd
from datetime import datetime
from pathlib import Path

def test_quality_framework():
    """
    Standalone test to demonstrate data quality framework
    Run this to see the validation framework in action
    """
    print("🔍 Testing Data Quality Framework...")

    try:
        import logging
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

        # Initialize validator
        validator = DataQualityValidator()

        # Create test data with known quality issues
        test_data = pd.DataFrame({
            'coin_id': ['bitcoin', 'ethereum', 'bitcoin', 'cardano'],  # Duplicate
            'coin_symbol': ['BTC', 'eth', 'BTC', 'ADA'],  # Lowercase issue
            'coin_name': ['Bitcoin', 'Ethereum', 'Bitcoin', 'Cardano'],
            'price_usd': [45000.0, 3000.0, 45000.0, None],  # Missing value
            'market_cap_usd': [900000000000, 350000000000, 900000000000, 15000000000],
            'volume_24h_usd': [25000000000, 15000000000, 25000000000, -1000000],  # Negative volume
            'price_change_24h_pct': [2.5, -1.8, 2.5, 8.2],
        })

        print(f"📊 Test Dataset: {len(test_data)} records")
        print("\n🧪 Running Comprehensive Quality Validation...")

        # Generate comprehensive report
        quality_report = validator.generate_comprehensive_quality_report(test_data)

        # Display results
        print("\n" + "=" * 60)
        print("📋 DATA QUALITY REPORT SUMMARY")
        print("=" * 60)

        overall = quality_report['overall_quality_assessment']
        print(f"Overall Status: {overall['overall_status']}")
        print(f"Quality Score: {overall['quality_score_percentage']}%")
        print(f"Validations Passed: {overall['validations_passed']}/{overall['total_validations']}")
        print(f"Recommendation: {overall['recommendation']}")

        print(f"\n🔍 DETAILED VALIDATION RESULTS:")
        print("-" * 40)

        for validation_type, results in quality_report['validations'].items():
            print(f"{validation_type.upper()}: {results['overall_status']}")

            if results['overall_status'] == 'FAIL':
                if 'failed_columns' in results:
                    print(f"  ❌ Failed columns: {results['failed_columns']}")
                if 'failed_rules' in results:
                    print(f"  ❌ Failed rules: {results['failed_rules']}")
                if 'failed_checks' in results:
                    print(f"  ❌ Failed checks: {results['failed_checks']}")

        # Save report
        output_dir = Path("data") / "quality_reports"
        output_dir.mkdir(parents=True, exist_ok=True)
        report_file = output_dir / f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        validator.save_quality_report(quality_report, report_file)

        print(f"\n💾 Quality report saved to: {report_file}")
        print(f"\n🎉 Data Quality Framework Test Complete!")

        return True

    except Exception as e:
        print(f"❌ Error during quality framework testing: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_quality_framework()