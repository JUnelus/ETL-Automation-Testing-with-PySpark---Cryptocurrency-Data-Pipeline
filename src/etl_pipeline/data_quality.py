from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import Dict, List, Tuple
import logging
from datetime import datetime


class DataQualityValidator:
    """
    Comprehensive data quality validation framework
    Essential for ETL Quality Engineering role
    """

    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
        self.quality_results = []

    def validate_completeness(self, df: DataFrame, critical_columns: List[str]) -> Dict:
        """Validate data completeness"""
        total_records = df.count()
        completeness_results = {}

        for column in critical_columns:
            non_null_count = df.filter(col(column).isNotNull()).count()
            completeness_pct = (non_null_count / total_records) * 100

            completeness_results[column] = {
                'completeness_percentage': completeness_pct,
                'null_count': total_records - non_null_count,
                'status': 'PASS' if completeness_pct >= 95 else 'FAIL'
            }

        return completeness_results

    def validate_accuracy(self, df: DataFrame) -> Dict:
        """Validate data accuracy with business rules"""
        accuracy_tests = {}

        # Price validation
        invalid_prices = df.filter(col("price_usd") <= 0).count()
        accuracy_tests['positive_prices'] = {
            'invalid_count': invalid_prices,
            'status': 'PASS' if invalid_prices == 0 else 'FAIL'
        }

        # Market cap validation
        invalid_market_caps = df.filter(col("market_cap_usd") <= 0).count()
        accuracy_tests['positive_market_caps'] = {
            'invalid_count': invalid_market_caps,
            'status': 'PASS' if invalid_market_caps == 0 else 'FAIL'
        }

        # Percentage change validation (-100% to +∞ is valid range)
        invalid_changes = df.filter(col("price_change_24h_pct") < -100).count()
        accuracy_tests['valid_percentage_changes'] = {
            'invalid_count': invalid_changes,
            'status': 'PASS' if invalid_changes == 0 else 'FAIL'
        }

        return accuracy_tests

    def validate_consistency(self, df: DataFrame) -> Dict:
        """Validate data consistency"""
        consistency_tests = {}

        # Symbol consistency (should be uppercase)
        inconsistent_symbols = df.filter(
            col("coin_symbol") != upper(col("coin_symbol"))
        ).count()

        consistency_tests['uppercase_symbols'] = {
            'inconsistent_count': inconsistent_symbols,
            'status': 'PASS' if inconsistent_symbols == 0 else 'FAIL'
        }

        return consistency_tests

    def generate_quality_report(self, df: DataFrame) -> Dict:
        """Generate comprehensive data quality report"""
        critical_columns = ['coin_id', 'price_usd', 'market_cap_usd', 'coin_symbol']

        report = {
            'timestamp': datetime.now().isoformat(),
            'total_records': df.count(),
            'completeness': self.validate_completeness(df, critical_columns),
            'accuracy': self.validate_accuracy(df),
            'consistency': self.validate_consistency(df)
        }

        # Calculate overall quality score
        all_tests = []
        for category in ['completeness', 'accuracy', 'consistency']:
            for test_name, results in report[category].items():
                all_tests.append(1 if results['status'] == 'PASS' else 0)

        report['overall_quality_score'] = sum(all_tests) / len(all_tests) * 100

        return report