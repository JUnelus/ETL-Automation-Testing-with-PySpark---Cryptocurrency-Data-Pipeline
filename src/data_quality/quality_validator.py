import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
import logging
from datetime import datetime
from pathlib import Path
import json


class DataQualityValidator:
    """
    Comprehensive Data Quality Validation Framework
    This is the CORE component for ETL Quality Engineering roles
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.quality_results = []
        self.validation_rules = {}

    def validate_completeness(self, df: pd.DataFrame, critical_columns: List[str],
                              threshold: float = 0.95) -> Dict:
        """
        Validate data completeness against business rules

        Args:
            df: DataFrame to validate
            critical_columns: List of columns that must meet completeness threshold
            threshold: Minimum completeness percentage (0.95 = 95%)

        Returns:
            Dictionary with completeness validation results
        """
        self.logger.info(f"Running completeness validation with {threshold * 100}% threshold")

        total_records = len(df)
        completeness_results = {
            'validation_type': 'completeness',
            'total_records': total_records,
            'threshold_percentage': threshold * 100,
            'column_results': {},
            'overall_status': 'PASS',
            'validation_timestamp': datetime.now().isoformat()
        }

        failed_columns = []

        for column in critical_columns:
            if column in df.columns:
                non_null_count = df[column].count()  # pandas count() excludes NaN
                completeness_pct = non_null_count / total_records if total_records > 0 else 0

                column_status = 'PASS' if completeness_pct >= threshold else 'FAIL'

                if column_status == 'FAIL':
                    failed_columns.append(column)

                completeness_results['column_results'][column] = {
                    'completeness_percentage': round(completeness_pct * 100, 2),
                    'null_count': total_records - non_null_count,
                    'non_null_count': non_null_count,
                    'status': column_status,
                    'meets_threshold': completeness_pct >= threshold
                }
            else:
                # Column doesn't exist - critical failure
                completeness_results['column_results'][column] = {
                    'status': 'FAIL',
                    'error': 'Column not found in dataset',
                    'meets_threshold': False
                }
                failed_columns.append(column)

        if failed_columns:
            completeness_results['overall_status'] = 'FAIL'
            completeness_results['failed_columns'] = failed_columns

        self.logger.info(f"Completeness validation: {completeness_results['overall_status']} "
                         f"({len(failed_columns)} failed columns)")

        return completeness_results

    def validate_accuracy(self, df: pd.DataFrame, business_rules: Dict = None) -> Dict:
        """
        Validate data accuracy using business rules
        Essential for financial data validation

        Args:
            df: DataFrame to validate
            business_rules: Custom business rules dictionary

        Returns:
            Dictionary with accuracy validation results
        """
        self.logger.info("Running accuracy validation with business rules")

        # Default business rules for cryptocurrency data
        default_rules = {
            'positive_prices': {
                'condition': df.get('price_usd', pd.Series([1])) > 0,
                'description': 'All prices must be positive'
            },
            'positive_market_caps': {
                'condition': df.get('market_cap_usd', pd.Series([1])) > 0,
                'description': 'All market caps must be positive'
            },
            'valid_percentage_changes': {
                'condition': df.get('price_change_24h_pct', pd.Series([0])).between(-99.99, 10000),
                'description': 'Price changes must be within reasonable bounds (-99.99% to +10000%)'
            },
            'reasonable_volumes': {
                'condition': df.get('volume_24h_usd', pd.Series([1])) >= 0,
                'description': 'Trading volumes must be non-negative'
            },
            'valid_symbols': {
                'condition': df.get('coin_symbol', pd.Series(['BTC'])).str.len() <= 10,
                'description': 'Coin symbols must be 10 characters or less'
            }
        }

        # Use provided rules or defaults
        rules_to_check = business_rules if business_rules else default_rules

        accuracy_results = {
            'validation_type': 'accuracy',
            'total_records': len(df),
            'rules_checked': len(rules_to_check),
            'rule_results': {},
            'overall_status': 'PASS',
            'validation_timestamp': datetime.now().isoformat()
        }

        failed_rules = []

        for rule_name, rule_config in rules_to_check.items():
            try:
                # Apply business rule condition
                valid_mask = rule_config['condition']

                if isinstance(valid_mask, pd.Series):
                    invalid_count = (~valid_mask).sum()
                    valid_count = valid_mask.sum()
                else:
                    # Handle scalar results
                    invalid_count = 0 if valid_mask else len(df)
                    valid_count = len(df) if valid_mask else 0

                rule_status = 'PASS' if invalid_count == 0 else 'FAIL'

                if rule_status == 'FAIL':
                    failed_rules.append(rule_name)

                accuracy_results['rule_results'][rule_name] = {
                    'description': rule_config['description'],
                    'valid_records': int(valid_count),
                    'invalid_records': int(invalid_count),
                    'accuracy_percentage': round((valid_count / len(df)) * 100, 2) if len(df) > 0 else 100,
                    'status': rule_status
                }

            except Exception as e:
                self.logger.error(f"Error evaluating rule '{rule_name}': {e}")
                accuracy_results['rule_results'][rule_name] = {
                    'description': rule_config.get('description', 'Unknown rule'),
                    'status': 'ERROR',
                    'error': str(e)
                }
                failed_rules.append(rule_name)

        if failed_rules:
            accuracy_results['overall_status'] = 'FAIL'
            accuracy_results['failed_rules'] = failed_rules

        self.logger.info(f"Accuracy validation: {accuracy_results['overall_status']} "
                         f"({len(failed_rules)} failed rules)")

        return accuracy_results

    def validate_consistency(self, df: pd.DataFrame) -> Dict:
        """
        Validate data consistency and standardization
        Critical for ETL data quality
        """
        self.logger.info("Running consistency validation")

        consistency_results = {
            'validation_type': 'consistency',
            'total_records': len(df),
            'consistency_checks': {},
            'overall_status': 'PASS',
            'validation_timestamp': datetime.now().isoformat()
        }

        failed_checks = []

        # Check 1: Symbol standardization (uppercase)
        if 'coin_symbol' in df.columns:
            symbols_series = df['coin_symbol'].dropna()
            if len(symbols_series) > 0:
                non_uppercase_count = (~symbols_series.str.isupper()).sum()

                consistency_results['consistency_checks']['uppercase_symbols'] = {
                    'description': 'All coin symbols should be uppercase',
                    'compliant_records': len(symbols_series) - non_uppercase_count,
                    'non_compliant_records': non_uppercase_count,
                    'compliance_percentage': round(
                        ((len(symbols_series) - non_uppercase_count) / len(symbols_series)) * 100, 2),
                    'status': 'PASS' if non_uppercase_count == 0 else 'FAIL'
                }

                if non_uppercase_count > 0:
                    failed_checks.append('uppercase_symbols')

        # Check 2: ID format consistency (lowercase, no spaces)
        if 'coin_id' in df.columns:
            ids_series = df['coin_id'].dropna()
            if len(ids_series) > 0:
                invalid_id_count = (~ids_series.str.match(r'^[a-z0-9-]+$')).sum()

                consistency_results['consistency_checks']['valid_id_format'] = {
                    'description': 'Coin IDs should contain only lowercase letters, numbers, and hyphens',
                    'compliant_records': len(ids_series) - invalid_id_count,
                    'non_compliant_records': invalid_id_count,
                    'compliance_percentage': round(((len(ids_series) - invalid_id_count) / len(ids_series)) * 100, 2),
                    'status': 'PASS' if invalid_id_count == 0 else 'FAIL'
                }

                if invalid_id_count > 0:
                    failed_checks.append('valid_id_format')

        # Check 3: Ranking consistency (should be sequential integers starting from 1)
        ranking_columns = [col for col in df.columns if 'rank' in col.lower()]
        for rank_col in ranking_columns:
            if rank_col in df.columns:
                ranks = df[rank_col].dropna()
                if len(ranks) > 0:
                    expected_ranks = set(range(1, len(ranks) + 1))
                    actual_ranks = set(ranks.astype(int))

                    missing_ranks = expected_ranks - actual_ranks
                    unexpected_ranks = actual_ranks - expected_ranks

                    rank_status = 'PASS' if len(missing_ranks) == 0 and len(unexpected_ranks) == 0 else 'FAIL'

                    consistency_results['consistency_checks'][f'{rank_col}_sequential'] = {
                        'description': f'{rank_col} should contain sequential integers from 1 to {len(ranks)}',
                        'expected_count': len(ranks),
                        'actual_unique_count': len(actual_ranks),
                        'missing_ranks': sorted(list(missing_ranks)) if missing_ranks else [],
                        'unexpected_ranks': sorted(list(unexpected_ranks)) if unexpected_ranks else [],
                        'status': rank_status
                    }

                    if rank_status == 'FAIL':
                        failed_checks.append(f'{rank_col}_sequential')

        if failed_checks:
            consistency_results['overall_status'] = 'FAIL'
            consistency_results['failed_checks'] = failed_checks

        self.logger.info(f"Consistency validation: {consistency_results['overall_status']} "
                         f"({len(failed_checks)} failed checks)")

        return consistency_results

    def validate_uniqueness(self, df: pd.DataFrame, unique_columns: List[str]) -> Dict:
        """
        Validate uniqueness constraints
        Essential for primary key validation
        """
        self.logger.info(f"Running uniqueness validation on {len(unique_columns)} columns")

        uniqueness_results = {
            'validation_type': 'uniqueness',
            'total_records': len(df),
            'unique_column_results': {},
            'overall_status': 'PASS',
            'validation_timestamp': datetime.now().isoformat()
        }

        failed_columns = []

        for column in unique_columns:
            if column in df.columns:
                total_values = df[column].count()  # Excludes NaN
                unique_values = df[column].nunique()  # Count of unique non-NaN values
                duplicate_count = total_values - unique_values

                column_status = 'PASS' if duplicate_count == 0 else 'FAIL'

                uniqueness_results['unique_column_results'][column] = {
                    'total_non_null_values': total_values,
                    'unique_values': unique_values,
                    'duplicate_count': duplicate_count,
                    'uniqueness_percentage': round((unique_values / total_values) * 100,
                                                   2) if total_values > 0 else 100,
                    'status': column_status
                }

                if column_status == 'FAIL':
                    failed_columns.append(column)
                    # Get sample duplicates for debugging
                    duplicates = df[df.duplicated(subset=[column], keep=False)][column].value_counts()
                    uniqueness_results['unique_column_results'][column][
                        'sample_duplicates'] = duplicates.head().to_dict()

            else:
                uniqueness_results['unique_column_results'][column] = {
                    'status': 'ERROR',
                    'error': 'Column not found in dataset'
                }
                failed_columns.append(column)

        if failed_columns:
            uniqueness_results['overall_status'] = 'FAIL'
            uniqueness_results['failed_columns'] = failed_columns

        self.logger.info(f"Uniqueness validation: {uniqueness_results['overall_status']} "
                         f"({len(failed_columns)} failed columns)")

        return uniqueness_results

    def generate_comprehensive_quality_report(self, df: pd.DataFrame,
                                              config: Dict = None) -> Dict:
        """
        Generate comprehensive data quality report
        This is the main function used by ETL pipelines

        Args:
            df: DataFrame to validate
            config: Configuration dictionary with validation parameters

        Returns:
            Complete data quality report
        """
        self.logger.info("Generating comprehensive data quality report")

        # Default configuration
        default_config = {
            'critical_columns': ['coin_id', 'coin_symbol', 'coin_name', 'price_usd', 'market_cap_usd'],
            'completeness_threshold': 0.95,
            'unique_columns': ['coin_id'],
            'business_rules': None,  # Use default rules
            'generate_summary': True
        }

        # Merge with provided config
        validation_config = {**default_config, **(config or {})}

        # Run all validations
        report = {
            'report_metadata': {
                'dataset_shape': df.shape,
                'validation_timestamp': datetime.now().isoformat(),
                'validation_config': validation_config
            },
            'validations': {}
        }

        # 1. Completeness validation
        report['validations']['completeness'] = self.validate_completeness(
            df,
            validation_config['critical_columns'],
            validation_config['completeness_threshold']
        )

        # 2. Accuracy validation
        report['validations']['accuracy'] = self.validate_accuracy(
            df,
            validation_config['business_rules']
        )

        # 3. Consistency validation
        report['validations']['consistency'] = self.validate_consistency(df)

        # 4. Uniqueness validation
        report['validations']['uniqueness'] = self.validate_uniqueness(
            df,
            validation_config['unique_columns']
        )

        # Calculate overall quality score
        validation_statuses = [v['overall_status'] for v in report['validations'].values()]
        passed_validations = sum(1 for status in validation_statuses if status == 'PASS')
        total_validations = len(validation_statuses)

        overall_quality_score = (passed_validations / total_validations) * 100 if total_validations > 0 else 0

        report['overall_quality_assessment'] = {
            'overall_status': 'PASS' if passed_validations == total_validations else 'FAIL',
            'quality_score_percentage': round(overall_quality_score, 2),
            'validations_passed': passed_validations,
            'total_validations': total_validations,
            'recommendation': self._get_quality_recommendation(overall_quality_score)
        }

        self.logger.info(f"Quality report generated: {overall_quality_score:.1f}% quality score")

        return report

    def _get_quality_recommendation(self, quality_score: float) -> str:
        """Provide quality score based recommendations"""
        if quality_score >= 95:
            return "EXCELLENT: Data quality meets production standards"
        elif quality_score >= 90:
            return "GOOD: Minor quality issues, safe for production with monitoring"
        elif quality_score >= 80:
            return "ACCEPTABLE: Some quality issues, review failed validations"
        elif quality_score >= 70:
            return "POOR: Significant quality issues, data cleansing required"
        else:
            return "CRITICAL: Major quality issues, do not use in production"

    def save_quality_report(self, report: Dict, output_path: str) -> None:
        """Save quality report to file"""
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, default=str)

            self.logger.info(f"Quality report saved to: {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to save quality report: {e}")
            raise
