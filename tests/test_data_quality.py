import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.data_quality.quality_validator import DataQualityValidator


class TestDataQualityValidator:
    """
    Comprehensive test suite for data quality validation
    This demonstrates professional testing skills for ETL QA roles
    """

    @pytest.fixture
    def validator(self):
        """Create validator instance"""
        return DataQualityValidator()

    @pytest.fixture
    def sample_crypto_data(self):
        """Create sample cryptocurrency data for testing"""
        return pd.DataFrame({
            'coin_id': ['bitcoin', 'ethereum', 'cardano', 'solana', 'duplicate-id'],
            'coin_symbol': ['BTC', 'ETH', 'ada', 'SOL', 'DUP'],  # One lowercase symbol
            'coin_name': ['Bitcoin', 'Ethereum', 'Cardano', 'Solana', 'Duplicate'],
            'price_usd': [45000.0, 3000.0, 0.45, 150.0, -10.0],  # One negative price
            'market_cap_usd': [900000000000, 350000000000, 15000000000, 50000000000, 100000000],
            'volume_24h_usd': [25000000000, 15000000000, 800000000, 2000000000, 50000000],
            'price_change_24h_pct': [2.5, -1.8, 8.2, 5.1, None],  # One null value
            'market_rank': [1, 2, 8, 5, 100]
        })

    @pytest.fixture
    def perfect_crypto_data(self):
        """Create perfect quality cryptocurrency data"""
        return pd.DataFrame({
            'coin_id': ['bitcoin', 'ethereum', 'cardano'],
            'coin_symbol': ['BTC', 'ETH', 'ADA'],
            'coin_name': ['Bitcoin', 'Ethereum', 'Cardano'],
            'price_usd': [45000.0, 3000.0, 0.45],
            'market_cap_usd': [900000000000, 350000000000, 15000000000],
            'volume_24h_usd': [25000000000, 15000000000, 800000000],
            'price_change_24h_pct': [2.5, -1.8, 8.2],
            'market_rank': [1, 2, 3]
        })

    def test_completeness_validation_pass(self, validator, perfect_crypto_data):
        """Test completeness validation with high-quality data"""
        critical_columns = ['coin_id', 'coin_symbol', 'price_usd']
        result = validator.validate_completeness(perfect_crypto_data, critical_columns)

        assert result['overall_status'] == 'PASS'
        assert result['total_records'] == 3

        for column in critical_columns:
            assert result['column_results'][column]['status'] == 'PASS'
            assert result['column_results'][column]['completeness_percentage'] == 100.0

    def test_completeness_validation_fail(self, validator, sample_crypto_data):
        """Test completeness validation with missing data"""
        critical_columns = ['coin_id', 'price_change_24h_pct']  # Has null value
        result = validator.validate_completeness(sample_crypto_data, critical_columns, threshold=1.0)

        assert result['overall_status'] == 'FAIL'
        assert 'price_change_24h_pct' in result['failed_columns']
        assert result['column_results']['price_change_24h_pct']['null_count'] == 1

    def test_accuracy_validation_business_rules(self, validator, sample_crypto_data):
        """Test accuracy validation with business rules"""
        result = validator.validate_accuracy(sample_crypto_data)

        # Should fail due to negative price
        assert result['overall_status'] == 'FAIL'
        assert 'positive_prices' in result['failed_rules']
        assert result['rule_results']['positive_prices']['invalid_records'] == 1

    def test_consistency_validation(self, validator, sample_crypto_data):
        """Test consistency validation"""
        result = validator.validate_consistency(sample_crypto_data)

        # Should fail due to lowercase symbol
        assert result['overall_status'] == 'FAIL'
        assert 'uppercase_symbols' in result['failed_checks']
        assert result['consistency_checks']['uppercase_symbols']['non_compliant_records'] == 1

    def test_uniqueness_validation(self, validator):
        """Test uniqueness validation with duplicate data"""
        duplicate_data = pd.DataFrame({
            'coin_id': ['bitcoin', 'ethereum', 'bitcoin'],  # Duplicate bitcoin
            'coin_symbol': ['BTC', 'ETH', 'BTC'],
            'price_usd': [45000.0, 3000.0, 45000.0]
        })

        result = validator.validate_uniqueness(duplicate_data, ['coin_id'])

        assert result['overall_status'] == 'FAIL'
        assert 'coin_id' in result['failed_columns']
        assert result['unique_column_results']['coin_id']['duplicate_count'] == 1

    def test_comprehensive_quality_report(self, validator, perfect_crypto_data):
        """Test comprehensive quality report generation"""
        report = validator.generate_comprehensive_quality_report(perfect_crypto_data)

        # Should pass all validations
        assert report['overall_quality_assessment']['overall_status'] == 'PASS'
        assert report['overall_quality_assessment']['quality_score_percentage'] == 100.0
        assert 'EXCELLENT' in report['overall_quality_assessment']['recommendation']

        # Check all validation types are present
        expected_validations = ['completeness', 'accuracy', 'consistency', 'uniqueness']
        for validation_type in expected_validations:
            assert validation_type in report['validations']
            assert report['validations'][validation_type]['overall_status'] == 'PASS'

    def test_custom_business_rules(self, validator, sample_crypto_data):
        """Test custom business rules"""
        # Look at the sample data - one coin has market_cap_usd = 100000000 (100M, which is < 1B)
        # So the test should expect this rule to FAIL for that coin

        custom_rules = {
            'market_cap_over_1b': {
                'condition': sample_crypto_data['market_cap_usd'] > 1e9,
                'description': 'Market cap should be over 1 billion'
            }
        }

        result = validator.validate_accuracy(sample_crypto_data, custom_rules)

        # The rule should FAIL because one coin (duplicate-id) has market cap of 100M < 1B
        assert result['rule_results']['market_cap_over_1b']['status'] == 'FAIL'
        assert result['rule_results']['market_cap_over_1b']['invalid_records'] == 1  # One coin fails
        assert result['rule_results']['market_cap_over_1b']['valid_records'] == 4  # Four coins pass

        # Test a rule that should pass
        custom_rules_pass = {
            'market_cap_over_10m': {
                'condition': sample_crypto_data['market_cap_usd'] > 10e6,  # 10 million
                'description': 'Market cap should be over 10 million'
            }
        }

        result_pass = validator.validate_accuracy(sample_crypto_data, custom_rules_pass)
        assert result_pass['rule_results']['market_cap_over_10m']['status'] == 'PASS'

    def test_quality_score_calculation(self, validator, sample_crypto_data):
        """Test quality score calculation with mixed results"""
        report = validator.generate_comprehensive_quality_report(sample_crypto_data)

        # Should have some failed validations
        assert report['overall_quality_assessment']['overall_status'] == 'FAIL'
        assert report['overall_quality_assessment']['quality_score_percentage'] < 100
        assert report['overall_quality_assessment']['validations_passed'] < report['overall_quality_assessment'][
            'total_validations']
