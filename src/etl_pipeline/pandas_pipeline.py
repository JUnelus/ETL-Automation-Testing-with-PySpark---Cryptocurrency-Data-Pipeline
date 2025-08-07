# Pandas-based ETL alternative that demonstrates the same concepts without Java

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import glob
import json


class PandasETLPipeline:
    """
    Pandas-based ETL pipeline that demonstrates the same concepts as PySpark
    Perfect for showcasing ETL skills without Java dependency
    """

    def __init__(self, app_name: str = "PandasETL"):
        self.app_name = app_name
        self.logger = logging.getLogger(__name__)

        # Setup directories
        self.base_dir = Path(__file__).parent.parent.parent
        self.data_dir = self.base_dir / "data"
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"

        # Create directories if they don't exist
        self.processed_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Initialized {app_name} ETL Pipeline")

    def get_latest_raw_file(self, pattern: str = "*market_data*.csv") -> str:
        """Get the most recent raw data file"""
        files = glob.glob(str(self.raw_dir / pattern))
        if not files:
            raise FileNotFoundError(f"No files found matching pattern: {pattern}")

        latest_file = max(files, key=lambda f: Path(f).stat().st_ctime)
        self.logger.info(f"Using latest raw file: {latest_file}")
        return latest_file

    def load_raw_data(self, file_path: str) -> pd.DataFrame:
        """Load raw CSV data with error handling"""
        try:
            df = pd.read_csv(file_path)
            self.logger.info(f"Loaded {len(df)} records from {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load data from {file_path}: {e}")
            raise

    def clean_market_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize cryptocurrency market data
        Demonstrates data quality and transformation skills
        """
        self.logger.info("Starting data cleaning transformation")

        # Create cleaned dataframe with proper column mapping
        cleaned_df = df.copy()

        # Rename columns to standardized format
        column_mapping = {
            'id': 'coin_id',
            'symbol': 'coin_symbol',
            'name': 'coin_name',
            'current_price': 'price_usd',
            'market_cap': 'market_cap_usd',
            'market_cap_rank': 'market_rank',
            'total_volume': 'volume_24h_usd',
            'price_change_percentage_24h': 'price_change_24h_pct',
            'price_change_percentage_7d': 'price_change_7d_pct',
            'price_change_percentage_30d': 'price_change_30d_pct'
        }

        # Rename columns that exist
        existing_columns = {k: v for k, v in column_mapping.items() if k in cleaned_df.columns}
        cleaned_df = cleaned_df.rename(columns=existing_columns)

        # Convert data types
        numeric_columns = ['price_usd', 'market_cap_usd', 'volume_24h_usd',
                           'price_change_24h_pct', 'price_change_7d_pct', 'price_change_30d_pct']

        for col in numeric_columns:
            if col in cleaned_df.columns:
                cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce')

        # Add timestamp columns
        cleaned_df['extraction_timestamp'] = pd.Timestamp.now()
        cleaned_df['extraction_date'] = pd.Timestamp.now().date()

        # Data quality filters
        initial_count = len(cleaned_df)
        cleaned_df = cleaned_df.dropna(subset=['price_usd', 'market_cap_usd'])
        cleaned_df = cleaned_df[
            (cleaned_df['price_usd'] > 0) &
            (cleaned_df['market_cap_usd'] > 0)
            ]

        # Standardize symbols to uppercase
        if 'coin_symbol' in cleaned_df.columns:
            cleaned_df['coin_symbol'] = cleaned_df['coin_symbol'].str.upper()

        final_count = len(cleaned_df)
        self.logger.info(f"Data cleaning completed: {final_count}/{initial_count} records retained")

        return cleaned_df

    def calculate_advanced_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate advanced financial metrics using Pandas
        Demonstrates complex analytical transformations
        """
        self.logger.info("Calculating advanced market metrics")

        metrics_df = df.copy()

        # Volume to Market Cap Ratio
        if 'volume_24h_usd' in metrics_df.columns and 'market_cap_usd' in metrics_df.columns:
            metrics_df['volume_to_market_cap_ratio'] = (
                    metrics_df['volume_24h_usd'] / metrics_df['market_cap_usd']
            ).round(6)

        # Price Volatility Score
        volatility_columns = ['price_change_24h_pct', 'price_change_7d_pct', 'price_change_30d_pct']
        available_vol_cols = [col for col in volatility_columns if col in metrics_df.columns]

        if available_vol_cols:
            volatility_data = metrics_df[available_vol_cols].fillna(0)
            metrics_df['volatility_score'] = np.sqrt(
                (volatility_data ** 2).sum(axis=1) / len(available_vol_cols)
            ).round(2)

        # Market Cap Categories
        if 'market_cap_usd' in metrics_df.columns:
            metrics_df['market_cap_category'] = pd.cut(
                metrics_df['market_cap_usd'],
                bins=[0, 100e6, 1e9, 10e9, 100e9, float('inf')],
                labels=['Micro Cap', 'Small Cap', 'Mid Cap', 'Large Cap', 'Mega Cap'],
                include_lowest=True
            )

        # Volatility Categories
        if 'volatility_score' in metrics_df.columns:
            metrics_df['volatility_category'] = pd.cut(
                metrics_df['volatility_score'],
                bins=[0, 2, 5, 10, 15, float('inf')],
                labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'],
                include_lowest=True
            )

        # Trading Activity Level
        if 'volume_to_market_cap_ratio' in metrics_df.columns:
            conditions = [
                metrics_df['volume_to_market_cap_ratio'] >= 0.5,
                metrics_df['volume_to_market_cap_ratio'] >= 0.1,
                metrics_df['volume_to_market_cap_ratio'] >= 0.05
            ]
            choices = ['Very Active', 'Active', 'Moderate']
            metrics_df['trading_activity'] = np.select(conditions, choices, default='Low Activity')

        # Rankings
        if 'market_cap_usd' in metrics_df.columns:
            metrics_df['calculated_rank'] = metrics_df['market_cap_usd'].rank(ascending=False, method='dense')

        self.logger.info("Advanced metrics calculation completed")
        return metrics_df

    def create_market_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create comprehensive market summary using pandas aggregations
        Demonstrates complex aggregation skills
        """
        self.logger.info("Creating market summary aggregations")

        group_columns = []
        if 'market_cap_category' in df.columns:
            group_columns.append('market_cap_category')
        if 'volatility_category' in df.columns:
            group_columns.append('volatility_category')

        if not group_columns:
            # Fallback: create simple summary
            summary_df = pd.DataFrame({
                'total_coins': [len(df)],
                'avg_price_usd': [df.get('price_usd', pd.Series([0])).mean()],
                'total_market_cap_billions': [df.get('market_cap_usd', pd.Series([0])).sum() / 1e9]
            })
        else:
            # Group by categories
            agg_dict = {
                'coin_id': 'count'
            }

            # Add aggregations for available columns
            if 'price_usd' in df.columns:
                agg_dict['price_usd'] = ['mean', 'min', 'max']
            if 'market_cap_usd' in df.columns:
                agg_dict['market_cap_usd'] = 'sum'
            if 'volume_24h_usd' in df.columns:
                agg_dict['volume_24h_usd'] = 'sum'
            if 'volume_to_market_cap_ratio' in df.columns:
                agg_dict['volume_to_market_cap_ratio'] = 'mean'
            if 'volatility_score' in df.columns:
                agg_dict['volatility_score'] = 'mean'
            if 'price_change_24h_pct' in df.columns:
                agg_dict['price_change_24h_pct'] = 'mean'
            if 'coin_symbol' in df.columns:
                agg_dict['coin_symbol'] = lambda x: list(x)

            summary_df = df.groupby(group_columns).agg(agg_dict).round(2)

            # Flatten column names
            summary_df.columns = [
                f"{col[1]}_{col[0]}" if isinstance(col, tuple) and col[1] else col[0]
                for col in summary_df.columns
            ]

            # Rename for clarity
            rename_dict = {
                'count_coin_id': 'coin_count',
                'mean_price_usd': 'avg_price_usd',
                'sum_market_cap_usd': 'total_market_cap_usd',
                'sum_volume_24h_usd': 'total_volume_usd',
                'mean_volume_to_market_cap_ratio': 'avg_volume_ratio',
                'mean_volatility_score': 'avg_volatility_score',
                'mean_price_change_24h_pct': 'avg_24h_change_pct',
                '<lambda>_coin_symbol': 'coins_in_category'
            }

            summary_df = summary_df.rename(columns={k: v for k, v in rename_dict.items()
                                                    if k in summary_df.columns})

            # Convert market cap to billions
            if 'total_market_cap_usd' in summary_df.columns:
                summary_df['total_market_cap_billions'] = (
                        summary_df['total_market_cap_usd'] / 1e9
                ).round(2)

            summary_df = summary_df.reset_index()

        self.logger.info("Market summary creation completed")
        return summary_df

    def create_performance_rankings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create performance rankings across different timeframes
        Demonstrates ranking and window function equivalent logic
        """
        self.logger.info("Creating performance rankings")

        rankings_df = df.copy()

        # Create rankings for different metrics
        ranking_columns = {
            'price_change_24h_pct': 'rank_24h',
            'price_change_7d_pct': 'rank_7d',
            'price_change_30d_pct': 'rank_30d',
            'market_cap_usd': 'rank_market_cap',
            'volume_24h_usd': 'rank_volume'
        }

        for source_col, rank_col in ranking_columns.items():
            if source_col in rankings_df.columns:
                rankings_df[rank_col] = rankings_df[source_col].rank(
                    ascending=False, method='dense'
                ).astype(int)

        # Calculate composite performance score
        performance_columns = ['price_change_24h_pct', 'price_change_7d_pct', 'price_change_30d_pct']
        available_perf_cols = [col for col in performance_columns if col in rankings_df.columns]

        if available_perf_cols:
            weights = [0.4, 0.35, 0.25][:len(available_perf_cols)]  # Adjust weights based on available columns
            weights = np.array(weights) / sum(weights)  # Normalize weights

            weighted_performance = sum(
                rankings_df[col].fillna(0) * weight
                for col, weight in zip(available_perf_cols, weights)
            )

            rankings_df['composite_performance_score'] = weighted_performance.round(2)
            rankings_df['composite_rank'] = rankings_df['composite_performance_score'].rank(
                ascending=False, method='dense'
            ).astype(int)

        self.logger.info("Performance rankings completed")
        return rankings_df

    def save_processed_data(self, df: pd.DataFrame, output_path: str, format_type: str = "parquet") -> None:
        """
        Save processed data with multiple format support
        """
        try:
            output_path = Path(output_path)

            if format_type.lower() == "parquet":
                df.to_parquet(output_path, index=False, compression='snappy')
            elif format_type.lower() == "csv":
                df.to_csv(output_path, index=False)
            elif format_type.lower() == "json":
                df.to_json(output_path, orient='records', indent=2)
            else:
                raise ValueError(f"Unsupported output format: {format_type}")

            self.logger.info(f"Successfully saved {len(df)} records to {output_path} as {format_type}")

        except Exception as e:
            self.logger.error(f"Failed to save data: {e}")
            raise

    def run_full_pipeline(self, input_file: str = None) -> Dict:
        """
        Execute the complete ETL pipeline using Pandas
        """
        pipeline_start = datetime.now()
        self.logger.info("🚀 Starting Pandas ETL Pipeline Execution")

        try:
            # Step 1: Load raw data
            if input_file is None:
                input_file = self.get_latest_raw_file()

            self.logger.info(f"Loading data from: {input_file}")
            raw_df = self.load_raw_data(input_file)

            # Step 2: Clean and standardize data
            self.logger.info("Cleaning and standardizing data")
            cleaned_df = self.clean_market_data(raw_df)

            # Step 3: Calculate advanced metrics
            self.logger.info("Calculating advanced metrics")
            metrics_df = self.calculate_advanced_metrics(cleaned_df)

            # Step 4: Create market summary
            self.logger.info("Creating market summary")
            summary_df = self.create_market_summary(metrics_df)

            # Step 5: Create performance rankings
            self.logger.info("Creating performance rankings")
            rankings_df = self.create_performance_rankings(metrics_df)

            # Step 6: Save processed data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Save main processed dataset
            main_output = self.processed_dir / f"crypto_processed_pandas_{timestamp}.parquet"
            self.save_processed_data(metrics_df, str(main_output), "parquet")

            # Save summary data
            summary_output = self.processed_dir / f"market_summary_pandas_{timestamp}.csv"
            self.save_processed_data(summary_df, str(summary_output), "csv")

            # Save rankings
            rankings_output = self.processed_dir / f"performance_rankings_pandas_{timestamp}.json"
            self.save_processed_data(rankings_df, str(rankings_output), "json")

            # Pipeline completion
            pipeline_end = datetime.now()
            execution_time = (pipeline_end - pipeline_start).total_seconds()

            results = {
                'status': 'SUCCESS',
                'execution_time_seconds': execution_time,
                'input_file': input_file,
                'records_processed': len(cleaned_df),
                'output_files': {
                    'main_dataset': str(main_output),
                    'market_summary': str(summary_output),
                    'performance_rankings': str(rankings_output)
                },
                'pipeline_timestamp': pipeline_end.isoformat()
            }

            self.logger.info(f"✅ ETL Pipeline completed successfully in {execution_time:.2f} seconds")
            self.logger.info(f"📊 Processed {len(cleaned_df)} records")

            # Display sample results
            self._display_sample_results(summary_df, rankings_df)

            return results

        except Exception as e:
            self.logger.error(f"❌ Pipeline execution failed: {e}")
            return {
                'status': 'FAILED',
                'error': str(e),
                'execution_time_seconds': (datetime.now() - pipeline_start).total_seconds()
            }

    def _display_sample_results(self, summary_df, rankings_df):
        """Display sample results for verification"""
        print("\n" + "=" * 60)
        print("📊 PANDAS ETL PIPELINE RESULTS PREVIEW")
        print("=" * 60)

        # Top performers
        if 'rank_24h' in rankings_df.columns:
            print("\n🏆 TOP 5 PERFORMERS (24H):")
            top_performers = rankings_df.nsmallest(5, 'rank_24h')
            display_columns = []
            for col in ['coin_name', 'coin_symbol', 'price_usd', 'price_change_24h_pct']:
                if col in top_performers.columns:
                    display_columns.append(col)

            if display_columns:
                print(top_performers[display_columns].to_string(index=False))

        print(f"\n📈 MARKET SUMMARY:")
        print(summary_df.to_string(index=False))
