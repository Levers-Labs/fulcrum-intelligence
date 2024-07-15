import pandas as pd
import numpy as np
import logging
from hierarchicalforecast.utils import aggregate
from statsforecast import StatsForecast
from statsforecast.models import Naive, AutoARIMA
from hierarchicalforecast.methods import BottomUp
from hierarchicalforecast.core import HierarchicalReconciliation
from typing import Any, Dict, List

pd.set_option('display.max_rows', None)
# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class HierarchicalForecast:
    def __init__(self, data: pd.DataFrame, hierarchy: List[Dict[str, Any]],period:int):
        """
        Initialize the HierarchicalForecast class with data and hierarchy.

        Parameters:
        data (pd.DataFrame): DataFrame containing hierarchical time series data.
        hierarchy (List[Dict[str, Any]]): List representing the hierarchical structure.
        """
        self.data = data
        self.hierarchy = hierarchy
        self.period = period
        self.hierarchical_df = None
        self.Y_hier_df = None
        self.S_df = None
        self.tags = None

    def preprocess_data(self):
        """
        Preprocess data by transforming and preparing hierarchical levels.
        """
        # Log-transform the data
        for col in self.data.columns:
            if col != 'ds':
                self.data[f'log_{col}'] = np.log(self.data[col])

        # Select log-transformed columns
        columns_to_keep = ['ds'] + [col for col in self.data.columns if 'log_' in col]
        self.data = self.data[columns_to_keep]

        # Generate hierarchical structure rows
        def create_rows(hierarchy, row, current_level):
            if not hierarchy:
                return []

            rows = []
            for node in hierarchy:
                metric_id = node['metric_id']
                new_level = f"{current_level}/{metric_id}" if current_level else f"/{metric_id}"

                # Recursively create rows for influences
                rows += create_rows(node['influences'], row, new_level)

                # Add the current metric
                rows.append({
                    'ds': row['ds'],
                    'level': new_level,
                    'metric': metric_id,
                    'y': row[f'log_{metric_id}']
                })

            return rows

        # Create rows for hierarchical DataFrame
        rows = []
        for _, row in self.data.iterrows():
            rows += create_rows(self.hierarchy, row, '')

        # Convert the list to a DataFrame
        self.hierarchical_df = pd.DataFrame(rows)
        print(self.hierarchical_df.head())

    def transform_hierarchical_df(self):
        """
        Transform hierarchical_df into separate level columns.
        """
        # Split the 'level' column into separate components
        level_split = self.hierarchical_df['level'].str.split('/', expand=True)

        # Create a new DataFrame with the transformed structure
        transformed_df = pd.DataFrame({
            'ds': self.hierarchical_df['ds'],
            'top_level': level_split[1].apply(lambda x: f"{x}" if pd.notna(x) else None),
            'middle_level': level_split[2].apply(lambda x: f"{x}" if pd.notna(x) else None),
            'bottom_level': level_split[3].apply(lambda x: f"{x}" if pd.notna(x) else None),
            'y': self.hierarchical_df['y']
        })

        transformed_df['middle_level'].fillna(transformed_df['top_level'], inplace=True)
        transformed_df['bottom_level'].fillna(transformed_df['middle_level'], inplace=True)
        self.hierarchical_df = transformed_df
        print(transformed_df.head())
        
        return transformed_df

    def aggregate_data(self):
        """
        Aggregate data according to the hierarchical levels.
        """
        hierarchy_levels = [
            ['top_level'],
            ['top_level', 'middle_level'],
            ['top_level', 'middle_level', 'bottom_level']
        ]

        self.Y_hier_df, self.S_df, self.tags = aggregate(df=self.hierarchical_df, spec=hierarchy_levels)
        self.Y_hier_df = self.Y_hier_df.reset_index()

    def filter_relevant_series(self):
        """
        Filter relevant series based on hierarchical structure and reconciliation needs.
        """
        df_pivot = self.hierarchical_df.pivot(index='ds', columns='bottom_level', values='y').reset_index()

        self.hierarchical_df['combination'] = self.hierarchical_df['top_level'] + '/' + self.hierarchical_df['middle_level'] + '/' + self.hierarchical_df['bottom_level']
        unique_combinations = self.hierarchical_df['combination'].unique()
        unique_combinations

        for combination in unique_combinations:
            levels = combination.split('/')
            if len(levels) == 3:
                new_col_name = '/'.join(levels)
                df_pivot[new_col_name] = df_pivot[levels[-1]]

        # Define metric calculations based on hierarchy
        df_melted = df_pivot.melt(
            id_vars=['ds'],
            value_vars=unique_combinations,
            var_name='unique_id',
            value_name='y'
        )

        self.Y_hier_df = df_melted.copy()
        relevant_series = set(df_melted['unique_id'].unique())

        def reduce_combination(combination):
            levels = combination.split('/')
            if len(levels) == 3:
                if levels[0] == levels[1] == levels[2]:
                    return levels[0]
                elif levels[1] == levels[2]:
                    return levels[0] + '/' + levels[1]
                else:
                    return combination
            return combination

        # Convert relevant_series to the reduced form
        reduced_relevant_series = {reduce_combination(comb) for comb in relevant_series}

        filtered_tags = {}
        for level, series in self.tags.items():
            filtered_tags[level] = [s for s in series if s in reduced_relevant_series]

        self.tags = filtered_tags

        self.S_df = self.S_df.loc[self.S_df.index.isin(reduced_relevant_series), self.S_df.columns.isin(reduced_relevant_series)]

    def train_models(self):
        """
        Train forecasting models and perform reconciliation.
        """
        # Split into train and test
        Y_test_df = self.Y_hier_df.groupby('unique_id').tail(4)
        Y_train_df = self.Y_hier_df.drop(Y_test_df.index).sort_values(by='ds')
        Y_test_df = Y_test_df.sort_values(by='ds')

        # Train models
        fcst = StatsForecast(df=Y_train_df, models=[AutoARIMA(), Naive()], freq='W', n_jobs=-1)
        Y_hat_df = fcst.forecast(h=self.period, fitted=True)
        Y_fitted_df = fcst.forecast_fitted_values()


        def update_index(index_value):
            parts = index_value.split('/')
            if len(parts) == 1:
                return f"{index_value}/{index_value}/{index_value}"
            elif len(parts) == 2:
                return f"{index_value}/{parts[1]}"
            else:
                return index_value

        self.S_df.index = self.S_df.index.map(update_index)

        # Filter predictions
        Y_hat_df = Y_hat_df[Y_hat_df.index.isin(self.S_df.index.unique())]
        Y_fitted_df = Y_fitted_df[Y_fitted_df.index.isin(self.S_df.index.unique())]

        # Reconciliation
        hrec = HierarchicalReconciliation(reconcilers=[BottomUp()])
        Y_hat_df = Y_hat_df.sort_values(by='ds')
        Y_fitted_df = Y_fitted_df.sort_values(by='ds')
        Y_rec_df = hrec.reconcile(Y_hat_df=Y_hat_df, Y_df=Y_fitted_df, S=self.S_df, tags=self.tags)
        Y_rec_df['AutoARIMA'] = np.exp(Y_rec_df['AutoARIMA'])
        Y_rec_df['Naive'] = np.exp(Y_rec_df['Naive'])
        Y_rec_df['AutoARIMA/BottomUp'] = np.exp(Y_rec_df['AutoARIMA/BottomUp'])
        Y_rec_df['Naive/BottomUp'] = np.exp(Y_rec_df['Naive/BottomUp'])
        Y_rec_df = Y_rec_df.head(self.period)
        df_reset = Y_rec_df.reset_index(drop=True)
        df_reset['ds'] = pd.to_datetime(df_reset['ds']).dt.strftime('%Y-%m-%d')
        # Converting to JSON
        json_result = df_reset.to_json(orient='records')

        return json_result

    def run(self):
        """
        Run the full forecasting pipeline.
        """
        try:
            logger.info("Starting data preprocessing...")
            self.preprocess_data()

            logger.info("Transforming hierarchical DataFrame...")
            self.hierarchical_df = self.transform_hierarchical_df()

            logger.info("Aggregating data according to hierarchical structure...")
            self.aggregate_data()

            logger.info("Filtering relevant series...")
            self.filter_relevant_series()

            logger.info("Training models and performing hierarchical reconciliation...")
            json_result = self.train_models()

            logger.info("Hierarchical forecasting and reconciliation complete.")
            return json_result

        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Generate or load your data
    date_range = pd.date_range(start='2020-01-01', periods=104, freq='W')
    np.random.seed(42)
    open_new_biz_opps = np.random.randint(50, 150, size=len(date_range))
    sqo_rate = np.random.uniform(0.1, 0.9, size=len(date_range))
    sqo_to_win_rate = np.random.uniform(0.05, 0.3, size=len(date_range))

    accept_opps = open_new_biz_opps * sqo_rate
    new_biz_deals = accept_opps * sqo_to_win_rate

    data = pd.DataFrame({
        'ds': date_range,
        'OpenNewBizOpps': open_new_biz_opps,
        'SQORate': sqo_rate,
        'SQOToWinRate': sqo_to_win_rate,
        'AcceptOpps': accept_opps,
        'NewBizDeals': new_biz_deals
    })

    # data = pd.read_csv("dummy_data.csv")

    hierarchy_list = [
        {
            "metric_id": "NewBizDeals",
            "influences": [
                {
                    "metric_id": "AcceptOpps",
                    "influences": [
                        {
                            "metric_id": "OpenNewBizOpps",
                            "influences": []
                        },
                        {
                            "metric_id": "SQORate",
                            "influences": []
                        }
                    ]
                },
                {
                    "metric_id": "SQOToWinRate",
                    "influences": []
                }
            ]
        }
    ]

    # Initialize and run the HierarchicalForecast
    forecast = HierarchicalForecast(data, hierarchy_list,period=4)
    results = forecast.run()
    
    print(results)

