import pandas as pd
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from typing import Any

import warnings
warnings.filterwarnings("ignore")

from fulcrum_core.execptions import AnalysisError, InsufficientDataError
from fulcrum_core.modules import BaseAnalyzer
import logging

logger = logging.getLogger(__name__)

class DataModelling(BaseAnalyzer):
    def __init__(self, file_paths=None, dataframes=None):
        """
        Initialize the Model class with either file paths or dataframes.
        
        Parameters:
        file_paths (list of str): List of file paths to the CSV files to be merged.
        dataframes (list of DataFrame): List of dataframes to be merged.
        """
        self.file_paths = file_paths
        self.dataframes = dataframes

    def merge_dataframes(self):
        """
        Merges multiple data files or dataframes based on the 'date' column and returns the merged dataframe.
        """
        dfs = []

        if self.file_paths:
            # Getting values from all files and changing column name 'value' to each specific filename_value
            for file_path in self.file_paths:
                df = pd.read_csv(file_path)
                df['date'] = pd.to_datetime(df['date'])
                value_column_name = file_path.split('\\')[-1].replace('.csv', '') + '_value'
                df.rename(columns={'value': value_column_name}, inplace=True)
                dfs.append(df)
        elif self.dataframes:
            # Directly use dataframes and change 'value' column names accordingly
            for i, df in enumerate(self.dataframes):
                df['date'] = pd.to_datetime(df['date'])
                value_column_name = f'dataframe_{i}_value'
                df.rename(columns={'value': value_column_name}, inplace=True)
                dfs.append(df)

        # Fetching common dates in all features
        common_dates = dfs[0]['date']
        for df in dfs[1:]:
            common_dates = common_dates[common_dates.isin(df['date'])]

        # Keeping only those dates that are common
        dfs = [df[df['date'].isin(common_dates)] for df in dfs]

        # Making a dataframe with 1st column as date and other features values 
        merged_df = dfs[0]
        for df in dfs[1:]:
            merged_df = merged_df.merge(df, on='date')

        return merged_df

    def read_data(self, merged_df):
        """
        Prepare data for modeling by dropping missing values and the 'date' column.
        """
        data = merged_df.dropna()
        data = data.drop('date', axis=1)
        return data

    def linear_regression_equation(self, data):
        """
        Fit a linear regression model and return the fitted model along with the equation.
        """
        X = data.iloc[:, :-1]  
        y = data.iloc[:, -1]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        scaler_X = StandardScaler()
        X_train_scaled = scaler_X.fit_transform(X_train)

        scaler_y = StandardScaler()
        y_train_scaled = scaler_y.fit_transform(y_train.values.reshape(-1, 1))

        model = LinearRegression()
        model.fit(X_train_scaled, y_train_scaled.ravel())

        # Equation coefficients
        coef = model.coef()
        intercept = model.intercept()

        equation = {col: coef[i] for i, col in enumerate(X_train.columns)}
        equation['constant'] = intercept

        return model, equation

    def polynomial_regression_equation(self, data):
        """
        Fit a polynomial regression model with hyperparameter tuning and scaling, and return the fitted model along with the equation.
        """
        param_grid = {
            'poly__degree': [2, 3, 4, 5]
        }

        X = data.iloc[:, :-1]  
        y = data.iloc[:, -1]

        # Splitting the data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        pipeline = Pipeline([
            ('poly', PolynomialFeatures()), 
            ('scaler', StandardScaler()), 
            ('regressor', LinearRegression())
        ])

        grid_search = GridSearchCV(pipeline, param_grid, cv=5)
        grid_search.fit(X_train, y_train)

        best_degree = grid_search.best_params_['poly__degree']
        best_model = grid_search.best_estimator()

        coef = best_model.named_steps['regressor'].coef()
        intercept = best_model.named_steps['regressor'].intercept()

        equation = {f"{col}^{i}": coef[j] for i in range(1, best_degree+1) for j, col in enumerate(X.columns)}
        equation['constant'] = intercept

        return best_model, equation

    def inference(self, X, y):
        rf_model = RandomForestRegressor(random_state=42)
        rf_model.fit(X, y)
        y_pred = rf_model.predict(X)
        return y_pred

    def linear_model_inference(self, model, X, y):
        """
        Perform inference using the linear regression model.
        """
        y_pred = model.predict(X)
        rmse = mean_squared_error(y, y_pred, squared=False)
        return rmse, y_pred

    def polynomial_model_inference(self, model, X, y):
        """
        Perform inference using the polynomial regression model.
        """
        y_pred = model.predict(X)
        rmse = mean_squared_error(y, y_pred, squared=False)
        return rmse, y_pred

    def analyze(self, df: pd.DataFrame, *args, **kwargs) -> dict[str, Any] | pd.DataFrame:
        """
        Analyze the provided DataFrame and return the analysis results.
        """
        try:
            # Read and preprocess the data
            data = self.read_data(df)

            # Features and target
            X = data.iloc[:, :-1]  
            y = data.iloc[:, -1]

            # Linear regression model and equation
            linear_model, linear_equation = self.linear_regression_equation(data)

            # Polynomial regression model and equation
            polynomial_model, polynomial_equation = self.polynomial_regression_equation(data)

            # Perform inference for both models
            linear_rmse, linear_pred = self.linear_model_inference(linear_model, X, y)
            poly_rmse, poly_pred = self.polynomial_model_inference(polynomial_model, X, y)

            # Data inference using Random Forest
            data_inference = self.inference(X, y)

            if linear_rmse<=poly_rmse:
                result = {
                "linear_model": linear_model,
                "linear_equation": linear_equation,
                "linear_rmse": linear_rmse,
                "linear_predictions": linear_pred,
                "polynomial_predictions": poly_pred,
                "data_inference": data_inference
                }
            else:
                result = {
                "linear_predictions": linear_pred,
                "polynomial_model": polynomial_model,
                "polynomial_equation": polynomial_equation,
                "polynomial_rmse": poly_rmse,
                "polynomial_predictions": poly_pred,
                "data_inference": data_inference
                }

            return result

        except Exception as e:
            logger.error(f"An error occurred during analysis: {str(e)}")
            raise AnalysisError(f"An error occurred during analysis: {str(e)}")

    def run(self):
        merge_df = self.merge_dataframes()
        analysis_result = self.analyze(merge_df)
        return analysis_result

if __name__ == "__main__":
    file_paths = [
        r'C:\Users\anubhav\Desktop\leverslabs\data_model\accept_opps_weekly_latest.csv',
        r'C:\Users\anubhav\Desktop\leverslabs\data_model\new_prosps_weekly_latest.csv',
        r'C:\Users\anubhav\Desktop\leverslabs\data_model\new_sales_accept_leads_weekly_latest.csv',
        r'C:\Users\anubhav\Desktop\leverslabs\data_model\sqls_weekly_latest.csv',
        r'C:\Users\anubhav\Desktop\leverslabs\data_model\new_biz_deals_weekly_latest.csv'
    ]

    model_trainer = DataModelling(file_paths=file_paths)
    analysis_result = model_trainer.run()