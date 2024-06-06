import matplotlib.pyplot as plt
from prophet import Prophet
import pandas as pd
from sklearn.metrics import mean_squared_error
import numpy as np


class ProphetSeasonality:
    def __init__(self, yearly_seasonality=True, weekly_seasonality=True):
        self.yearly_seasonality = yearly_seasonality
        self.weekly_seasonality = weekly_seasonality
        self.model = None
        self.fitted = False

    def fit(self, df: pd.DataFrame):
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = df['value'].astype(float)
        df.rename(columns={'date': 'ds', 'value': 'y'}, inplace=True)

        # Fit the model with additive seasonality
        model_additive = Prophet(yearly_seasonality=self.yearly_seasonality, 
                                 weekly_seasonality=self.weekly_seasonality, 
                                 seasonality_mode='additive')
        model_additive.add_seasonality(name='monthly', period=30.5, fourier_order=5)
        model_additive.add_seasonality(name='quarterly', period=91.25, fourier_order=5)
        model_additive.fit(df)
        forecast_additive = model_additive.predict(df)
        rmse_additive = np.sqrt(mean_squared_error(df['y'], forecast_additive['yhat']))

        # Fit the model with multiplicative seasonality
        model_multiplicative = Prophet(yearly_seasonality=self.yearly_seasonality, 
                                       weekly_seasonality=self.weekly_seasonality, 
                                       seasonality_mode='multiplicative')
        model_multiplicative.add_seasonality(name='monthly', period=30.5, fourier_order=5)
        model_multiplicative.add_seasonality(name='quarterly', period=91.25, fourier_order=5)
        model_multiplicative.fit(df)
        forecast_multiplicative = model_multiplicative.predict(df)
        rmse_multiplicative = np.sqrt(mean_squared_error(df['y'], forecast_multiplicative['yhat']))

        # Decide which model to use
        if rmse_additive < rmse_multiplicative:
            self.model = model_additive
            print("Additive model is better with RMSE:", rmse_additive)
        else:
            self.model = model_multiplicative
            print("Multiplicative model is better with RMSE:", rmse_multiplicative)

        self.fitted = True

    def predict(self, periods: int):
        if not self.fitted:
            raise ValueError("The model must be fitted before prediction.")
        future = self.model.make_future_dataframe(periods=periods)
        forecast = self.model.predict(future)
        return forecast

    def extract_seasonal_component(self, forecast: pd.DataFrame, actual_df: pd.DataFrame):
        seasonal_component = forecast[['ds', 'yhat', 'yearly', 'weekly', 'monthly', 'quarterly']].copy()
        actual_df['ds'] = pd.to_datetime(actual_df['date'])
        seasonal_component = pd.merge(seasonal_component, actual_df, on='ds', how='left')

        seasonal_component['yearly_pct_impact'] = seasonal_component['yearly'] / seasonal_component['value'] * 100
        seasonal_component['weekly_pct_impact'] = seasonal_component['weekly'] / seasonal_component['value'] * 100
        seasonal_component['monthly_pct_impact'] = seasonal_component['monthly'] / seasonal_component['value'] * 100
        seasonal_component['quarterly_pct_impact'] = seasonal_component['quarterly'] / seasonal_component['value'] * 100
        return seasonal_component

    def plot_seasonal_components(self, seasonal_component: pd.DataFrame):
        plt.figure(figsize=(14, 12))
        plt.subplot(511)
        plt.plot(seasonal_component['ds'], seasonal_component['yearly'], label='Yearly Seasonality')
        plt.legend()
        plt.subplot(512)
        plt.plot(seasonal_component['ds'], seasonal_component['weekly'], label='Weekly Seasonality')
        plt.legend()
        plt.subplot(513)
        plt.plot(seasonal_component['ds'], seasonal_component['monthly'], label='Monthly Seasonality')
        plt.legend()
        plt.subplot(514)
        plt.plot(seasonal_component['ds'], seasonal_component['quarterly'], label='Quarterly Seasonality')
        plt.legend()
        plt.subplot(515)
        plt.plot(seasonal_component['ds'], seasonal_component['yearly_pct_impact'], label='Yearly % Impact')
        plt.plot(seasonal_component['ds'], seasonal_component['weekly_pct_impact'], label='Weekly % Impact')
        plt.plot(seasonal_component['ds'], seasonal_component['monthly_pct_impact'], label='Monthly % Impact')
        plt.plot(seasonal_component['ds'], seasonal_component['quarterly_pct_impact'], label='Quarterly % Impact')
        
        plt.legend()
        plt.show()


def process():
    # Load data
    df = pd.read_csv(r"C:\Users\anubhav\Desktop\leverslabs\data_model\sqls_weekly_latest.csv")
    # df = pd.read_csv(r"C:\Users\anubhav\Desktop\leverslabs\data_24_04\Electric_Production.csv")

    # Initialize the forecasting model
    ps = ProphetSeasonality()

    # Fit the model
    ps.fit(df)

    # Predict with zero additional periods (for demonstration)
    forecast = ps.predict(periods=0)

    # Extract the seasonal component
    seasonal_component = ps.extract_seasonal_component(forecast, df)

    # Plot the seasonal components and percentage impacts
    ps.plot_seasonal_components(seasonal_component)

    # Print the seasonal components
    print("Seasonal Components:")
    print(seasonal_component)

    return seasonal_component


if __name__ == "__main__":
    seasonal_components = process()
