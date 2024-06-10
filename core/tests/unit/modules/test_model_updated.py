import pytest
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from fulcrum_core.execptions import AnalysisError
from fulcrum_core.modules.model_updated import DataModelling

@pytest.fixture
def sample_dataframes():
    df1 = pd.DataFrame({'date': pd.date_range(start='2022-01-01', periods=5),
                        'value1': [1, 2, 3, 4, 5]})
    df2 = pd.DataFrame({'date': pd.date_range(start='2022-01-01', periods=5),
                        'value2': [6, 7, 8, 9, 10]})
    return [df1, df2]

def test_merge_dataframes(sample_dataframes):
    model = DataModelling(dataframes=sample_dataframes)
    merged_df = model.merge_dataframes()
    assert len(merged_df) > 0  # Check if the merged dataframe is not empty

def test_read_data():
    data = pd.DataFrame({'date': [1, 2, 3, 4, 5], 'value': [10, 20, 30, 40, 50]})
    model = DataModelling()
    processed_data = model.read_data(data)
    assert len(processed_data) == len(data)  # Check if no rows were dropped
    assert 'date' not in processed_data.columns  # Check if the 'date' column was dropped

def test_linear_regression_equation():
    data = pd.DataFrame({'date': [1, 2, 3, 4, 5], 'value': [10, 20, 30, 40, 50]})
    model = DataModelling()
    linear_model, equation = model.linear_regression_equation(data)
    assert isinstance(linear_model, LinearRegression)
    assert isinstance(equation, dict)

def test_polynomial_regression_equation():
    data = pd.DataFrame({'date': [1, 2, 3, 4, 5], 'value': [10, 20, 30, 40, 50]})
    model = DataModelling()
    poly_model, equation = model.polynomial_regression_equation(data)
    assert isinstance(poly_model, Pipeline)
    assert isinstance(equation, dict)

def test_inference():
    X = pd.DataFrame({'date': [1, 2, 3, 4, 5]})
    y = pd.Series([10, 20, 30, 40, 50])
    model = DataModelling()
    y_pred = model.inference(X, y)
    assert len(y_pred) == len(y)  # Check if predictions were made for all instances

def test_linear_model_inference():
    model = LinearRegression()
    X = pd.DataFrame({'date': [1, 2, 3, 4, 5]})
    y = pd.Series([10, 20, 30, 40, 50])
    model.fit(X, y)
    model_trainer = DataModelling()
    rmse, y_pred = model_trainer.linear_model_inference(model, X, y)
    assert isinstance(rmse, float)
    assert len(y_pred) == len(y)

def test_polynomial_model_inference():
    model = Pipeline([('poly', PolynomialFeatures()), 
                      ('scaler', StandardScaler()), 
                      ('regressor', LinearRegression())])
    X = pd.DataFrame({'date': [1, 2, 3, 4, 5]})
    y = pd.Series([10, 20, 30, 40, 50])
    model.fit(X, y)
    model_trainer = DataModelling()
    rmse, y_pred = model_trainer.polynomial_model_inference(model, X, y)
    assert isinstance(rmse, float)
    assert len(y_pred) == len(y)

def test_analyze_with_missing_date_column():
    model = DataModelling()
    data = pd.DataFrame({'date': [1, 2, 3, 4, 5], 'value': [10, 20, 30, 40, 50]})
    with pytest.raises(AnalysisError):
        model.analyze(data)

def test_run_with_valid_data():
    file_paths = [
        r'C:\Users\anubhav\Desktop\leverslabs\data_model\accept_opps_weekly_latest.csv',
        r'C:\Users\anubhav\Desktop\leverslabs\data_model\sqls_weekly_latest.csv'
    ]
    model = DataModelling(file_paths=file_paths)
    analysis_result = model.run()
    assert analysis_result is not None
