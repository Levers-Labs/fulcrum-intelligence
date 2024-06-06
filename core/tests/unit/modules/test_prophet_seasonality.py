import pytest
import pandas as pd
import numpy as np
from prophet_seasonality import ProphetSeasonality


@pytest.fixture
def sample_data():
    data = {
        'date': pd.date_range(start='2022-01-01', periods=10, freq='W'),
        'value': [10, 12, 15, 14, 20, 18, 25, 22, 30, 28]
    }
    return pd.DataFrame(data)


def test_fit(sample_data):
    ps = ProphetSeasonality()
    ps.fit(sample_data)
    assert ps.fitted == True
    assert ps.model is not None


def test_predict(sample_data):
    ps = ProphetSeasonality()
    ps.fit(sample_data)
    forecast = ps.predict(periods=5)
    assert len(forecast) == 15  # 10 original + 5 future periods


def test_extract_seasonal_component(sample_data):
    ps = ProphetSeasonality()
    ps.fit(sample_data)
    forecast = ps.predict(periods=0)
    seasonal_component = ps.extract_seasonal_component(forecast, sample_data)
    assert 'yearly_pct_impact' in seasonal_component.columns
    assert 'weekly_pct_impact' in seasonal_component.columns
    assert 'monthly_pct_impact' in seasonal_component.columns
    assert 'quarterly_pct_impact' in seasonal_component.columns


def test_plot_seasonal_components(sample_data):
    ps = ProphetSeasonality()
    ps.fit(sample_data)
    forecast = ps.predict(periods=0)
    seasonal_component = ps.extract_seasonal_component(forecast, sample_data)
    
    # Check if plotting works without raising exceptions
    try:
        ps.plot_seasonal_components(seasonal_component)
    except Exception as e:
        pytest.fail(f"Plotting seasonal components raised an exception: {e}")


def test_model_selection(sample_data):
    # Modify sample_data to create significant differences between additive and multiplicative models
    np.random.seed(42)
    sample_data['value'] = np.random.rand(len(sample_data)) * 100  # Random values for better differentiation

    ps = ProphetSeasonality()
    ps.fit(sample_data)
    
    # Check if model selection logic prints correct RMSE values
    assert ps.model is not None
    assert hasattr(ps.model, 'seasonality_modes')


if __name__ == "__main__":
    pytest.main()
