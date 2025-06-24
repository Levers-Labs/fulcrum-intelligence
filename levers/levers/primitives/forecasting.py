# =============================================================================
# Forecasting Primitives
#
# This file includes primitives for time series forecasting:
# - Statistical forecasting (Naive, SES, Holt-Winters, ARIMA)
# - Upstream metric forecasting
# - Scenario generation (best/worst cases)
# - Forecast accuracy evaluation
# - Forecast uncertainty assessment
# - Driver-based decomposition of forecasts
#
# Family: forecasting
# Version: 1.0
#
# Dependencies:
#   - pandas as pd
#   - numpy as np
#   - statsmodels.tsa.holtwinters
#   - pmdarima for auto_arima
# =============================================================================

from typing import Any

import numpy as np
import pandas as pd
from pmdarima import arima as pmd_arima
from statsmodels.tsa.holtwinters import ExponentialSmoothing, SimpleExpSmoothing

from levers.exceptions import PrimitiveError, ValidationError
from levers.models import Granularity
from levers.primitives import convert_grain_to_freq


def simple_forecast(
    df: pd.DataFrame,
    value_col: str = "value",
    periods: int = 7,
    method: str = "ses",
    seasonal_periods: int | None = None,
    date_col: str | None = None,
    freq: str | None = None,
    grain: Granularity | None = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Produce a forecast using one of various methods: naive, ses, holtwinters, or auto_arima.

    Family: forecasting
    Version: 1.0

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing time series data
    value_col : str, default="value"
        Column containing values to forecast
    periods : int, default=7
        Number of periods to forecast
    method : str, default="ses"
        Forecasting method: 'naive', 'ses', 'holtwinters', or 'auto_arima'
    seasonal_periods : int | None, default=None
        Number of periods in a seasonal cycle (e.g., 7 for weekly, 12 for monthly)
    date_col : str | None, default=None
        Column containing dates
    freq : str | None, default=None
        Pandas frequency string for resampling (e.g., 'D', 'W', 'M')
    grain : str | None, default=None
        Time grain description (e.g., 'day', 'week', 'month')
    **kwargs
        Additional parameters for the forecasting methods

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ['date', 'forecast']
    """
    # Validate inputs
    if value_col not in df.columns:
        raise ValidationError(f"value_col '{value_col}' not found in DataFrame")
    if date_col is not None and date_col not in df.columns:
        raise ValidationError(f"date_col '{date_col}' not found in DataFrame")

    valid_methods = ["naive", "ses", "holtwinters", "auto_arima"]
    if method not in valid_methods:
        raise PrimitiveError(f"method '{method}' not recognized. Use one of {valid_methods}", "simple_forecast")

    # Create a copy of the DataFrame
    dff = df.copy()

    # Time series preparation
    if date_col is None:
        # No date column provided, use index as time
        series = dff[value_col].dropna()
        if len(series) < 2 and method != "naive":
            method = "naive"  # Fallback to naive if not enough data
    else:
        # Convert date column to datetime
        dff[date_col] = pd.to_datetime(dff[date_col])
        dff.sort_values(date_col, inplace=True)
        dff.set_index(date_col, inplace=True)

        # If grain is provided, convert to frequency string
        if grain is not None:
            freq = convert_grain_to_freq(grain)

        # Resample if frequency is specified
        if freq is not None:
            # Resample to regular frequency
            dff = dff[value_col].resample(freq).mean()  # type: ignore
            series = dff.ffill()  # type: ignore  # Forward fill gaps
        else:
            series = dff[value_col].dropna()

        if len(series) < 2 and method != "naive":
            method = "naive"  # Fallback to naive if not enough data

    # Execute forecasting method
    if method == "naive":
        # Naive forecast: use the last observed value
        last_val = series.iloc[-1]

        # Generate future dates if date_col and freq provided
        if date_col and freq and not series.index.empty:
            last_date = series.index[-1]
            future_idx = pd.date_range(start=last_date, periods=periods + 1, freq=freq)[1:]
            fc_vals = [last_val] * periods
            return pd.DataFrame({"date": future_idx, "forecast": fc_vals})
        else:
            # Use numeric indices for future periods
            idx_future = np.arange(len(series), len(series) + periods)
            fc_vals = [last_val] * periods
            return pd.DataFrame({"date": idx_future, "forecast": fc_vals})

    elif method == "ses":
        # Simple Exponential Smoothing
        smoothing_level = kwargs.get("smoothing_level", 0.2)
        # Default to estimated initialization for better results
        model = SimpleExpSmoothing(series, initialization_method=kwargs.get("initialization_method", "estimated"))
        fit = model.fit(smoothing_level=smoothing_level, **{k: v for k, v in kwargs.items() if k != "smoothing_level"})
        fc_vals = fit.forecast(periods)

    elif method == "holtwinters":
        # Holt-Winters Exponential Smoothing
        trend = kwargs.pop("trend", None)
        seasonal = kwargs.pop("seasonal", None)

        model = ExponentialSmoothing(
            series,
            trend=trend,
            seasonal=seasonal,
            seasonal_periods=seasonal_periods,
            initialization_method=kwargs.pop("initialization_method", "estimated"),
        )
        fit = model.fit(**kwargs)
        fc_vals = fit.forecast(periods)

    elif method == "auto_arima":
        # ARIMA model selection with pmdarima
        do_seasonal = seasonal_periods is not None and seasonal_periods > 1

        default_arima_kwargs = dict(
            start_p=1,
            start_q=1,
            max_p=5,
            max_q=5,
            seasonal=do_seasonal,
            m=seasonal_periods if do_seasonal else 1,
            stepwise=True,
            error_action="ignore",
            suppress_warnings=True,
        )

        # Update defaults with any provided kwargs
        for k, v in default_arima_kwargs.items():
            kwargs.setdefault(k, v)

        model = pmd_arima.auto_arima(series, **kwargs)
        fc_vals = model.predict(n_periods=periods)

    # Generate output DataFrame with dates
    if date_col:
        # Future dates based on frequency
        if freq is None:
            # Fallback to numeric indices
            idx_future = np.arange(len(series), len(series) + periods)
            return pd.DataFrame({"date": idx_future, "forecast": fc_vals}).reset_index(drop=True)
        else:
            # Generate proper date range
            last_idx = series.index[-1]
            future_idx = pd.date_range(last_idx, periods=periods + 1, freq=freq)[1:]
            return pd.DataFrame({"date": future_idx, "forecast": fc_vals}).reset_index(drop=True)
    else:
        # Use numeric indices
        idx_future = np.arange(len(series), len(series) + periods)
        return pd.DataFrame({"date": idx_future, "forecast": fc_vals}).reset_index(drop=True)


def forecast_with_confidence_intervals(
    df: pd.DataFrame,
    value_col: str = "value",
    periods: int = 7,
    confidence_level: float = 0.95,
    method: str = "ses",
    date_col: str | None = None,
    freq: str | None = None,
    grain: Granularity | None = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Generate forecast with confidence intervals using bootstrap or model-based methods.

    Family: forecasting
    Version: 1.0

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing time series data
    value_col : str, default="value"
        Column containing values to forecast
    periods : int, default=7
        Number of periods to forecast
    confidence_level : float, default=0.95
        Confidence level for prediction intervals
    method : str, default="ses"
        Forecasting method
    date_col : str | None, default=None
        Column containing dates
    freq : str | None, default=None
        Pandas frequency string
    grain : str | None, default=None
        Time grain description
    **kwargs
        Additional parameters

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ['date', 'forecast', 'lower_bound', 'upper_bound']
    """
    # Get basic forecast
    forecast_df = simple_forecast(
        df=df, value_col=value_col, periods=periods, method=method, date_col=date_col, freq=freq, grain=grain, **kwargs
    )

    # Calculate confidence intervals using simple percentage-based approach
    # In a more sophisticated implementation, this would use model-specific methods
    forecast_values = forecast_df["forecast"]

    # Estimate volatility from historical data
    if len(df) > 1:
        returns = df[value_col].pct_change().dropna()
        volatility = returns.std()
    else:
        volatility = 0.1  # Default 10% volatility

    # Calculate confidence intervals
    z_score = 1.96 if confidence_level == 0.95 else 2.58 if confidence_level == 0.99 else 1.65
    margin = z_score * volatility * forecast_values

    forecast_df["lower_bound"] = forecast_values - margin
    forecast_df["upper_bound"] = forecast_values + margin
    forecast_df["confidence_level"] = confidence_level

    return forecast_df


def calculate_forecast_accuracy(
    actual_df: pd.DataFrame,
    forecast_df: pd.DataFrame,
    date_col: str = "date",
    actual_col: str = "actual",
    forecast_col: str = "forecast",
) -> dict[str, Any]:
    """
    Calculate accuracy metrics by comparing forecast with actual values.

    Family: forecasting
    Version: 1.0

    Parameters
    ----------
    actual_df : pd.DataFrame
        DataFrame containing actual values
    forecast_df : pd.DataFrame
        DataFrame containing forecast values
    date_col : str, default="date"
        Column containing dates for joining
    actual_col : str, default="actual"
        Column containing actual values
    forecast_col : str, default="forecast"
        Column containing forecast values

    Returns
    -------
    dict[str, Any]
        Dictionary containing accuracy metrics:
        - 'rmse': Root Mean Square Error
        - 'mae': Mean Absolute Error
        - 'mape': Mean Absolute Percentage Error
        - 'n': Number of observations used
        - 'bias': Mean forecast error (negative = under-forecast)
    """
    # Validate inputs
    if date_col not in actual_df.columns:
        raise ValidationError(f"date_col '{date_col}' not found in actual_df")
    if date_col not in forecast_df.columns:
        raise ValidationError(f"date_col '{date_col}' not found in forecast_df")
    if actual_col not in actual_df.columns:
        raise ValidationError(f"actual_col '{actual_col}' not found in actual_df")
    if forecast_col not in forecast_df.columns:
        raise ValidationError(f"forecast_col '{forecast_col}' not found in forecast_df")

    # Convert date columns to datetime for proper joining
    dfa = actual_df.copy()
    dff = forecast_df.copy()

    dfa[date_col] = pd.to_datetime(dfa[date_col])
    dff[date_col] = pd.to_datetime(dff[date_col])

    # Merge actual and forecast
    merged = pd.merge(dfa[[date_col, actual_col]], dff[[date_col, forecast_col]], on=date_col, how="inner").dropna(
        subset=[actual_col, forecast_col]
    )

    if merged.empty:
        return {"rmse": None, "mae": None, "mape": None, "n": 0, "bias": None}

    # Calculate error metrics
    errors = merged[forecast_col] - merged[actual_col]
    abs_errors = errors.abs()

    # Root Mean Square Error
    rmse = float(np.sqrt((errors**2).mean()))

    # Mean Absolute Error
    mae = float(abs_errors.mean())

    # Bias (mean forecast error)
    bias = float(errors.mean())

    # Mean Absolute Percentage Error
    def safe_ape(row):
        if row[actual_col] == 0:
            return None
        return abs(row[forecast_col] - row[actual_col]) / abs(row[actual_col]) * 100.0

    merged["ape"] = merged.apply(safe_ape, axis=1)
    mape = merged["ape"].mean(skipna=True)

    return {"rmse": rmse, "mae": mae, "mape": float(mape) if pd.notna(mape) else None, "n": len(merged), "bias": bias}


def generate_forecast_scenarios(
    forecast_df: pd.DataFrame,
    buffer_pct: float = 10.0,
    forecast_col: str = "forecast",
    best_case_col: str = "best_case",
    worst_case_col: str = "worst_case",
) -> pd.DataFrame:
    """
    Create best and worst case scenarios around an existing forecast.

    Family: forecasting
    Version: 1.0

    Parameters
    ----------
    forecast_df : pd.DataFrame
        DataFrame containing the forecast
    buffer_pct : float, default=10.0
        Percentage buffer to apply (e.g., 10.0 for ±10%)
    forecast_col : str, default="forecast"
        Column containing the forecast values
    best_case_col : str, default="best_case"
        Name for the best case column
    worst_case_col : str, default="worst_case"
        Name for the worst case column

    Returns
    -------
    pd.DataFrame
        Original DataFrame with additional best_case and worst_case columns
    """
    # Validate inputs
    if forecast_col not in forecast_df.columns:
        raise ValidationError(f"forecast_col '{forecast_col}' not found in DataFrame")

    # Create a copy of the DataFrame
    result_df = forecast_df.copy()

    # Ensure forecast column is numeric
    if not pd.api.types.is_numeric_dtype(result_df[forecast_col]):
        result_df[forecast_col] = pd.to_numeric(result_df[forecast_col], errors="coerce")

    # Calculate best and worst case values
    buffer_factor = buffer_pct / 100.0
    result_df[best_case_col] = result_df[forecast_col] * (1.0 + buffer_factor)
    result_df[worst_case_col] = result_df[forecast_col] * (1.0 - buffer_factor)

    return result_df
