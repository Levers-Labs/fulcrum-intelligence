import logging
from datetime import date, timedelta

import pandas as pd
import pmdarima as pm

from fulcrum_core.enums import Granularity
from fulcrum_core.execptions import AnalysisError, InsufficientDataError

logger = logging.getLogger(__name__)


class SimpleForecast:
    DEFAULT_CONFIDENCE_INTERVAL: float = 95
    DECIMAL_PRECISION = 3

    # todo: fix negative confidence_intervals
    # todo: fix issues for month even with min values
    # todo: fix issues where day grain takes too long to train

    def __init__(self, df: pd.DataFrame, grain: Granularity):
        """
        Initialize the SimpleForcasting class
        :param df: Input dataframe with date and value time series data
        :param grain: Granularity of the data
        """
        self.grain = grain
        self.min_values = self._get_min_data_points()
        self.grain_type, self.interval_gap = self._get_grain_type_interval_gap()
        self.freq = self._get_frequency()

        # Preprocess data
        self.df = self._preprocess_data(df)

        # Validate data
        self._validate_data()

    def _get_forecast_start_date(self) -> pd.Timestamp:
        """
        Get the start date for the forecasting
        """
        return self.df.index[-1] if isinstance(self.df.index, pd.DatetimeIndex) else self.df["date"].iloc[-1]

    def _get_min_data_points(self) -> int:
        """
        Get m value according to the grain
        """
        if self.grain == Granularity.WEEK:
            return 52
        elif self.grain == Granularity.MONTH:
            return 12
        elif self.grain == Granularity.QUARTER:
            return 4
        elif self.grain == Granularity.DAY:
            return 30
        else:
            return 1

    def _get_grain_type_interval_gap(self) -> tuple[str, timedelta]:
        """
        Get a grain type and interval gap according to the grain
        """
        if self.grain == Granularity.WEEK:
            interval_gap = timedelta(weeks=2)
            grain_type = "W"
        elif self.grain == Granularity.MONTH:
            interval_gap = timedelta(weeks=8)
            grain_type = "ME"
        elif self.grain == Granularity.QUARTER:
            interval_gap = timedelta(weeks=24)
            grain_type = "Q"
        else:
            interval_gap = timedelta(days=2)
            grain_type = "D"
        return grain_type, interval_gap

    def _get_frequency(self) -> str:
        """
        Get frequency according to the grain
        """
        if self.grain == Granularity.WEEK:
            return "W-MON"
        elif self.grain == Granularity.MONTH:
            return "MS"
        elif self.grain == Granularity.QUARTER:
            return "QS"
        else:
            return "D"

    def _preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess the data according to the grain
        Convert date to datetime
        Convert value to float
        Drop the rows with a date difference greater than the interval gap
        Resample the data according to the grain
        :return: Preprocessed dataframe
        """
        df["date"] = pd.to_datetime(df["date"])
        df["value"] = df["value"].astype(float)

        # Preprocessing steps conditioned according to grains
        i = 0
        while i < len(df) - 1:
            date_diff = df["date"].iloc[i + 1] - df["date"].iloc[i]
            if date_diff <= self.interval_gap:
                break
            else:
                df = df.drop(df.index[i])

        df.reset_index(drop=True, inplace=True)
        df.set_index("date", inplace=True)

        # Proportion greater than 80%
        proportion_greater_than_zero = (df["value"] > 0).mean()
        if proportion_greater_than_zero >= 0.8:
            # resampling
            df = df.resample(self.grain_type).sum().fillna(0)
            df.reset_index(inplace=True)
        else:
            logger.warning("Data proportion less than 80%")
        return df

    def _validate_data(self):
        """
        Validate the data according to the grain
        Validate the data points are greater than m value
        """
        if len(self.df) < self.min_values:
            raise InsufficientDataError(f"Data points should be greater than {self.min_values}")

    def train(self) -> pm.arima.ARIMA:
        """
        Train the ARIMA model with the training data
        :return: ARIMA model
        """
        # Training data
        train_values = self.df["value"]
        model = pm.auto_arima(
            train_values,
            start_p=1,
            start_q=1,
            max_p=5,
            max_q=5,
            m=self.min_values,
            seasonal=True,
            max_P=4,
            max_D=4,
            max_Q=4,
            trace=True,
            error_action="ignore",
            suppress_warnings=True,
            stepwise=True,
        )
        return model

    def _predict(
        self, future_dates: pd.DatetimeIndex, conf_interval: float = DEFAULT_CONFIDENCE_INTERVAL
    ) -> list[dict]:
        """
        Train the model on preprocessed dataframe.
        Predict the forecast values for future dates.
        :param future_dates: Future dates to predict
        :param conf_interval: a confidence interval
        :return: list of dict with date, value, confidence_interval
        """
        # Calculate confidence interval value according to percentage
        alpha = conf_interval / 100.0

        # train the model
        model = self.train()

        # Forecast length
        n_periods = len(future_dates)

        # Predict forecast values, and if confidence_interval not passed, then it takes default
        forecast_values, conf_int = model.predict(n_periods=n_periods, return_conf_int=True, alpha=alpha)

        # prepare date and value dict list response
        forecast_values = forecast_values.tolist()
        conf_int = conf_int.tolist()

        res = []
        for i in range(len(future_dates)):
            res.append(
                {
                    "date": future_dates[i].date(),  # noqa
                    "value": round(forecast_values[i], self.DECIMAL_PRECISION),
                    "confidence_interval": list(map(lambda x: round(x, self.DECIMAL_PRECISION), conf_int[i])),
                }
            )

        return res

    def predict_n(self, n: int, conf_interval: float = DEFAULT_CONFIDENCE_INTERVAL) -> list[dict]:
        """
        Predict n future dates
        Validate n > 1, if not raise ValueError
        Calculate future dates according to the granularity
        Getting the latest start date, and predicting future dates
        :param n: number of future dates to predict
        :param conf_interval: a confidence interval
        :return: list of dict with date, value, confidence_interval
        """
        start_date = self._get_forecast_start_date()

        # validate n > 1
        if n < 1:
            raise AnalysisError("n should be greater than 1")
        # Finding future dates forecasted according to the granularity
        future_dates = pd.date_range(start=start_date, periods=n, freq=self.freq, inclusive="neither")

        return self._predict(future_dates, conf_interval=conf_interval)

    def predict_till_date(self, end_date: date, conf_interval: float = DEFAULT_CONFIDENCE_INTERVAL) -> list[dict]:
        """
        Predict future dates till end_date
        Validate end_date > start_date, if not raise ValueError
        Calculate future dates according to the granularity
        Getting the latest start date, and predicting future dates
        :param end_date: end date to predict
        :param conf_interval: a confidence interval
        :return: list of dict with date, value, confidence_interval
        """
        start_date = self._get_forecast_start_date()

        # validate end_date > start_date
        if pd.Timestamp(end_date) <= start_date:
            raise AnalysisError("end_date should be greater than start_date")

        future_dates = pd.date_range(start=start_date, end=end_date, freq=self.freq)

        # exclude if start_date is in future_dates
        if start_date in future_dates:
            future_dates = future_dates[1:]

        # if no future dates, raise error
        if future_dates.empty:
            raise AnalysisError("No future dates to predict")

        return self._predict(future_dates, conf_interval=conf_interval)
