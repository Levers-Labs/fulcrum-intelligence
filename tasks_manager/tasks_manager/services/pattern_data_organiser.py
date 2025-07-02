"""Service for fetching data for pattern analysis."""

import logging
from datetime import date
from typing import Any, Protocol, TypeVar

import pandas as pd
from pandas import DataFrame

from levers.models import DataSourceType, Granularity, PatternConfig
from query_manager.core.schemas import MetricDetail
from query_manager.semantic_manager.crud import SemanticManager

logger = logging.getLogger(__name__)

T = TypeVar("T")
DataDict = dict[str, DataFrame]


class DataFetcher(Protocol):
    """Protocol for data fetching functions."""

    async def __call__(self, metric_id: str, grain: Granularity, start_date: date, end_date: date, **kwargs) -> Any:
        """Fetch data with the given parameters."""
        ...


class PatternDataOrganiser:
    """Service for fetching and organizing data for pattern analysis."""

    def __init__(self, semantic_manager: SemanticManager):
        """
        Initialize the pattern data organiser.

        Args:
            semantic_manager: SemanticManager instance for fetching data
        """
        self.semantic_manager = semantic_manager

        # Data source fetchers mapping
        self._fetchers: dict[DataSourceType, DataFetcher] = {
            DataSourceType.METRIC_TIME_SERIES: self._fetch_metric_time_series,
            DataSourceType.METRIC_WITH_TARGETS: self._fetch_metric_with_targets,
            DataSourceType.DIMENSIONAL_TIME_SERIES: self._fetch_dimensional_time_series,
            DataSourceType.MULTI_METRIC: self._fetch_multi_metric,
            DataSourceType.METRIC_TARGETS_ONLY: self._fetch_metric_targets,
        }

    async def fetch_data_for_pattern(
        self,
        config: PatternConfig,
        metric_id: str,
        grain: Granularity,
        metric_definition: MetricDetail | None = None,
        **fetch_kwargs,
    ) -> DataDict:
        """
        Fetch all data required for a pattern based on its configuration.

        Args:
            config: Pattern configuration
            metric_id: The metric ID
            grain: Data granularity
            metric_definition: Optional metric definition containing related metrics
            **fetch_kwargs: Additional keyword arguments

        Returns:
            Dictionary of DataFrames with standardized data keys from the configuration
        """
        logger.info(
            "Fetching data for pattern %s, metric %s, grain %s",
            config.pattern_name,
            metric_id,
            grain,
        )

        # Set the pattern name in context for grain mapping
        self._current_pattern_name = config.pattern_name

        try:
            return await self._fetch_data_sources(
                config=config,
                metric_id=metric_id,
                grain=grain,
                metric_definition=metric_definition,
                **fetch_kwargs,
            )
        finally:
            # Clean up context
            self._current_pattern_name = None  # type: ignore

    async def _fetch_data_sources(
        self,
        config: PatternConfig,
        metric_id: str,
        grain: Granularity,
        metric_definition: MetricDetail | None = None,
        **fetch_kwargs,
    ) -> DataDict:
        """
        Fetch data for all data sources configured in the pattern.

        Args:
            config: Pattern configuration
            metric_id: The metric ID
            grain: Data granularity
            metric_definition: Optional metric definition
            kwargs: Additional keyword arguments

        Returns:
            Dictionary of DataFrames with keys from the configuration's data_sources
        """
        result: DataDict = {}

        for data_source in config.data_sources:
            data_key = data_source.data_key
            source_type = data_source.source_type

            # Calculate date range using the centralized method in AnalysisWindowConfig
            start_date, end_date = config.analysis_window.get_date_range(
                grain=grain, look_forward=data_source.look_forward
            )

            logger.debug(
                "Fetching %s data for pattern %s, date range %s to %s",
                source_type,
                config.pattern_name,
                start_date,
                end_date,
            )

            # Get the appropriate fetcher for this source type
            fetcher = self._fetchers.get(source_type)
            if not fetcher:
                logger.error(
                    "No fetcher available for source type %s in pattern %s",
                    source_type,
                    config.pattern_name,
                )
                raise ValueError(f"No fetcher available for source type {source_type} in pattern {config.pattern_name}")

            # Special handling for multi-metric source
            if source_type == DataSourceType.MULTI_METRIC:
                data = await self._handle_multi_metric_fetch(
                    fetcher=fetcher,
                    metric_id=metric_id,
                    grain=grain,
                    start_date=start_date,
                    end_date=end_date,
                    metric_definition=metric_definition,
                    **fetch_kwargs,
                )
            else:
                # Fetch data for this source type
                data = await fetcher(
                    metric_id=metric_id, grain=grain, start_date=start_date, end_date=end_date, **fetch_kwargs
                )
            df = self._convert_to_dataframe(data, source_type)

            result[data_key] = df
            logger.debug(
                "Fetched %d rows for %s data in pattern %s",
                len(df),
                source_type,
                config.pattern_name,
            )

        # Log empty datasets
        for key, df in result.items():
            if df.empty:
                logger.warning("Empty dataset for %s in pattern %s", key, config.pattern_name)

        return result

    async def _fetch_metric_time_series(
        self, metric_id: str, grain: Granularity, start_date: date, end_date: date, **kwargs
    ) -> list[dict[str, Any]]:
        """Fetch metric time series data."""
        series = await self.semantic_manager.get_metric_time_series(
            metric_id=metric_id,
            grain=grain,  # type: ignore
            start_date=start_date,
            end_date=end_date,
        )
        return [dict(date=item.date, value=item.value) for item in series]

    async def _fetch_metric_with_targets(
        self, metric_id: str, grain: Granularity, start_date: date, end_date: date, **kwargs
    ) -> list[dict[str, Any]]:
        """Fetch metric time series data with targets."""
        return await self.semantic_manager.get_metric_time_series_with_targets(
            metric_id=metric_id,
            grain=grain,  # type: ignore
            start_date=start_date,
            end_date=end_date,
        )

    async def _fetch_dimensional_time_series(
        self,
        metric_id: str,
        grain: Granularity,
        start_date: date,
        end_date: date,
        dimension_name: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Fetch dimensional time series data."""
        series = await self.semantic_manager.get_dimensional_time_series(
            metric_id=metric_id,
            grain=grain,  # type: ignore
            start_date=start_date,
            end_date=end_date,
            dimension_names=[dimension_name] if dimension_name else None,
        )
        return [
            dict(
                date=item.date,
                value=item.value,
                dimension_name=item.dimension_name,
                dimension_slice=item.dimension_slice,
            )
            for item in series
        ]

    async def _fetch_multi_metric(
        self,
        metric_id: str,
        grain: Granularity,
        start_date: date,
        end_date: date,
        metric_ids: list[str] | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Fetch data for multiple metrics."""
        all_metrics = metric_ids or [metric_id]
        result = await self.semantic_manager.get_multi_metric_time_series(
            metric_ids=all_metrics,
            grain=grain,  # type: ignore
            start_date=start_date,
            end_date=end_date,
        )
        return [dict(date=item.date, value=item.value, metric_id=item.metric_id) for item in result]

    async def _fetch_metric_targets(
        self, metric_id: str, grain: Granularity, start_date: date, end_date: date, **kwargs
    ) -> list[dict[str, Any]]:
        """Fetch metric targets only no."""

        targets = await self.semantic_manager.get_targets(metric_id=metric_id, start_date=start_date, end_date=end_date)
        return [
            dict(
                date=target.target_date,
                target_value=target.target_value,
                metric_id=target.metric_id,
                grain=target.grain.value,
            )
            for target in targets
        ]

    async def _handle_multi_metric_fetch(
        self,
        fetcher: DataFetcher,
        metric_id: str,
        grain: Granularity,
        start_date: date,
        end_date: date,
        metric_definition: MetricDetail | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Handle fetching for multi-metric data sources."""
        # Get related metrics from metric definition
        related_metrics: list[str] = []

        if metric_definition:
            # Extract related metrics based on relationships
            if metric_definition.influences:
                related_metrics.extend([m for m in metric_definition.influences])
            if metric_definition.inputs:
                related_metrics.extend([m for m in metric_definition.inputs])
            if metric_definition.outputs:
                related_metrics.extend([m for m in metric_definition.outputs])
            if metric_definition.influencers:
                related_metrics.extend([m for m in metric_definition.influencers])

        # Include the primary metric
        all_metrics = [metric_id] + related_metrics

        # Remove duplicates while preserving order
        all_metrics = list(dict.fromkeys(all_metrics))

        data = await fetcher(
            metric_id=metric_id,
            grain=grain,
            start_date=start_date,
            end_date=end_date,
            metric_ids=all_metrics,
        )

        return data

    def _convert_to_dataframe(self, data: Any, source_type: DataSourceType) -> DataFrame:
        """
        Convert API response data to a pandas DataFrame with appropriate structure.

        Args:
            data: Data from the semantic manager
            source_type: Type of data source

        Returns:
            Properly structured pandas DataFrame
        """
        if not data:
            return pd.DataFrame()

        # Convert to a DataFrame
        return pd.DataFrame(data)

    async def check_pattern_data_requirements(
        self, config: PatternConfig, metric_id: str, grain: Granularity
    ) -> dict[str, bool]:
        """
        Check if all required data for a pattern is available.

        Args:
            config: Pattern configuration
            metric_id: The metric ID
            grain: Data granularity

        Returns:
            Dictionary with data source keys and boolean availability
        """
        result = {}

        for data_source in config.data_sources:
            source_type = data_source.source_type

            # Calculate date range using the centralized method
            start_date, end_date = config.analysis_window.get_date_range(
                grain=grain, look_forward=data_source.look_forward
            )

            try:
                has_data = await self._check_data_availability(
                    source_type=source_type,
                    metric_id=metric_id,
                    grain=grain,
                    start_date=start_date,
                    end_date=end_date,
                )
            except Exception as e:
                logger.exception("Error checking data availability for %s: %s", source_type, str(e))
                has_data = False

            result[data_source.data_key] = has_data

        return result

    async def _check_data_availability(
        self,
        source_type: DataSourceType,
        metric_id: str,
        grain: Granularity,
        start_date: date,
        end_date: date,
    ) -> bool:
        """
        Check if data is available for a specific data source type.

        Args:
            source_type: The type of data source
            metric_id: The metric ID
            grain: Data granularity
            start_date: Start date for data fetching
            end_date: End date for data fetching

        Returns:
            True if data is available, False otherwise
        """
        # Get the appropriate fetcher for this source type
        fetcher = self._fetchers.get(source_type)
        if not fetcher:
            raise ValueError(f"No fetcher available for source type {source_type}")

        # Fetch data for this source type
        data = await fetcher(
            metric_id=metric_id,
            grain=grain,
            start_date=start_date,
            end_date=end_date,
        )

        # Return True if data is available, False otherwise
        return len(data) > 0
