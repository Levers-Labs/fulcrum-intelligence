import logging
import warnings

import numpy as np
import pandas as pd

warnings.simplefilter(action="ignore", category=FutureWarning)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def compute_initial_half_averages(
    data,
    index_of_first_half_average,
    processing_start_index,
    half_average_point,
    latest_two_half_average_indices,
):
    current_half_average_index = index_of_first_half_average
    row_count = 0
    half_average = 0
    initial_half_average_calculation_frequency = 2
    for _, row in data[processing_start_index:].iterrows():
        half_average += row["METRIC_VALUE"]
        row_count += 1

        if row_count % half_average_point == 0:
            latest_two_half_average_indices.append(int(current_half_average_index))
            data.at[current_half_average_index, "HALF_AVERAGE"] = half_average / half_average_point
            data.at[current_half_average_index, "CENTRAL_LINE"] = half_average / half_average_point
            current_half_average_index += half_average_point
            half_average = 0
            initial_half_average_calculation_frequency -= 1

        if initial_half_average_calculation_frequency == 0:
            break
    return data


def set_increment_per_time_period(data, processing_start_index, latest_two_half_average_indices, half_average_point):
    current_increment_per_time_period_index = processing_start_index
    data.at[current_increment_per_time_period_index, "INCREMENT_PER_TIME_PERIOD"] = (
        data.at[latest_two_half_average_indices[1], "HALF_AVERAGE"]
        - data.at[latest_two_half_average_indices[0], "HALF_AVERAGE"]
    ) / half_average_point

    return data, current_increment_per_time_period_index


def compute_central_line(
    data,
    latest_two_half_average_indices,
    half_average_point,
    processing_start_index,
    current_increment_per_time_period_index,
):
    # set the values at the indices of half average
    data.at[latest_two_half_average_indices[0], "CENTRAL_LINE"] = data.at[
        latest_two_half_average_indices[0], "HALF_AVERAGE"
    ]
    data.at[latest_two_half_average_indices[1], "CENTRAL_LINE"] = data.at[
        latest_two_half_average_indices[1], "HALF_AVERAGE"
    ]

    # set the first value:
    data.at[processing_start_index, "CENTRAL_LINE"] = data.at[latest_two_half_average_indices[0], "CENTRAL_LINE"] - (
        (half_average_point // 2) * data.at[current_increment_per_time_period_index, "INCREMENT_PER_TIME_PERIOD"]
    )

    for processing_start_index_rolling, _ in data[processing_start_index:].iterrows():
        if processing_start_index_rolling == processing_start_index:
            continue

        data.at[processing_start_index_rolling, "CENTRAL_LINE"] = (
            data.at[current_increment_per_time_period_index, "INCREMENT_PER_TIME_PERIOD"]
            # ruff-ignore[call-overload]
            + data.at[processing_start_index_rolling - 1, "CENTRAL_LINE"]  # type: ignore
        )
    return data


def compute_average_moving_ranges(data, processing_start_index, half_average_point, debug):
    average_moving_range: float = 0.0
    average_moving_range_row_count = 0
    for processing_start_index_rolling, _ in data[processing_start_index:].iterrows():
        if processing_start_index_rolling == 0:
            continue
        average_moving_range += data.at[processing_start_index_rolling, "MOVING_RANGES"]
        average_moving_range_row_count += 1

        if average_moving_range_row_count == (half_average_point + 1):
            if debug:
                logger.debug(
                    "average_moving_range: " + str(average_moving_range) + " " + str(half_average_point + 1),
                )
            average_moving_range = float(average_moving_range) / float(half_average_point + 1)
            break

    data["AVERAGE_MOVING_RANGE"][processing_start_index:] = average_moving_range

    return data


def compute_limits(data, processing_start_index):
    data["LCL"][processing_start_index:] = data.apply(
        lambda row: max(row["CENTRAL_LINE"] - row["AVERAGE_MOVING_RANGE"] * 2.66, 0),
        axis=1,
    )[processing_start_index:]
    data["UCL"][processing_start_index:] = data.apply(
        lambda row: row["CENTRAL_LINE"] + row["AVERAGE_MOVING_RANGE"] * 2.66, axis=1
    )[processing_start_index:]

    return data


def detect_signal(data, processing_start_index):
    consecutive_above_central_line_count = 0
    consecutive_below_central_line_count = 0
    values_above_or_below_central_line = []
    closer_to_ucl_or_lcl_than_central_line_count = []
    is_signal_detected = False
    signal_detected_index = -1

    for processing_start_index_rolling, _ in data[processing_start_index:].iterrows():

        if (
            data.at[processing_start_index_rolling, "METRIC_VALUE"] > data.at[processing_start_index_rolling, "UCL"]
            or data.at[processing_start_index_rolling, "METRIC_VALUE"] < data.at[processing_start_index_rolling, "LCL"]
        ):
            # SIGNAL DETECTED
            # ruff-ignore[call-overload]
            signal_detected_index = int(processing_start_index_rolling)  # type: ignore
            data.at[processing_start_index_rolling, "SIGNAL_DETECTED"] = "rule_1"
            is_signal_detected = True
            break

        if data.at[processing_start_index_rolling, "METRIC_VALUE"] > data.at[processing_start_index_rolling, "UCL"]:
            consecutive_above_central_line_count += 1
            consecutive_below_central_line_count = 0

        if data.at[processing_start_index_rolling, "METRIC_VALUE"] < data.at[processing_start_index_rolling, "LCL"]:
            consecutive_below_central_line_count += 1
            consecutive_above_central_line_count = 0

        if consecutive_above_central_line_count == 7 or consecutive_below_central_line_count == 7:
            # SIGNAL DETECTED
            # ruff-ignore[call-overload]
            signal_detected_index = int(processing_start_index_rolling)  # type: ignore
            data.at[processing_start_index_rolling, "SIGNAL_DETECTED"] = "rule_2"
            is_signal_detected = True
            break

        if abs(
            data.at[processing_start_index_rolling, "METRIC_VALUE"] - data.at[processing_start_index_rolling, "UCL"]
        ) < abs(
            data.at[processing_start_index_rolling, "METRIC_VALUE"]
            - data.at[processing_start_index_rolling, "CENTRAL_LINE"]
        ) or abs(
            data.at[processing_start_index_rolling, "METRIC_VALUE"] - data.at[processing_start_index_rolling, "LCL"]
        ) < abs(
            data.at[processing_start_index_rolling, "METRIC_VALUE"]
            - data.at[processing_start_index_rolling, "CENTRAL_LINE"]
        ):
            closer_to_ucl_or_lcl_than_central_line_count.append("y")
        else:
            closer_to_ucl_or_lcl_than_central_line_count.append("n")

        # check the last 4 values in the array closer_to_ucl_or_lcl_than_central_line_count:
        if len(closer_to_ucl_or_lcl_than_central_line_count) >= 4:
            if closer_to_ucl_or_lcl_than_central_line_count[-4:].count("y") >= 3:
                # SIGNAL DETECTED
                # ruff-ignore[call-overload]
                signal_detected_index = int(processing_start_index_rolling)  # type: ignore
                data.at[processing_start_index_rolling, "SIGNAL_DETECTED"] = "rule_3"
                is_signal_detected = True
                break

        if (
            data.at[processing_start_index_rolling, "METRIC_VALUE"]
            > data.at[processing_start_index_rolling, "CENTRAL_LINE"]
        ):
            values_above_or_below_central_line.append("a")
        elif (
            data.at[processing_start_index_rolling, "METRIC_VALUE"]
            < data.at[processing_start_index_rolling, "CENTRAL_LINE"]
        ):
            values_above_or_below_central_line.append("b")

        # check if 10 out of last 12 values - all are above or below central line
        if (
            values_above_or_below_central_line[-12:].count("a") >= 10
            or values_above_or_below_central_line[-12:].count("b") >= 10
        ):
            # SIGNAL DETECTED
            # ruff-ignore[call-overload]
            signal_detected_index = int(processing_start_index_rolling)  # type: ignore
            data.at[processing_start_index_rolling, "SIGNAL_DETECTED"] = "rule_4"
            is_signal_detected = True
            break
    return data, is_signal_detected, signal_detected_index


def apply_process_control_from_index(
    data: pd.DataFrame,
    half_average_point: int,
    processing_start_index: int,
    debug: bool = False,
) -> tuple[int, pd.DataFrame]:
    signal_detected_index = -1

    # case when there are not enough points left in the dataframe
    if processing_start_index + half_average_point + 1 > len(data) - 1:
        return signal_detected_index, data

    index_of_first_half_average = processing_start_index + half_average_point // 2
    latest_two_half_average_indices: list[float] = []

    # compute initial half averages
    data = compute_initial_half_averages(
        data,
        index_of_first_half_average,
        processing_start_index,
        half_average_point,
        latest_two_half_average_indices,
    )

    if debug:
        logger.debug(latest_two_half_average_indices)

    if len(latest_two_half_average_indices) < 2:
        return signal_detected_index, data

    # setting the INCREMENT_PER_TIME_PERIOD
    data, current_increment_per_time_period_index = set_increment_per_time_period(
        data, processing_start_index, latest_two_half_average_indices, half_average_point
    )

    # compute entire central line
    data = compute_central_line(
        data,
        latest_two_half_average_indices,
        half_average_point,
        processing_start_index,
        current_increment_per_time_period_index,
    )

    # compute the average moving ranges
    data = compute_average_moving_ranges(data, processing_start_index, half_average_point, debug)

    # compute the UCL & LCL
    data = compute_limits(data, processing_start_index)

    # now start scanning for signal
    data, is_signal_detected, signal_detected_index = detect_signal(data, processing_start_index)

    if debug:
        signal_detected_index = 22

    if not is_signal_detected:
        signal_detected_index = -1

    return signal_detected_index, data


def process_control(
    data: pd.DataFrame,
    metric_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    grain: str,
    debug: bool = False,
) -> list[dict]:
    """
    Implement the process control for the given list of metric values.

    param:
        metric_id: given metric_id in string format
        start_date = pd.to_datetime(start_date, format="%Y-%m-%d")
        end_date = pd.to_datetime(end_date, format="%Y-%m-%d")
        grain: Can be "DAY", "WEEK", "MONTH", and "QUARTER".

    NOTES:
         1. For any grain, there should be a 1:1 mapping between the grain and the metric value.
         2. Before passing the data to this function, it should be filtered on metric_id, start_date, and end_date.
         For e.g.: please have a look the file core/data/process_control.csv to check how the data should look like.
         3. Data should be in the sorted order of GRAIN used.

    response:
        A dict having:
            1. Half averages: numpy array
            2. Central Line: numpy array
            3. UCL: numpy array
            4. LCL: numpy array
            (additional information on the above-mentioned columns mentioned
             in the blog - https://www.staceybarr.com/measure-up/interpreting-signals-in-trending-kpis/)
            5. date: string - value of the 1st row of the column 'GRAIN'
            6. grain: string - the grain on which we want to apply the process control. Can be DAY, WEEK, MONTH,
                and QUARTER
            7. start_date: string - the start date from the data has been filtered
            8. end_date: string - the end date from the data has been filtered
            9. metric_id: string - the metric_id on which the data has been filtered

    Example usage:

        from core.fulcrum_core import AnalysisManager
        analysis_manager = AnalysisManager()
        response = analysis_manager.process_control(data, "","","","MONTH" , debug=False)
        logger.debug("Test file response:\n", response)

    """
    data.index = data.index.astype(int)
    data["METRIC_VALUE"] = pd.to_numeric(data["METRIC_VALUE"], errors="coerce")
    data = data.drop_duplicates(subset=["GRAIN"]).reset_index(drop=True)  # ensuring 1:1 mapping

    data["HALF_AVERAGE"] = np.nan
    data["INCREMENT_PER_TIME_PERIOD"] = np.nan
    data["CENTRAL_LINE"] = np.nan
    half_average_point = 9
    data["AVERAGE_MOVING_RANGE"] = np.nan
    data["UCL"] = np.nan
    data["LCL"] = np.nan
    data["SIGNAL_DETECTED"] = ""

    # compute the moving ranges
    data["MOVING_RANGES"] = data["METRIC_VALUE"].diff().abs()

    signal_detected_index = 0

    signal_detected_index, data = apply_process_control_from_index(
        data, half_average_point, signal_detected_index, debug=debug
    )
    while signal_detected_index != -1:
        signal_detected_index, data = apply_process_control_from_index(
            data, half_average_point, signal_detected_index, debug=debug
        )
        if debug:
            logger.debug("Signal detected index: " + str(signal_detected_index))
            break

    if debug:
        logger.debug(data)
    data.fillna("", inplace=True)
    data["DATE"] = data["GRAIN"]
    data["GRAIN"] = grain
    return data.to_dict(orient="records")
