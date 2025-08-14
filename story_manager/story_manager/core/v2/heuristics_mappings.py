# STORY_TYPE_HEURISTIC_MAPPING_V2 for v2 stories
# This is the mapping of story types to heuristics for each grain
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType

STORY_TYPE_HEURISTIC_MAPPING_V2: dict[str, Any] = {
    # Goal vs Actual - On Track
    StoryType.ON_TRACK: {
        Granularity.DAY: {"salient_expression": "{{performance_percent}} >= 5", "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": "{{performance_percent}} >= 5", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{performance_percent}} >= 5", "cool_off_duration": 1},
    },
    # Goal vs Actual - Off Track
    StoryType.OFF_TRACK: {
        Granularity.DAY: {"salient_expression": "{{gap_percent}} >= 5", "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": "{{gap_percent}} >= 5", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{gap_percent}} >= 5", "cool_off_duration": 1},
    },
    # Status Change - Improving Status
    StoryType.IMPROVING_STATUS: {
        Granularity.DAY: {
            "salient_expression": "({{performance_percent}} >= 3) and ({{old_status_duration}} >= 3)",
            "cool_off_duration": 3,
        },
        Granularity.WEEK: {
            "salient_expression": "({{performance_percent}} >= 5) and ({{old_status_duration}} >= 2)",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{performance_percent}} >= 5) and ({{old_status_duration}} >= 1)",
            "cool_off_duration": 1,
        },
    },
    # Status Change - Worsening Status
    StoryType.WORSENING_STATUS: {
        Granularity.DAY: {
            "salient_expression": "({{gap_percent}} >= 3) and ({{old_status_duration}} >= 3)",
            "cool_off_duration": 3,
        },
        Granularity.WEEK: {
            "salient_expression": "({{gap_percent}} >= 5) and ({{old_status_duration}} >= 2)",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{gap_percent}} >= 5) and ({{old_status_duration}} >= 1)",
            "cool_off_duration": 1,
        },
    },
    # TODO: is this needed? Likely Status - Likely On-Track StoryType.LIKELY_ON_TRACK: { Granularity.DAY: {
    #  "salient_expression": "{{deviation}} >= 5", "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression":
    #  "{{deviation}} >= 5", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{deviation}} >= 5",
    #  "cool_off_duration": 1}, }, # Likely Status - Likely Off-Track # CSV: [z]% = deviation from target (missing by
    #  percentage) # Template: deviation = deviation from target StoryType.LIKELY_OFF_TRACK: { Granularity.DAY: {
    #  "salient_expression": "{{deviation}} >= 5", "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression":
    #  "{{deviation}} >= 5", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{deviation}} >= 5",
    #  "cool_off_duration": 1}, }, Likely Status - Forecasted On-Track StoryType.FORECASTED_ON_TRACK: {
    #  Granularity.DAY: {"salient_expression": "{{deviation}} >= 5", "cool_off_duration": 3}, Granularity.WEEK: {
    #  "salient_expression": "{{deviation}} >= 5", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression":
    #  "{{deviation}} >= 5", "cool_off_duration": 1}, }, # Likely Status - Forecasted Off-Track # CSV: [z]% =
    #  deviation from target (missing by percentage) # Template: deviation = deviation from target
    #  StoryType.FORECASTED_OFF_TRACK: { Granularity.DAY: {"salient_expression": "{{deviation}} >= 5",
    #  "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": "{{deviation}} >= 5", "cool_off_duration":
    #  2}, Granularity.MONTH: {"salient_expression": "{{deviation}} >= 5", "cool_off_duration": 1}, },
    # TODO: Complete this mappings
    # Likely Status - Pacing On-Track # CSV: [z]% = deviation from target, [a]% = percentage through period #
    # Template: deviation = deviation from target, period_progress = percentage through period
    # StoryType.PACING_ON_TRACK: { Granularity.DAY: {"salient_expression": "({{deviation}} >= 3) and ({{
    # period_progress}} >= 20)", "cool_off_duration": 2}, Granularity.WEEK: {"salient_expression": "({{deviation}} >=
    # 3) and ({{period_progress}} >= 20)", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "({{
    # deviation}} >= 3) and ({{period_progress}} >= 20)", "cool_off_duration": 1}, }, # Likely Status - Pacing
    # Off-Track # CSV: [z]% = deviation from target, [a]% = percentage through period # Template: deviation =
    # deviation from target, period_progress = percentage through period StoryType.PACING_OFF_TRACK: {
    # Granularity.DAY: {"salient_expression": "({{deviation}} >= 3) and ({{period_progress}} >= 20)",
    # "cool_off_duration": 2}, Granularity.WEEK: {"salient_expression": "({{deviation}} >= 3) and ({{
    # period_progress}} >= 20)", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "({{deviation}}
    # >= 3) and ({{period_progress}} >= 20)", "cool_off_duration": 1}, }, # Required Performance # CSV: [x]% =
    # required growth rate # Template: required_growth = required growth rate (from original mapping)
    # StoryType.REQUIRED_PERFORMANCE: { Granularity.DAY: {"salient_expression": "{{required_growth}} >= 1",
    # "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": "{{required_growth}} >= 2",
    # "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{required_growth}} >= 5",
    # "cool_off_duration": 2}, }, # Hold Steady # CSV: [y] = time to maintain (number of periods) # Template:
    # time_to_maintain = time to maintain (from constants.py) StoryType.HOLD_STEADY: { Granularity.DAY: {
    # "salient_expression": "{{time_to_maintain}} >= 6", "cool_off_duration": 3}, Granularity.WEEK: {
    # "salient_expression": "{{time_to_maintain}} >= 4", "cool_off_duration": 2}, Granularity.MONTH: {
    # "salient_expression": "{{time_to_maintain}} >= 2", "cool_off_duration": 2}, }, # Significant Segments - Top 4
    # Segments # CSV: [z]% = max diff percent, [w]% = total share percent # Template: max_diff_percent = max diff
    # percent, total_share_percent = total share percent StoryType.TOP_4_SEGMENTS: { Granularity.DAY: {
    # "salient_expression": "({{max_diff_percent}} >= 2) and ({{total_share_percent}} >= 3)", "cool_off_duration":
    # 2}, Granularity.WEEK: {"salient_expression": "({{max_diff_percent}} >= 3) and ({{total_share_percent}} >= 5)",
    # "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "({{max_diff_percent}} >= 3) and ({{
    # total_share_percent}} >= 5)", "cool_off_duration": 1}, }, # Significant Segments - Bottom 4 Segments # CSV: [
    # z]% = max diff percent, [w]% = total share percent # Template: max_diff_percent = max diff percent,
    # total_share_percent = total share percent StoryType.BOTTOM_4_SEGMENTS: { Granularity.DAY: {
    # "salient_expression": "({{max_diff_percent}} >= 2) and ({{total_share_percent}} >= 3)", "cool_off_duration":
    # 2}, Granularity.WEEK: {"salient_expression": "({{max_diff_percent}} >= 3) and ({{total_share_percent}} >= 5)",
    # "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "({{max_diff_percent}} >= 3) and ({{
    # total_share_percent}} >= 5)", "cool_off_duration": 1}, }, # Significant Segments - Segment Comparison # CSV: [
    # x]% = performance difference percent # Template: performance_diff_percent = performance difference percent
    # StoryType.SEGMENT_COMPARISONS: { Granularity.DAY: {"salient_expression": "{{performance_diff_percent}} >= 3",
    # "cool_off_duration": 2}, Granularity.WEEK: {"salient_expression": "{{performance_diff_percent}} >= 3",
    # "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{performance_diff_percent}} >= 3",
    # "cool_off_duration": 1}, }, # Growth Rates - Accelerating Growth # CSV: [x]% = current growth, [y]% = average
    # growth # Template: current_growth = current growth, average_growth = average growth
    # StoryType.ACCELERATING_GROWTH: { Granularity.DAY: {"salient_expression": "({{current_growth}} - {{
    # average_growth}}) >= 2", "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": "({{current_growth}}
    # - {{average_growth}}) >= 5", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "({{
    # current_growth}} - {{average_growth}}) >= 8", "cool_off_duration": 1}, }, # Growth Rates - Slowing Growth #
    # CSV: [y]% = average growth, [x]% = current growth # Template: average_growth = average growth, current_growth =
    # current growth StoryType.SLOWING_GROWTH: { Granularity.DAY: {"salient_expression": "({{average_growth}} - {{
    # current_growth}}) >= 2", "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": "({{average_growth}}
    # - {{current_growth}}) >= 5", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "({{
    # average_growth}} - {{current_growth}}) >= 8", "cool_off_duration": 1}, }, # Long-Range - Improving Performance
    # CSV: [y]% = average growth, [z]% = overall growth # Template: trend_avg_growth = average growth, overall_growth
    # = overall growth (inferred) StoryType.IMPROVING_PERFORMANCE: { Granularity.DAY: {"salient_expression": "({{
    # trend_avg_growth}} >= 2) and ({{overall_growth}} >= 5)", "cool_off_duration": 7}, Granularity.WEEK: {
    # "salient_expression": "({{trend_avg_growth}} >= 2) and ({{overall_growth}} >= 5)", "cool_off_duration": 2},
    # Granularity.MONTH: {"salient_expression": "({{trend_avg_growth}} >= 2) and ({{overall_growth}} >= 5)",
    # "cool_off_duration": 2}, }, # Long-Range - Worsening Performance # CSV: [y]% = average decline, [z]% = overall
    # decline # Template: trend_avg_growth = average decline (negative), overall_decline = overall decline (inferred)
    # StoryType.WORSENING_PERFORMANCE: { Granularity.DAY: {"salient_expression": "({{trend_avg_growth}} >= 2) and ({{
    # overall_decline}} >= 5)", "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression": "({{
    # trend_avg_growth}} >= 2) and ({{overall_decline}} >= 5)", "cool_off_duration": 2}, Granularity.MONTH: {
    # "salient_expression": "({{trend_avg_growth}} >= 2) and ({{overall_decline}} >= 5)", "cool_off_duration": 2}, },
    # Trend Changes - Stable Trend # CSV: [n] = trend duration # Template: trend_duration = trend duration
    # StoryType.STABLE_TREND: { Granularity.DAY: {"salient_expression": "{{trend_duration}} >= 14",
    # "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression": "{{trend_duration}} >= 5",
    # "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{trend_duration}} >= 2",
    # "cool_off_duration": 2}, }, # Trend Changes - New Upward Trend # CSV: [x]% = trend average growth # Template:
    # trend_avg_growth = trend average growth StoryType.NEW_UPWARD_TREND: { Granularity.DAY: {"salient_expression":
    # "{{trend_avg_growth}} >= 2", "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression": "{{
    # trend_avg_growth}} >= 3", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{
    # trend_avg_growth}} >= 5", "cool_off_duration": 1}, }, # Trend Changes - New Downward Trend # CSV: [x]% = trend
    # average decline (negative growth) # Template: trend_avg_growth = trend average decline (will be negative)
    # StoryType.NEW_DOWNWARD_TREND: { Granularity.DAY: {"salient_expression": "{{trend_avg_growth}} <= -2",
    # "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression": "{{trend_avg_growth}} <= -3",
    # "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{trend_avg_growth}} <= -5",
    # "cool_off_duration": 1}, }, # Trend Changes - Performance Plateau StoryType.PERFORMANCE_PLATEAU: {
    # Granularity.DAY: {"salient_expression": None, "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression":
    # None, "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": None, "cool_off_duration": 1}, },
    # Trend Exceptions - Spike # CSV: [x]% = deviation above normal range # Template: deviation_percent = deviation
    # above normal range StoryType.SPIKE: { Granularity.DAY: {"salient_expression": "{{deviation_percent}} >= 5",
    # "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": "{{deviation_percent}} >= 5",
    # "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{deviation_percent}} >= 5",
    # "cool_off_duration": 1}, }, # Trend Exceptions - Drop # CSV: [x]% = deviation below normal range # Template:
    # deviation_percent = deviation below normal range StoryType.DROP: { Granularity.DAY: {"salient_expression": "{{
    # deviation_percent}} >= 5", "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": "{{
    # deviation_percent}} >= 5", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{
    # deviation_percent}} >= 5", "cool_off_duration": 1}, }, # Segment Changes - New Strongest Segment # CSV: [x]% =
    # diff from average percent (above segment avg) # Template: diff_from_avg_percent = diff from average percent
    # StoryType.NEW_STRONGEST_SEGMENT: { Granularity.DAY: {"salient_expression": "{{diff_from_avg_percent}} >= 5",
    # "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": "{{diff_from_avg_percent}} >= 5",
    # "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{diff_from_avg_percent}} >= 5",
    # "cool_off_duration": 1}, }, # Segment Changes - New Weakest Segment # CSV: [x]% = diff from average percent (
    # below segment avg) # Template: diff_from_avg_percent = diff from average percent StoryType.NEW_WEAKEST_SEGMENT:
    # { Granularity.DAY: {"salient_expression": "{{diff_from_avg_percent}} >= 5", "cool_off_duration": 3},
    # Granularity.WEEK: {"salient_expression": "{{diff_from_avg_percent}} >= 5", "cool_off_duration": 2},
    # Granularity.MONTH: {"salient_expression": "{{diff_from_avg_percent}} >= 5", "cool_off_duration": 1}, },
    # Segment Changes - New Largest Segment # CSV: [x]% = current share, [y]% = prior share # Template:
    # current_share_percent = current share, prior_share_percent = prior share StoryType.NEW_LARGEST_SEGMENT: {
    # Granularity.DAY: {"salient_expression": "({{current_share_percent}} - {{prior_share_percent}}) >= 2",
    # "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": "({{current_share_percent}} - {{
    # prior_share_percent}}) >= 5", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "({{
    # current_share_percent}} - {{prior_share_percent}}) >= 5", "cool_off_duration": 1}, }, # Segment Changes - New
    # Smallest Segment # CSV: [y]% = prior share, [x]% = current share # Template: prior_share_percent = prior share,
    # current_share_percent = current share StoryType.NEW_SMALLEST_SEGMENT: { Granularity.DAY: {"salient_expression":
    # "({{prior_share_percent}} - {{current_share_percent}}) >= 2", "cool_off_duration": 3}, Granularity.WEEK: {
    # "salient_expression": "({{prior_share_percent}} - {{current_share_percent}}) >= 5", "cool_off_duration": 2},
    # Granularity.MONTH: {"salient_expression": "({{prior_share_percent}} - {{current_share_percent}}) >= 5",
    # "cool_off_duration": 1}, }, # Record Values - Record High StoryType.RECORD_HIGH: { Granularity.DAY: {
    # "salient_expression": None, "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": None,
    # "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": None, "cool_off_duration": 1}, },
    # Record Values - Record Low StoryType.RECORD_LOW: { Granularity.DAY: {"salient_expression": None,
    # "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": None, "cool_off_duration": 2},
    # Granularity.MONTH: {"salient_expression": None, "cool_off_duration": 1}, }, # Benchmark Comparisons
    # StoryType.BENCHMARKS: { Granularity.DAY: {"salient_expression": "{{x}} >= 5", "cool_off_duration": 3},
    # Granularity.WEEK: {"salient_expression": "{{x}} >= 5", "cool_off_duration": 2}, Granularity.MONTH: {
    # "salient_expression": "{{x}} >= 5", "cool_off_duration": 1}, }, # Root Cause Summary - Primary Root Cause
    # Factor StoryType.PRIMARY_ROOT_CAUSE_FACTOR: { Granularity.DAY: {"salient_expression": "{{x}} >= 3",
    # "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": "{{x}} >= 3", "cool_off_duration": 2},
    # Granularity.MONTH: {"salient_expression": "{{x}} >= 3", "cool_off_duration": 1}, }, # Segment Drift - Growing
    # Segment StoryType.GROWING_SEGMENT: { Granularity.DAY: {"salient_expression": "{{z}} >= 5", "cool_off_duration":
    # 7}, Granularity.WEEK: {"salient_expression": "{{z}} >= 5", "cool_off_duration": 2}, Granularity.MONTH: {
    # "salient_expression": "{{z}} >= 5", "cool_off_duration": 1}, }, # Segment Drift - Shrinking Segment
    # StoryType.SHRINKING_SEGMENT: { Granularity.DAY: {"salient_expression": "{{z}} >= 5", "cool_off_duration": 7},
    # Granularity.WEEK: {"salient_expression": "{{z}} >= 5", "cool_off_duration": 2}, Granularity.MONTH: {
    # "salient_expression": "{{z}} >= 5", "cool_off_duration": 1}, }, # Segment Drift - Improving Segment
    # StoryType.IMPROVING_SEGMENT: { Granularity.DAY: {"salient_expression": "{{z}} >= 5", "cool_off_duration": 7},
    # Granularity.WEEK: {"salient_expression": "{{z}} >= 5", "cool_off_duration": 2}, Granularity.MONTH: {
    # "salient_expression": "{{z}} >= 5", "cool_off_duration": 1}, }, # Segment Drift - Worsening Segment
    # StoryType.WORSENING_SEGMENT: { Granularity.DAY: {"salient_expression": "{{z}} >= 5", "cool_off_duration": 7},
    # Granularity.WEEK: {"salient_expression": "{{z}} >= 5", "cool_off_duration": 2}, Granularity.MONTH: {
    # "salient_expression": "{{z}} >= 5", "cool_off_duration": 1}, }, # Component Drift - Improving Component
    # StoryType.IMPROVING_COMPONENT: { Granularity.DAY: {"salient_expression": "{{z}} >= 5", "cool_off_duration": 7},
    # Granularity.WEEK: {"salient_expression": "{{z}} >= 5", "cool_off_duration": 2}, Granularity.MONTH: {
    # "salient_expression": "{{z}} >= 5", "cool_off_duration": 1}, }, # Component Drift - Worsening Component
    # StoryType.WORSENING_COMPONENT: { Granularity.DAY: {"salient_expression": "{{z}} >= 5", "cool_off_duration": 7},
    # Granularity.WEEK: {"salient_expression": "{{z}} >= 5", "cool_off_duration": 2}, Granularity.MONTH: {
    # "salient_expression": "{{z}} >= 5", "cool_off_duration": 1}, }, # Influence Drift - Stronger Influence
    # Relationship StoryType.STRONGER_INFLUENCE: { Granularity.DAY: {"salient_expression": "({{y}} - {{z}}) >= 5",
    # "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression": "({{y}} - {{z}}) >= 5", "cool_off_duration":
    # 2}, Granularity.MONTH: {"salient_expression": "({{y}} - {{z}}) >= 5", "cool_off_duration": 1}, }, # Influence
    # Drift - Weaker Influence Relationship StoryType.WEAKER_INFLUENCE: { Granularity.DAY: {"salient_expression": "({
    # {z}} - {{y}}) >= 5", "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression": "({{z}} - {{y}}) >= 5",
    # "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "({{z}} - {{y}}) >= 5", "cool_off_duration":
    # 1}, }, # Influence Drift - Improving Influence Metric StoryType.IMPROVING_INFLUENCE: { Granularity.DAY: {
    # "salient_expression": "{{z}} >= 10", "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression": "{{z}}
    # >= 10", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{z}} >= 10", "cool_off_duration":
    # 1}, }, # Influence Drift - Worsening Influence Metric StoryType.WORSENING_INFLUENCE: { Granularity.DAY: {
    # "salient_expression": "{{z}} >= 10", "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression": "{{z}}
    # >= 10", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{z}} >= 10", "cool_off_duration":
    # 1}, }, # Headwinds/Tailwinds - Unfavorable Driver Trend StoryType.UNFAVORABLE_DRIVER_TREND: { Granularity.DAY:
    # {"salient_expression": "{{x}} >= 3", "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": "{{x}}
    # >= 5", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{x}} >= 8", "cool_off_duration":
    # 1}, }, # Headwinds/Tailwinds - Favorable Driver Trend StoryType.FAVORABLE_DRIVER_TREND: { Granularity.DAY: {
    # "salient_expression": "{{x}} >= 3", "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": "{{x}} >=
    # 5", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{x}} >= 8", "cool_off_duration": 1},
    # }, # Headwinds/Tailwinds - Unfavorable Seasonal Trend StoryType.UNFAVORABLE_SEASONAL_TREND: { Granularity.DAY:
    # {"salient_expression": "{{x}} >= 5", "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression": "{{x}}
    # >= 5", "cool_off_duration": 4}, Granularity.MONTH: {"salient_expression": "{{x}} >= 5", "cool_off_duration":
    # 2}, }, # Headwinds/Tailwinds - Favorable Seasonal Trend StoryType.FAVORABLE_SEASONAL_TREND: { Granularity.DAY:
    # {"salient_expression": "{{x}} >= 5", "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression": "{{x}}
    # >= 5", "cool_off_duration": 4}, Granularity.MONTH: {"salient_expression": "{{x}} >= 5", "cool_off_duration":
    # 2}, }, # Headwinds/Tailwinds - Volatility Alert StoryType.VOLATILITY_ALERT: { Granularity.DAY: {
    # "salient_expression": "{{x}} >= 3", "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression": "{{x}} >=
    # 5", "cool_off_duration": 4}, Granularity.MONTH: {"salient_expression": "{{x}} >= 5", "cool_off_duration": 2},
    # }, # Headwinds/Tailwinds - Concentration Risk StoryType.CONCENTRATION_RISK: { Granularity.DAY: {
    # "salient_expression": "{{x}} >= 70", "cool_off_duration": 7}, Granularity.WEEK: {"salient_expression": "{{x}}
    # >= 70", "cool_off_duration": 4}, Granularity.MONTH: {"salient_expression": "{{x}} >= 70", "cool_off_duration":
    # 2}, }, # Portfolio - Portfolio Status Overview StoryType.PORTFOLIO_STATUS_OVERVIEW: { Granularity.DAY: {
    # "salient_expression": "({{x}} + {{y}} + {{z}}) > 0", "cool_off_duration": 3}, Granularity.WEEK: {
    # "salient_expression": "({{x}} + {{y}} + {{z}}) > 0", "cool_off_duration": 2}, Granularity.MONTH: {
    # "salient_expression": "({{x}} + {{y}} + {{z}}) > 0", "cool_off_duration": 1}, }, # Portfolio - Portfolio
    # Performance Overview StoryType.PORTFOLIO_PERFORMANCE_OVERVIEW: { Granularity.DAY: {"salient_expression": "({{
    # x}} > 0) and ({{z}} > 0)", "cool_off_duration": 3}, Granularity.WEEK: {"salient_expression": "({{x}} > 0) and (
    # {{z}} > 0)", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "({{x}} > 0) and ({{z}} > 0)",
    # "cool_off_duration": 1}, }, # Seasonal Patterns - Seasonal Pattern Match StoryType.SEASONAL_PATTERN_MATCH: {
    # Granularity.DAY: {"salient_expression": "{{x}} <= 3", "cool_off_duration": 7}, Granularity.WEEK: {
    # "salient_expression": "{{x}} <= 3", "cool_off_duration": 2}, Granularity.MONTH: {"salient_expression": "{{x}}
    # <= 3", "cool_off_duration": 1}, }, # Seasonal Patterns - Seasonal Pattern Break
    # StoryType.SEASONAL_PATTERN_BREAK: { Granularity.DAY: {"salient_expression": "{{x}} >= 5", "cool_off_duration":
    # 7}, Granularity.WEEK: {"salient_expression": "{{x}} >= 5", "cool_off_duration": 2}, Granularity.MONTH: {
    # "salient_expression": "{{x}} >= 5", "cool_off_duration": 1}, },
}
