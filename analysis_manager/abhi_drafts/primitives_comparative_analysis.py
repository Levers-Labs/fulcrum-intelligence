
from __future__ import annotations

import logging
from typing import List, Dict, Union, Sequence, Optional

import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.tsa.stattools import grangercausalitytests
import statsmodels.api as sm

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # silence unless configured by caller


# --------------------------------------------------------------------------- #
# 1.  Granger-causality
# --------------------------------------------------------------------------- #
def perform_granger_causality_test(
    df: pd.DataFrame,
    target: str,
    exog: str,
    maxlag: int = 14,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Run a (univariate) Granger-causality test: does *exog* help predict *target*?

    Parameters
    ----------
    df : pd.DataFrame
        Must contain **two** numeric columns: one called *target*, one *exog*.
        The index should be ordered chronologically (ascending).
    target : str
        Column in *df* that we are trying to predict.
    exog : str
        Column in *df* whose predictive power we are testing.
    maxlag : int, default 14
        Highest lag (in periods) to evaluate.
    verbose : bool, default False
        Whether to let `statsmodels` print its detailed output.

    Returns
    -------
    pd.DataFrame
        Columns:
        - ``lag``  : int
        - ``p_value`` : float  (SSR-based F-test p-value)

        The DataFrame has one row per tested lag (1 … *maxlag*).  Rows are
        sorted by lag ASC.  You can pick the row with the *smallest* p-value
        (< 0.05 conventionally) to identify the “best” lag.
    """
    if target not in df.columns or exog not in df.columns:
        raise ValueError("Both `target` and `exog` columns must be present in df")

    # Ensure numeric & drop NA rows
    sub = df[[target, exog]].copy().dropna()
    sub = sub.astype(float)
    if sub.shape[0] < (maxlag + 10):
        raise ValueError("Too few observations for requested `maxlag`.")

    # statsmodels expects columns ordered [target, exog]
    data_for_test = sub[[target, exog]]

    # Run test
    gc_dict = grangercausalitytests(
        data_for_test, maxlag=maxlag, verbose=verbose
    )

    # Extract p-values
    rows = []
    for lag, res in gc_dict.items():
        ssr_ftest_pvalue = res[0]["ssr_ftest"][1]  # (stat, p, df_denom, df_num)
        rows.append({"lag": lag, "p_value": ssr_ftest_pvalue})

    return pd.DataFrame(rows, columns=["lag", "p_value"])


# --------------------------------------------------------------------------- #
# 2.  Correlation matrix among multiple metrics                               #
# --------------------------------------------------------------------------- #
def compare_metrics_correlation(
    ledger_df: pd.DataFrame,
    method: str = "pearson",
    dropna: bool = True,
) -> pd.DataFrame:
    """
    Compute the pairwise correlation matrix for metrics in *ledger_df*.

    `ledger_df` format mirrors the Levers “ledger”: columns
    ``['metric_name', 'date', 'dimension', 'value']``.

    Only rows where ``dimension == "Total"`` are considered, matching the
    ComparativeAnalysis Pattern behaviour.

    Parameters
    ----------
    ledger_df : pd.DataFrame
    method : {"pearson", "spearman", "kendall"}, default "pearson"
    dropna : bool, default True
        If *True*, *pandas* will compute correlations pairwise after
        dropping NA rows for each column pair.

    Returns
    -------
    pd.DataFrame
        A symmetric correlation matrix whose indices/columns are metric names.
    """
    # Filter and pivot
    filt = ledger_df[ledger_df["dimension"] == "Total"]
    pivot = (
        filt.pivot_table(
            index="date",
            columns="metric_name",
            values="value",
            aggfunc="mean",
        )
        .sort_index()
    )
    if dropna:
        corr = pivot.corr(method=method)
    else:
        corr = pivot.corr(method=method, min_periods=1)

    return corr


# --------------------------------------------------------------------------- #
# 3.  Predictive significance (lag-based)                                     #
# --------------------------------------------------------------------------- #
def detect_metric_predictive_significance(
    x: Union[pd.Series, Sequence[float]],
    y: Union[pd.Series, Sequence[float]],
    maxlag: int = 14,
    method: str = "pearson",
    alpha: float = 0.05,
) -> pd.DataFrame:
    """
    Evaluate whether past values of *x* are statistically associated with
    contemporaneous *y*.

    A simple cross-correlation approach is used: for every lag *ℓ*
    (1 … *maxlag*) we compute the correlation between ``x.shift(ℓ)`` and *y*,
    along with its p-value.

    Parameters
    ----------
    x, y : array-like
        Numeric time-aligned sequences.
    maxlag : int, default 14
    method : {"pearson", "spearman"}, default "pearson"
    alpha : float, default 0.05
        Significance level for the boolean flag.

    Returns
    -------
    pd.DataFrame
        Columns:
        - ``lag``                : int
        - ``correlation``        : float
        - ``p_value``            : float
        - ``significant``        : bool  (p_value < alpha)
    """
    if method not in {"pearson", "spearman"}:
        raise ValueError("method must be 'pearson' or 'spearman'")

    x_series = pd.Series(x, dtype=float).reset_index(drop=True)
    y_series = pd.Series(y, dtype=float).reset_index(drop=True)
    if len(x_series) != len(y_series):
        raise ValueError("`x` and `y` must be of equal length")

    rows = []
    for lag in range(1, maxlag + 1):
        x_shifted = x_series.shift(lag)
        mask = ~(x_shifted.isna() | y_series.isna())
        if mask.sum() < 3:
            continue  # not enough overlap

        if method == "pearson":
            corr, pval = stats.pearsonr(x_shifted[mask], y_series[mask])
        else:  # spearman
            corr, pval = stats.spearmanr(x_shifted[mask], y_series[mask])

        rows.append(
            {
                "lag": lag,
                "correlation": corr,
                "p_value": pval,
                "significant": pval < alpha,
            }
        )

    return pd.DataFrame(rows, columns=["lag", "correlation", "p_value", "significant"])


# --------------------------------------------------------------------------- #
# 4.  Interaction-effect analysis                                             #
# --------------------------------------------------------------------------- #
def analyze_metric_interactions(
    df: pd.DataFrame,
    target: str,
    drivers: List[str],
    include_main_effects: bool = True,
) -> pd.DataFrame:
    """
    Quantify synergy (interaction terms) between multiple driver metrics.

    We fit a linear model:

    ``target ~ X1 + X2 + … + (X1 * X2) + …``

    Parameters
    ----------
    df : pd.DataFrame
        Must contain *target* and *drivers* columns, pre-aligned by date.
    target : str
        Dependent variable column.
    drivers : list[str]
        Independent variables to consider.
    include_main_effects : bool, default True
        If *False*, the model is ``target ~ X1*X2*…`` (interactions only).

    Returns
    -------
    pd.DataFrame
        One row per coefficient of interest (only interaction terms are
        returned), with columns:
        - ``term``      : str  (e.g. "X1:X2")
        - ``coef``      : float
        - ``t_stat``    : float
        - ``p_value``   : float
    """
    # Build design matrix with interaction terms
    X = df[drivers].astype(float).copy()
    if include_main_effects:
        for a in drivers:
            X[a] = sm.add_constant(X[a])  # statsmodels will auto-add const later

    # Generate interaction columns manually to retain full control
    inter_cols = []
    for i in range(len(drivers)):
        for j in range(i + 1, len(drivers)):
            col_name = f"{drivers[i]}:{drivers[j]}"
            X[col_name] = df[drivers[i]] * df[drivers[j]]
            inter_cols.append(col_name)

    if not inter_cols:
        raise ValueError("Need at least two drivers to compute interactions")

    # Fit OLS
    y = df[target].astype(float)
    X_ols = sm.add_constant(X)  # adds intercept
    model = sm.OLS(y, X_ols, missing="drop").fit()

    # Extract only interaction rows
    out_rows = []
    for term in inter_cols:
        if term in model.params.index:
            out_rows.append(
                {
                    "term": term,
                    "coef": model.params[term],
                    "t_stat": model.tvalues[term],
                    "p_value": model.pvalues[term],
                }
            )

    return pd.DataFrame(out_rows, columns=["term", "coef", "t_stat", "p_value"])


# --------------------------------------------------------------------------- #
# 5.  Benchmarking against a peer/industry metric                             #
# --------------------------------------------------------------------------- #
def benchmark_metrics_against_peers(
    df: pd.DataFrame,
    my_metric: str,
    peer_metric: str,
) -> pd.DataFrame:
    """
    Compare your metric’s performance to an external benchmark over time.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns ``["date", my_metric, peer_metric]``.
    my_metric : str
        Column representing your organisation’s metric.
    peer_metric : str
        Column representing the peer / industry benchmark.

    Returns
    -------
    pd.DataFrame
        Columns:
        - ``date``
        - ``my_value``
        - ``peer_value``
        - ``difference``       (my – peer)
        - ``ratio``            (my / peer)  [nan-safe]
    """
    required_cols = {"date", my_metric, peer_metric}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"df must contain {required_cols}")

    res = (
        df[["date", my_metric, peer_metric]]
        .copy()
        .rename(columns={my_metric: "my_value", peer_metric: "peer_value"})
    )
    res["difference"] = res["my_value"] - res["peer_value"]
    res["ratio"] = res["my_value"] / res["peer_value"].replace({0: np.nan})

    return res.sort_values("date").reset_index(drop=True)


# --------------------------------------------------------------------------- #
# 6.  Generic two-sample statistical test                                     #
# --------------------------------------------------------------------------- #
def detect_statistical_significance(
    group_a: Sequence[float],
    group_b: Sequence[float],
    test: str = "t-test",
    alpha: float = 0.05,
) -> Dict[str, Union[str, float, bool]]:
    """
    Determine whether *group_a* and *group_b* differ significantly.

    Supported tests
    ---------------
    - ``"t-test"``           : Welch’s two-sample t-test (unequal variances)
    - ``"mannwhitney"``      : Mann–Whitney U (non-parametric)
    - ``"ks"``               : Kolmogorov–Smirnov (distributional diff)

    Parameters
    ----------
    group_a, group_b : array-like
        Numeric samples.
    test : str, default "t-test"
    alpha : float, default 0.05
        Significance threshold.

    Returns
    -------
    dict
        {
          "test"      : str,
          "statistic" : float,
          "p_value"   : float,
          "significant": bool
        }
    """
    a = np.asarray(group_a, dtype=float)
    b = np.asarray(group_b, dtype=float)

    if test == "t-test":
        stat, pval = stats.ttest_ind(a, b, equal_var=False, nan_policy="omit")
    elif test == "mannwhitney":
        stat, pval = stats.mannwhitneyu(a, b, alternative="two-sided")
    elif test == "ks":
        stat, pval = stats.ks_2samp(a, b, alternative="two-sided", mode="auto")
    else:
        raise ValueError("Unsupported `test` specified")

    return {
        "test": test,
        "statistic": float(stat),
        "p_value": float(pval),
        "significant": bool(pval < alpha),
    }


if __name__ == "__main__":
    print("comparative_analysis primitives module loaded successfully")
