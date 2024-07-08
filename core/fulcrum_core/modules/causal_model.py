import logging
import re
from typing import Any

import pandas as pd
from dowhy import CausalModel

from fulcrum_core.execptions import AnalysisError
from fulcrum_core.modules import BaseAnalyzer, SeasonalityAnalyzer

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
logger = logging.getLogger(__name__)


class CausalModelAnalyzer(BaseAnalyzer):
    """
    A class for performing causal model analysis on time series data.
    """

    def __init__(self, target_metric_id: str, influencers: list[dict[str, Any]], precision: int = 3, **kwargs):
        """
        Initialize the CausalModelAnalyzer.

        Args:
            target_metric_id (str): The target metric id.
            influencers (list[dict[str, Any]]): List of influencer dictionaries defining the causal relationships.
            precision (int): The decimal precision for rounding values. Default is 3.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(precision=precision)
        self.target_metric_id = target_metric_id
        self.influencers = influencers

    def validate_input(self, df: pd.DataFrame, **kwargs):
        """
        Validate the input DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame to validate.
            **kwargs: Additional keyword arguments.

        Raises:
            InsufficientDataError: If the input DataFrame is empty or missing required columns.
        """
        # make sure it has metric_id, date and value column
        if not all(col in df.columns for col in ["metric_id", "date", "value"]):
            raise ValueError("Invalid input dataframe. It should have 'metric_id', 'date', and 'value' columns.")
        # make sure metric_id is unique
        if df["metric_id"].nunique() > 1:
            raise ValueError("Invalid input dataframe. 'metric_id' should be unique.")
        input_dfs = kwargs.get("input_dfs", [])
        if len(input_dfs) > 0:
            for df in input_dfs:
                if not all(col in df.columns for col in ["metric_id", "date", "value"]):
                    raise ValueError(
                        "Invalid input dataframe. It should have 'metric_id', 'date', and 'value' columns."
                    )
                # make sure metric_id is unique
                if df["metric_id"].nunique() > 1:
                    raise ValueError("Invalid input dataframe. 'metric_id' should be unique.")

        # validate the influencers ( min 1 influencer)
        if len(self.influencers) < 1:
            raise ValueError("Influencers list is empty. Please provide at least one influencer.")

    def integrate_yearly_seasonality(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Integrate yearly seasonality into the DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with integrated yearly seasonality.
        """
        seasonal_df = df.copy(deep=True)
        ps = SeasonalityAnalyzer(yearly_seasonality=True, weekly_seasonality=False)

        seasonal_effects = pd.DataFrame({"date": seasonal_df["date"]})

        temp_df = seasonal_df[["date", self.target_metric_id]].rename(columns={self.target_metric_id: "value"})
        ps.fit(temp_df)
        forecast = ps.predict(periods=0)
        forecast.rename(columns={"ds": "date"}, inplace=True)
        yearly_effect = forecast[["date", "yearly"]].rename(columns={"yearly": f"yearly_{self.target_metric_id}"})
        seasonal_effects = seasonal_effects.merge(yearly_effect, on="date")

        return seasonal_df.merge(seasonal_effects, on="date")

    def get_hierarchical_columns_and_edges(self, node: dict[str, Any]) -> tuple[list[str], list[str]]:
        """
        Extract columns and graph edges from the influencers list in a hierarchical manner.

        Args:
            node (dict[str, Any]): The node from which to extract columns and edges.

        Returns:
            tuple[list[str], list[str]]: A tuple containing the list of selected columns and graph edges.
        """
        columns = []
        edges = []

        # Normalize the metric ID of the current node and add it to the columns list if not already present
        metric_id = node["metric_id"]
        if metric_id not in columns:
            columns.append(metric_id)

        # Iterate over the influencers of the current node
        influencers = node.get("influencers", [])
        for influence in influencers:
            # Normalize the metric ID of the influence and add it to the columns list if not already present
            source_metric = influence["metric_id"]
            if source_metric not in columns:
                columns.append(source_metric)
            # Add an edge from the influence to the current node
            edges.append(f"{source_metric} -> {metric_id}")
            # Recursively extract columns and edges from the influence
            inf_columns, inf_edges = self.get_hierarchical_columns_and_edges(influence)
            # Filter out columns that are already in the list to avoid duplicates
            inf_columns = [col for col in inf_columns if col not in columns]
            # Extend the columns and edges lists with the new columns and edges
            columns.extend(inf_columns)
            edges.extend(inf_edges)

        return columns, edges

    def build_causal_graph(self) -> tuple[list[str], str]:
        """
        Build a causal graph definition string from for the influencers list.

        Returns:
            tuple[list[str], str]: A tuple containing the list of selected columns and graph edges.
        """
        # get the columns and edges from the influencers list
        nodes, edges = self.get_hierarchical_columns_and_edges(
            {"metric_id": self.target_metric_id, "influencers": self.influencers}
        )
        if not edges:
            raise AnalysisError("Graph edges are empty. Check the hierarchy JSON and column mappings.")
        # add seasonlity node as well as edges
        nodes.append(f"yearly_{self.target_metric_id}")
        edges.append(f"yearly_{self.target_metric_id} -> {self.target_metric_id}")
        # build the graph definition string
        return nodes, f"digraph {{ {'; '.join(edges)}; }}"

    def parse_graph_definition(self, graph_definition: str) -> list[tuple[str, str]]:
        """
        Parse a causal graph definition string into a list of graph edges.

        Args:
            graph_definition (str): The causal graph definition string.

        Returns:
            list[tuple[str, str]]: List of graph edges as tuples of source and target column names.
        """
        edges = []
        graph_definition = graph_definition.strip().replace("digraph {", "").replace("}", "")
        for line in graph_definition.strip().split(";"):
            line = line.strip()

            if "->" in line:
                parts = line.split("->")
                source = parts[0].strip()
                target = parts[1].strip()
                edges.append((source, target))
        return edges

    def categorize_columns(self, graph_definition: str) -> tuple[list[str], list[str]]:
        """
        Categorize columns into direct and indirect impact columns based on the causal graph definition.

        Args:
            graph_definition (str): The causal graph definition string.

        Returns:
            tuple[list[str], list[str]]: A tuple containing the lists of direct and indirect impact columns.
        """
        edges = self.parse_graph_definition(graph_definition)
        # Identify direct columns
        direct_columns = {source for source, target in edges if target == self.target_metric_id}
        indirect_columns = {source for source, target in edges if target != self.target_metric_id}

        return list(direct_columns), list(indirect_columns)

    def effect_to_percentage(self, effect: float, total_absolute_effect: float) -> float:
        """
        Convert an effect value to a percentage of the total absolute effect.

        Args:
            effect (float): The effect value.
            total_absolute_effect (float): The total absolute effect value.

        Returns:
            float: The percentage effect value.
        """
        if total_absolute_effect == 0:
            return 0
        return (abs(effect) / total_absolute_effect) * 100

    def normalize_effects_row(self, row: pd.Series, effects: list[str]) -> dict[str, float]:
        """
        Normalize the effects of a single row to percentages.

        Args:
            row (pd.Series): The input row.
            effects (list[str]): List of effect column names.

        Returns:
            dict[str, float]: Dictionary of normalized percentage effects.
        """
        total_absolute_effect = sum(abs(row[effect]) for effect in effects)
        # if total absolute effect is zero, return zero for all effects
        if total_absolute_effect == 0:
            return {effect: 0 for effect in effects}
        return {effect: (abs(row[effect]) / total_absolute_effect) * 100 for effect in effects}

    def causal_analysis(
        self, merged_df: pd.DataFrame, columns: list[str], graph_definition: str
    ) -> tuple[pd.DataFrame, pd.DataFrame, str]:
        """
        Perform causal analysis on the input data.

        Args:
            merged_df (pd.DataFrame): The input DataFrame.
            columns (list[str]): List of column names from the hierarchy.
            graph_definition (str): The causal graph definition string.

        Returns:
            tuple[pd.DataFrame, pd.DataFrame, str]: A tuple containing the contributions DataFrame,
            natural effect DataFrame, and updated graph definition string.
        """
        factor_columns = [col for col in columns if col != self.target_metric_id]
        # model definintion containing data, treatment variables which are factor columns, outcome and digraph
        model = CausalModel(
            data=merged_df,
            # Exclude the target column from treatment
            treatment=factor_columns,
            outcome=self.target_metric_id,
            graph=graph_definition,
        )

        # Identifies effect and estimates coefficients
        identified_estimand = model.identify_effect(proceed_when_unidentifiable=True)
        causal_estimate = model.estimate_effect(identified_estimand, method_name="backdoor.linear_regression")
        effect = causal_estimate.realized_estimand_expr
        effect_summary = causal_estimate.estimator.model.params

        # Coefficients x1, x2 etc. are changed to original column names dynamically
        factor_dict = {f"x{i + 1}": factor_columns[i] for i in range(len(factor_columns))}

        # Calculating direct effects of all columns to outcome
        direct_effects = {}
        for generic_name, original_name in factor_dict.items():
            if generic_name not in effect_summary.index:
                logger.warning(
                    "Coefficient for %s (mapped from %s) not found in the model.", original_name, generic_name
                )
                continue
            direct_effects[original_name] = effect_summary[generic_name]

        # Seperating direct impact and indirect impact columns
        direct_columns, indirect_columns = self.categorize_columns(graph_definition)
        # sorting the columns based on the index of the column in the factor_columns list
        direct_columns = sorted(direct_columns, key=lambda x: factor_columns.index(x))
        indirect_columns = sorted(indirect_columns, key=lambda x: factor_columns.index(x))

        # calculating Percentage effect ie. total absolute effect
        total_absolute_effect = sum(abs(direct_effects[variable]) for variable in factor_columns)

        normalized_percentage_impact = {
            variable: self.effect_to_percentage(direct_effects[variable], total_absolute_effect)
            for variable in factor_columns
        }

        impact_df = pd.DataFrame(normalized_percentage_impact, index=[0])

        impact_df.columns = [f"{variable}_normalized_percentage" for variable in impact_df.columns]  # type: ignore

        normalized_row_effects = merged_df.apply(lambda row: self.normalize_effects_row(row, factor_columns), axis=1)

        # Adding columns coefficient and percantage for each column
        for effect in factor_columns:
            merged_df["coefficient_" + effect] = direct_effects[effect]

        for effect in factor_columns:
            merged_df[effect + "_percentage"] = normalized_row_effects.apply(lambda x: x[effect])  # noqa

        natural_effect = pd.DataFrame()

        # Calculating direct and indirect impacts of columns which have indirect impact on outcome
        for i in range(len(indirect_columns)):
            model_in = CausalModel(
                data=merged_df,
                treatment=indirect_columns[i],  # Exclude the outcome column from treatment
                outcome=self.target_metric_id,
                graph=graph_definition,
            )
            identified_estimand_nde = model_in.identify_effect(
                estimand_type="nonparametric-nde", proceed_when_unidentifiable=True
            )
            identified_estimand_nie = model_in.identify_effect(
                estimand_type="nonparametric-nie", proceed_when_unidentifiable=True
            )
            estimate_nde = model_in.estimate_effect(
                identified_estimand_nde, method_name="mediation.two_stage_regression"
            )
            estimate_nie = model_in.estimate_effect(
                identified_estimand_nie, method_name="mediation.two_stage_regression"
            )

            natural_effect[f"coefficient_NDE_{indirect_columns[i]}"] = [estimate_nde.value]
            natural_effect[f"coefficient_NIE_{indirect_columns[i]}"] = [estimate_nie.value]

            total_effect = abs(estimate_nde.value) + abs(estimate_nie.value)

            if total_effect != 0:
                natural_effect[f"NDE_{indirect_columns[i]}_percentage"] = [
                    (abs(estimate_nde.value) / total_effect) * 100
                ]
                natural_effect[f"NIE_{indirect_columns[i]}_percentage"] = [
                    (abs(estimate_nie.value) / total_effect) * 100
                ]
            else:
                natural_effect[f"NDE_{indirect_columns[i]}_percentage"] = [0]
                natural_effect[f"NIE_{indirect_columns[i]}_percentage"] = [0]

        natural_effect = pd.concat([natural_effect, impact_df], axis=1)
        # Dropping original columns
        merged_df = merged_df.drop(factor_columns, axis=1)
        merged_df = merged_df.drop(self.target_metric_id, axis=1)
        merged_df = merged_df.drop("date", axis=1)
        return merged_df, natural_effect, graph_definition

    def prepare_output(
        self, contributions: pd.DataFrame, natural_effect: pd.DataFrame, graph: str
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Prepare the output coefficients and percentage impact DataFrames.

        Args:
            contributions (pd.DataFrame): The contributions DataFrame.
            natural_effect (pd.DataFrame): The natural effect DataFrame.
            graph (str): The causal graph definition string.

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the coefficients and percentage impact DataFrames.
        """
        coefficient_columns = natural_effect.filter(regex="^coefficient")
        coefficient_df = pd.DataFrame(coefficient_columns)

        contributions = contributions.drop(contributions.filter(regex="_percentage$").columns, axis=1)

        natural_effect = natural_effect.drop(natural_effect.filter(regex="^coefficient_").columns, axis=1)
        pattern = r"^(NDE|NIE).*percentage$"

        # Filter the columns using the regex pattern
        nde_nie_percentage_columns = natural_effect.loc[:, natural_effect.columns.str.contains(pattern)]

        # Save the filtered DataFrame to another variable
        filtered_df = nde_nie_percentage_columns
        natural_effect = natural_effect.loc[:, ~natural_effect.columns.str.contains("NDE|NIE")]
        natural_effect.columns = natural_effect.columns.str.replace("_normalized_percentage", "", regex=False)

        contributions = contributions.iloc[[0]]

        columns_to_remove = [
            col.split("coefficient_NDE_")[1] for col in coefficient_df.columns if col.startswith("coefficient_NDE_")
        ]

        contributions.drop(
            columns=[
                f"coefficient_{col}" for col in columns_to_remove if f"coefficient_{col}" in contributions.columns
            ],
            inplace=True,
        )
        final_contributions = pd.merge(coefficient_df, contributions, left_index=True, right_index=True)
        final_contributions.columns = final_contributions.columns.str.replace("coefficient_", "", regex=False)

        relationships = re.findall(r"(\w+)\s*->\s*(\w+)", graph)

        # Initialize matrix with nodes as indices
        nodes = sorted({node for relation in relationships for node in relation})
        coefficients = pd.DataFrame(0.0, index=nodes, columns=nodes)
        relation_dict: dict = {}
        for source, target in relationships:
            if source in relation_dict:
                relation_dict[source].append(target)
            else:
                relation_dict[source] = [target]

        # Map coefficients from final_contributions to matrix based on relationships
        for col_name, value in final_contributions.iloc[0].items():
            col_name = str(col_name)
            if "NDE_" in col_name:
                source_node = col_name.split("NDE_")[1]
                current_node = source_node
                while current_node in relation_dict:
                    next_node = relation_dict[current_node][0]  # Assuming one-to-one relationship
                    current_node = next_node
                coefficients.loc[source_node, current_node] = value
            if "NIE_" in col_name:
                source_node = col_name.split("NIE_")[1]
                for source, target in relationships:
                    if source == source_node:
                        coefficients.loc[source, target] = value
            else:
                for source, target in relationships:
                    if source == col_name:
                        coefficients.loc[source, target] = value

        percentage = pd.concat([natural_effect, filtered_df], axis=1)

        return coefficients, percentage

    def analyze(
        self, df: pd.DataFrame, input_dfs: list[pd.DataFrame], **kwargs  # type: ignore  # noqa
    ) -> pd.DataFrame:
        """
        Analyze the input DataFrame using causal model analysis.

        Args:
            df (pd.DataFrame): The input DataFrame.
            input_dfs (list[pd.DataFrame]): List of input DataFrames.
            **kwargs: Additional keyword arguments.

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the coefficients and percentage impact DataFrames.
        """
        merged_df = self.merge_dataframes([df] + input_dfs)
        merged_df = self.integrate_yearly_seasonality(merged_df)
        columns, graph_definition = self.build_causal_graph()
        contributions, natural_effect, graph = self.causal_analysis(merged_df, columns, graph_definition)
        coefficients, _ = self.prepare_output(contributions, natural_effect, graph)
        return coefficients
