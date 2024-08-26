import copy
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

    # def integrate_yearly_seasonality(self, df: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     Integrate yearly seasonality into the DataFrame.
    #
    #     Args:
    #         df (pd.DataFrame): The input DataFrame.
    #
    #     Returns:
    #         pd.DataFrame: The DataFrame with integrated yearly seasonality.
    #     """
    #     seasonal_df = df.copy(deep=True)
    #     ps = SeasonalityAnalyzer(yearly_seasonality=True, weekly_seasonality=False)
    #
    #     seasonal_effects = pd.DataFrame({"date": seasonal_df["date"]})
    #
    #     temp_df = seasonal_df[["date", self.target_metric_id]].rename(columns={self.target_metric_id: "value"})
    #     ps.fit(temp_df)
    #     forecast = ps.predict(periods=0)
    #     forecast.rename(columns={"ds": "date"}, inplace=True)
    #     yearly_effect = forecast[["date", "yearly"]].rename(columns={"yearly": f"yearly_{self.target_metric_id}"})
    #     seasonal_effects = seasonal_effects.merge(yearly_effect, on="date")
    #
    #     return seasonal_df.merge(seasonal_effects, on="date")
    def integrate_yearly_seasonality(self, df: pd.DataFrame) -> pd.DataFrame:
        seasonal_df = df.copy(deep=True)
        ps = SeasonalityAnalyzer(yearly_seasonality=True, weekly_seasonality=False)

        seasonal_effects = pd.DataFrame({"date": seasonal_df["date"]})

        for column in seasonal_df.columns:
            if column != "date":
                temp_df = seasonal_df[["date", column]].rename(columns={column: "value"})
                ps.fit(temp_df)
                forecast = ps.predict(periods=0)
                forecast.rename(columns={"ds": "date"}, inplace=True)
                yearly_effect = forecast[["date", "yearly"]].rename(columns={"yearly": f"yearly_{column}"})
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

    def build_hierarchy(self, metric_id, flipped_dict):
        components = []
        if metric_id in flipped_dict:
            for child_metric in flipped_dict[metric_id]:
                components.append(self.build_hierarchy(child_metric, flipped_dict))

        return {
            "metric_id": metric_id,
            "model": {"coefficient": 0, "coefficient_root": 0, "relative_impact": 0, "relative_impact_root": 0},
            "components": components,
        }

    def find_metric(self, data, metric_id):
        if data["metric_id"] == metric_id:
            return data
        for component in data["components"]:
            result = self.find_metric(component, metric_id)
            if result:
                return result
        return None

    def calculate_total_sum(
        self, metric_id, flipped_relation_dict, value_dict, result_output, outcome_column, total_sum=0
    ):
        for id_list in flipped_relation_dict.values():
            if metric_id in id_list:
                for child_metric_id in id_list:
                    child_metric_info = self.find_metric(result_output, child_metric_id)
                    coefficient_root = child_metric_info["model"]["coefficient_root"]
                    value = value_dict[child_metric_id]
                    total_sum += coefficient_root * value

                if self.relation_dict[metric_id][0] == outcome_column:
                    break
                else:
                    total_sum = self.calculate_total_sum(
                        self.relation_dict[metric_id][0],
                        flipped_relation_dict,
                        value_dict,
                        result_output,
                        outcome_column,
                        total_sum,
                    )
                    return total_sum

        return total_sum

    def calculate_relative_impact(self, metric_id, flipped_relation_dict, value_dict, result_output):
        coefficients_values = [
            (self.find_metric(result_output, m_id)["model"]["coefficient"], value_dict[m_id])
            for m_id in flipped_relation_dict[metric_id]
        ]
        sum_coefficients_values = sum(c * v for c, v in coefficients_values)

        for m_id in flipped_relation_dict[metric_id]:
            metric_info = self.find_metric(result_output, m_id)
            coefficient = metric_info["model"]["coefficient"]
            value = value_dict[m_id]
            relative_impact = (coefficient * value) / sum_coefficients_values if sum_coefficients_values != 0 else 0
            metric_info["model"]["relative_impact"] = relative_impact * 100
            if metric_info["model"]["coefficient"] == metric_info["model"]["coefficient_root"]:
                metric_info["model"]["relative_impact_root"] = relative_impact * 100
            else:
                total_sum = self.calculate_total_sum(
                    m_id, flipped_relation_dict, value_dict, result_output, self.target_metric_id, 0
                )
                coefficient_root = metric_info["model"]["coefficient_root"]
                relative_impact_root = (coefficient_root * value) / total_sum * 100 if total_sum != 0 else 0
                metric_info["model"]["relative_impact_root"] = relative_impact_root

    def causal_analysis(self, merged_df: pd.DataFrame, columns: list[str], graph_definition: str) -> dict:
        """
        Perform causal analysis on the input data.

        Args:
            merged_df (pd.DataFrame): The input DataFrame.
            columns (list[str]): List of column names from the hierarchy.
            graph_definition (str): The causal graph definition string.

        Returns:
            dict: A dictionary containing the hierarchical result structure.
        """
        try:
            factor_columns = [col for col in merged_df.columns if col not in ["date", self.target_metric_id]]
            outcome_column = self.target_metric_id
            factor_columns.append(outcome_column)
            normalized_factor_columns = {self.normalize_name(col): col for col in factor_columns}
            graph_edges = graph_definition.split("digraph { ")[1].strip(" }").split("; ")
            updated_edges = []
            for edge in graph_edges:
                if "->" in edge:
                    src, dst = edge.split(" -> ")
                    src_norm = self.normalize_name(src)
                    dst_norm = self.normalize_name(dst)
                    src_replaced = normalized_factor_columns.get(src_norm, src)
                    dst_replaced = normalized_factor_columns.get(dst_norm, dst)
                    updated_edges.append(f"{src_replaced} -> {dst_replaced}")

            updated_graph_definition = f"digraph {{ {'; '.join(updated_edges)} }}"

            relationships = re.findall(r"(\w+)\s*->\s*(\w+)", updated_graph_definition)

            nodes = sorted({node for relation in relationships for node in relation})

            result = pd.DataFrame(0.0, index=nodes, columns=nodes)  # noqa

            self.relation_dict: dict = {}
            for source, target in relationships:
                if source in self.relation_dict:
                    self.relation_dict[source].append(target)
                else:
                    self.relation_dict[source] = [target]

            flipped_relation_dict: dict = {}
            for treatment, targets in self.relation_dict.items():
                for target in targets:
                    if target in flipped_relation_dict:
                        flipped_relation_dict[target].append(treatment)
                    else:
                        flipped_relation_dict[target] = [treatment]

            value_dict = {col: merged_df[col].mean() for col in merged_df.columns if col != "date"}

            result_output = self.build_hierarchy(outcome_column, flipped_relation_dict)

            target_columns = list(flipped_relation_dict.keys())

            def update_result_output(result_output, metric_id, treatment, coefficient, key="coefficient"):
                if result_output["metric_id"] == metric_id:
                    for component in result_output["components"]:
                        if component["metric_id"] == treatment:
                            component["model"][key] = coefficient
                            break
                else:
                    for component in result_output["components"]:
                        update_result_output(component, metric_id, treatment, coefficient, key)

            for outcome_column1 in target_columns:
                treatment_columns = flipped_relation_dict[outcome_column1]
                model1 = CausalModel(
                    data=merged_df, treatment=treatment_columns, outcome=outcome_column1, graph=updated_graph_definition
                )
                identified_estimand1 = model1.identify_effect(proceed_when_unidentifiable=True)
                causal_estimate1 = model1.estimate_effect(
                    identified_estimand1, method_name="backdoor.linear_regression"
                )
                effect_summary1 = causal_estimate1.estimator.model.params
                effect_names1 = effect_summary1.index[1:]
                for treatment1, effect_name in zip(treatment_columns, effect_names1):
                    coefficient = effect_summary1[effect_name]
                    update_result_output(result_output, outcome_column1, treatment1, coefficient, key="coefficient")

            direct_columns, indirect_columns = self.categorize_columns(updated_graph_definition)
            direct_columns = sorted(direct_columns, key=lambda x: factor_columns.index(x))
            indirect_columns = sorted(indirect_columns, key=lambda x: factor_columns.index(x))

            for i in range(len(indirect_columns)):
                model_in = CausalModel(
                    data=merged_df,
                    treatment=indirect_columns[i],
                    outcome=outcome_column,
                    graph=updated_graph_definition,
                )
                identified_estimand_in = model_in.identify_effect(
                    estimand_type="nonparametric-nie", proceed_when_unidentifiable=True
                )
                causal_estimate_in = model_in.estimate_effect(
                    identified_estimand_in, method_name="mediation.two_stage_regression"
                )
                coefficient_root = causal_estimate_in.value
                update_result_output(
                    result_output,
                    self.relation_dict[indirect_columns[i]][0],
                    indirect_columns[i],
                    coefficient_root,
                    key="coefficient_root",
                )

            def update_coefficient_root(data):
                if "model" in data:
                    if data["model"]["coefficient_root"] == 0 and data["model"]["coefficient"] != 0:
                        data["model"]["coefficient_root"] = data["model"]["coefficient"]

                if "components" in data:
                    for component in data["components"]:
                        update_coefficient_root(component)

            update_coefficient_root(result_output)

            updated_result_output = copy.deepcopy(result_output)

            for key in flipped_relation_dict:
                self.calculate_relative_impact(key, flipped_relation_dict, value_dict, updated_result_output)

            return updated_result_output

        except Exception as e:
            logger.error(f"Exception occurred: {e}")
            return {}

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

    def analyze(self, df: pd.DataFrame, *args, **kwargs) -> dict[str, Any] | pd.DataFrame:
        """
        Analyze the input DataFrame using causal model analysis.

        Args:
            df (pd.DataFrame): The input DataFrame.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            dict[str, Any] | pd.DataFrame: A dictionary containing the hierarchical result structure or a DataFrame.
        """
        input_dfs = kwargs.get("input_dfs", [])
        merged_df = self.merge_dataframes([df] + input_dfs)
        merged_df = self.integrate_yearly_seasonality(merged_df)
        columns, graph_definition = self.build_causal_graph()
        result = self.causal_analysis(merged_df, columns, graph_definition)
        return result
        # merged_df = self.merge_dataframes([df] + input_dfs)
        # merged_df = self.integrate_yearly_seasonality(merged_df)
        # columns, graph_definition = self.build_causal_graph()
        # result = self.causal_analysis(merged_df, columns, graph_definition)
        # contributions, natural_effect, graph = result["contributions"], result["natural_effect"], result["graph"]
        # coefficients, percentage = self.prepare_output(contributions, natural_effect, graph)
        # return coefficients, percentage

    def merge_dataframes(self, dataframes: list[pd.DataFrame]) -> pd.DataFrame:
        if len(dataframes) == 1:
            df = dataframes[0]
            df.columns = df.columns.str.strip()

            # Ensure 'date' and 'metric_id' columns exist
            if "date" not in df.columns or "metric_id" not in df.columns:
                raise ValueError("DataFrame must contain 'date' and 'metric_id' columns")

            # Convert 'date' column to datetime
            df["date"] = pd.to_datetime(df["date"])

            # Pivot the DataFrame to have metrics as columns
            df_pivoted = df.pivot(index="date", columns="metric_id", values="value").reset_index()
            df_pivoted.columns.name = None  # Remove the name from the columns index

            df_pivoted = self.integrate_yearly_seasonality(df_pivoted)
            return df_pivoted

        dfs = []
        for df in dataframes:
            df.columns = df.columns.str.strip()

            # Ensure 'date' and 'metric_id' columns exist
            if "date" not in df.columns or "metric_id" not in df.columns:
                raise ValueError("DataFrame must contain 'date' and 'metric_id' columns")

            # Convert 'date' column to datetime
            df["date"] = pd.to_datetime(df["date"])

            # Pivot the DataFrame to have metrics as columns
            df_pivoted = df.pivot(index="date", columns="metric_id", values="value").reset_index()
            df_pivoted.columns.name = None  # Remove the name from the columns index

            dfs.append(df_pivoted)

        # Find common dates across all DataFrames
        common_dates = set(dfs[0]["date"])
        for df in dfs[1:]:
            common_dates &= set(df["date"])

        # Filter DataFrames to include only common dates
        dfs = [df[df["date"].isin(common_dates)] for df in dfs]

        # Merge all DataFrames
        merged_df = dfs[0]
        for df in dfs[1:]:
            merged_df = merged_df.merge(df, on="date", how="outer")

        merged_df = self.integrate_yearly_seasonality(merged_df)
        return merged_df

    def normalize_name(self, name):
        return name.replace("_", "").lower()
