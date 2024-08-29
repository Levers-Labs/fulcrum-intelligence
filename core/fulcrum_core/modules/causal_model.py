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
    def __init__(self, target_metric_id: str, influencers: list[dict[str, Any]], precision: int = 3, **kwargs):
        super().__init__(precision=precision)
        self.target_metric_id = target_metric_id
        self.influencers = influencers
        self.relation_dict = {}
        self.flipped_relation_dict = {}
        self.column_mapping = {}
        self.reverse_column_mapping = {}

    def validate_input(self, df: pd.DataFrame, **kwargs):
        if not all(col in df.columns for col in ["metric_id", "date", "value"]):
            raise ValueError("Invalid input dataframe. It should have 'metric_id', 'date', and 'value' columns.")

        input_dfs = kwargs.get("input_dfs", [])
        if len(input_dfs) > 0:
            for df in input_dfs:
                if not all(col in df.columns for col in ["metric_id", "date", "value"]):
                    raise ValueError(
                        "Invalid input dataframe. It should have 'metric_id', 'date', and 'value' columns."
                    )
                if df["metric_id"].nunique() > 1:
                    raise ValueError("Invalid input dataframe. 'metric_id' should be unique.")
        if len(self.influencers) < 1:
            raise ValueError("Influencers list is empty. Please provide at least one influencer.")

    def integrate_yearly_seasonality(self, df: pd.DataFrame) -> pd.DataFrame:
        seasonal_df = df.copy(deep=True)
        ps = SeasonalityAnalyzer(yearly_seasonality=True, weekly_seasonality=False)

        if self.target_metric_id not in seasonal_df.columns:
            logger.error(f"Target metric '{self.target_metric_id}' not found in DataFrame")
            return seasonal_df

        temp_df = seasonal_df[["date", self.target_metric_id]].rename(columns={self.target_metric_id: "value"})
        ps.fit(temp_df)
        forecast = ps.predict(periods=0)
        yearly_effect = forecast[["ds", "yearly"]].rename(
            columns={"ds": "date", "yearly": f"yearly_{self.target_metric_id}"}
        )

        seasonal_df = seasonal_df.merge(yearly_effect, on="date", how="left")
        return seasonal_df

    def extract_columns_from_hierarchy(self, influencers):
        columns = set()
        graph_edges = []

        def traverse_hierarchy(node):
            if isinstance(node, dict) and "metric_id" in node:
                target_metric = self.normalize_name(node["metric_id"])
                columns.add(target_metric)
                if "influences" in node and isinstance(node["influences"], list):
                    for influence in node["influences"]:
                        if isinstance(influence, dict) and "metric_id" in influence:
                            source_metric = influence["metric_id"]
                            columns.add(source_metric)
                            graph_edges.append(f"{self.normalize_name(source_metric)} -> {target_metric}")
                            traverse_hierarchy(influence)
                        else:
                            print(f"Invalid influence node, expected a dictionary with 'metric_id': {influence}")
                else:
                    print(f"Node has no valid 'influences' list: {node}")
            else:
                print(f"Invalid node detected, expected a dictionary with 'metric_id': {node}")

        traverse_hierarchy({"metric_id": self.target_metric_id, "influences": influencers})

        return list(columns), graph_edges

    def add_direct_connections(self, all_columns, selected_columns, outcome_column):
        additional_edges = []
        selected_columns_set = {col for col in selected_columns}
        normalized_outcome = outcome_column

        for column in all_columns:
            if column != "date":
                normalized_column = column
                if normalized_column not in selected_columns_set:
                    additional_edges.append(f"{normalized_column} -> {normalized_outcome}")

        return additional_edges

    def get_all_metric_columns(self):
        all_columns = set()

        # Add columns from the hierarchy
        def add_columns_from_hierarchy(node):
            if isinstance(node, dict) and "metric_id" in node:
                all_columns.add(node["metric_id"])
                for influence in node.get("influences", []):
                    add_columns_from_hierarchy(influence)

        add_columns_from_hierarchy({"metric_id": self.target_metric_id, "influences": self.influencers})

        # Add columns from the actual DataFrame
        if hasattr(self, "df"):
            all_columns.update(col for col in self.df.columns if col != "date")

        return list(all_columns)

    def prepare_data(self, merged_df):
        self.create_column_mapping(merged_df.columns)
        selected_columns, graph_edges = self.extract_columns_from_hierarchy(self.influencers)
        selected_columns.append(self.target_metric_id)
        additional_edges = self.add_direct_connections(merged_df.columns, selected_columns, self.target_metric_id)
        graph_edges.extend(additional_edges)
        prepared_data = merged_df.copy()
        return prepared_data, list(prepared_data.columns), graph_edges

    def create_column_mapping(self, columns):
        self.column_mapping = {self.normalize_name(col): col for col in columns if col != "date"}
        self.reverse_column_mapping = {v: k for k, v in self.column_mapping.items()}

    def build_causal_graph(self, graph_edges):
        return "digraph { " + "; ".join(graph_edges) + " }"

    def build_hierarchy(self, metric_id):
        components = []
        if str(metric_id) in self.flipped_relation_dict:
            for child_metric in self.flipped_relation_dict[metric_id]:
                components.append(self.build_hierarchy(child_metric))

        return {
            "metric_id": metric_id,
            "model": {"coefficient": 0.0, "relative_impact": 0.0, "coefficient_root": 0.0, "relative_impact_root": 0.0},
            "components": components,
        }

    def calculate_relative_impact(self, metric_id, flipped_relation_dict, value_dict, result_output, outcome_column):
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
                    m_id, flipped_relation_dict, value_dict, result_output, outcome_column
                )
                coefficient_root = metric_info["model"]["coefficient_root"]
                relative_impact_root = (coefficient_root * value) / total_sum * 100 if total_sum != 0 else 0
                metric_info["model"]["relative_impact_root"] = relative_impact_root

        return result_output

    def calculate_total_sum(self, metric_id, flipped_relation_dict, value_dict, result_output, outcome, total_sum=0):
        for id_list in flipped_relation_dict.values():
            if metric_id in id_list:
                for child_metric_id in id_list:
                    child_metric_info = self.find_metric(result_output, child_metric_id)
                    coefficient_root = child_metric_info["model"]["coefficient_root"]
                    value = value_dict[child_metric_id]
                    total_sum += coefficient_root * value

                if self.relation_dict[metric_id][0] == outcome:
                    break
                else:
                    total_sum = self.calculate_total_sum(
                        self.relation_dict[metric_id][0],
                        flipped_relation_dict,
                        value_dict,
                        result_output,
                        outcome,
                        total_sum,
                    )
        return total_sum

    def find_metric(self, result_output, metric_id):
        if result_output["metric_id"] == metric_id:
            return result_output
        for component in result_output["components"]:
            result = self.find_metric(component, metric_id)
            if result:
                return result
        return None

    def causal_analysis(self, prepared_data: pd.DataFrame, columns: list[str], graph_definition: str) -> dict:
        try:
            data = prepared_data.copy()
            outcome_column = self.target_metric_id
            factor_columns = [col for col in columns if col != outcome_column and col != "date"]
            factor_columns.append(outcome_column)

            # Normalize column names and update graph definition
            normalized_factor_columns = {self.normalize_name(col): col for col in factor_columns}
            updated_graph_definition = self.update_graph_definition(graph_definition, normalized_factor_columns)

            # Extract relationships from the graph
            relationships = re.findall(r"(\w+)\s*->\s*(\w+)", updated_graph_definition)
            for source, target in relationships:
                if source in self.relation_dict:
                    self.relation_dict[source].append(target)
                else:
                    self.relation_dict[source] = [target]

            # this dict contains every mapping of target to all sources
            for treatment, targets in self.relation_dict.items():
                for target in targets:
                    if target in self.flipped_relation_dict:
                        self.flipped_relation_dict[target].append(treatment)
                    else:
                        self.flipped_relation_dict[target] = [treatment]

            value_dict = {col: data[col].mean() for col in data.columns if col != "date"}

            # Initialize result_output structure
            result_output = self.build_hierarchy(outcome_column)

            # Estimate causal effects for all relationships
            for target in self.flipped_relation_dict:
                treatments = self.flipped_relation_dict[target]
                model = CausalModel(data=data, treatment=treatments, outcome=target, graph=updated_graph_definition)
                identified_estimate = model.identify_effect(proceed_when_unidentifiable=True)
                if identified_estimate is None:
                    continue

                causal_estimate = model.estimate_effect(identified_estimate, method_name="backdoor.linear_regression")
                if (
                    causal_estimate is None
                    or causal_estimate.estimator is None
                    or causal_estimate.estimator.model is None
                ):
                    continue
                effect_summary = causal_estimate.estimator.model.params
                if effect_summary is None:
                    continue

                effect_names = effect_summary.index[1:]
                for treatment, effect_name in zip(treatments, effect_names):
                    coefficient = effect_summary[effect_name]
                    self.update_result_output(result_output, target, treatment, coefficient, key="coefficient")

            # Calculate indirect effects
            direct_columns, indirect_columns = self.categorize_columns(updated_graph_definition, outcome_column)

            for indirect_column in indirect_columns:
                model_in = CausalModel(
                    data=data, treatment=indirect_column, outcome=outcome_column, graph=updated_graph_definition
                )
                identified_estimate_in = model_in.identify_effect(
                    estimand_type="nonparametric-nie", proceed_when_unidentifiable=True
                )
                causal_estimate_in = model_in.estimate_effect(
                    identified_estimate_in, method_name="mediation.two_stage_regression"
                )
                coefficient_root = causal_estimate_in.value
                self.update_result_output(
                    result_output,
                    self.relation_dict[indirect_column][0],
                    indirect_column,
                    coefficient_root,
                    key="coefficient_root",
                )

            # Update coefficient_root where not calculated
            self.update_coefficient_root(data)

            # Calculate relative impacts
            updated_result_output = copy.deepcopy(result_output)
            for key in self.flipped_relation_dict:
                updated_result_output = self.calculate_relative_impact(
                    key, self.flipped_relation_dict, value_dict, updated_result_output, outcome_column
                )

            return updated_result_output

        except Exception as e:
            logger.error(f"Exception occurred in causal_analysis method: {e}")
            return {"error": str(e)}

    def update_graph_definition(self, graph_definition: str, normalized_factor_columns: dict) -> str:
        graph_edges = graph_definition.split("digraph { ")[1].strip(" }").split("; ")
        updated_edges = []
        for edge in graph_edges:
            if "->" in edge:
                src, dst = edge.split(" -> ")
                src_norm = src
                dst_norm = dst
                src_replaced = normalized_factor_columns.get(src_norm, src)
                dst_replaced = normalized_factor_columns.get(dst_norm, dst)
                updated_edges.append(f"{src_replaced} -> {dst_replaced}")
        return f"digraph {{ {'; '.join(updated_edges)} }}"

    def update_coefficient_root(self, data):
        if "model" in data:
            if data["model"]["coefficient_root"] == 0 and data["model"]["coefficient"] != 0:
                data["model"]["coefficient_root"] = data["model"]["coefficient"]
        if "components" in data:
            for component in data["components"]:
                self.update_coefficient_root(component)

    def update_result_output(self, result_output, metric_id, treatment, coefficient, key="coefficient"):
        if result_output["metric_id"] == metric_id:
            for component in result_output["components"]:
                if component["metric_id"] == treatment:
                    component["model"][key] = coefficient
                    break
        else:
            for component in result_output["components"]:
                self.update_result_output(component, metric_id, treatment, coefficient, key)

    def analyze(self, df: pd.DataFrame, *args, **kwargs) -> dict[str, Any] | pd.DataFrame:
        try:
            input_dfs = kwargs.get("input_dfs", [])
            merged_df = self.merge_dataframes([df] + input_dfs)

            merged_df = self.integrate_yearly_seasonality(merged_df)

            prepared_data, selected_columns, graph_edges = self.prepare_data(merged_df)
            graph_definition = self.build_causal_graph(graph_edges)

            result = self.causal_analysis(prepared_data, selected_columns, graph_definition)
            return result
        except Exception as e:
            logger.error(f"Exception occurred in analyze method: {e}")
            return {"error": str(e)}

    def merge_dataframes(self, dataframes: list[pd.DataFrame]) -> pd.DataFrame:
        if len(dataframes) == 1:
            df = dataframes[0]
            df.columns = df.columns.str.strip()

            if "date" not in df.columns or "metric_id" not in df.columns:
                raise ValueError("DataFrame must contain 'date' and 'metric_id' columns")

            df["date"] = pd.to_datetime(df["date"])

            df_pivoted = df.pivot(index="date", columns="metric_id", values="value").reset_index()
            df_pivoted.columns.name = None

            return df_pivoted

        dfs = []
        for df in dataframes:
            df.columns = df.columns.str.strip()

            if "date" not in df.columns or "metric_id" not in df.columns:
                raise ValueError("DataFrame must contain 'date' and 'metric_id' columns")

            df["date"] = pd.to_datetime(df["date"])

            df_pivoted = df.pivot(index="date", columns="metric_id", values="value").reset_index()
            df_pivoted.columns.name = None

            dfs.append(df_pivoted)

        common_dates = set(dfs[0]["date"])
        for df in dfs[1:]:
            common_dates &= set(df["date"])

        dfs = [df[df["date"].isin(common_dates)] for df in dfs]

        merged_df = dfs[0]
        for df in dfs[1:]:
            merged_df = merged_df.merge(df, on="date", how="outer")

        return merged_df

    def categorize_columns(self, graph_definition, outcome_column):
        relationships = re.findall(r"(\w+)\s*->\s*(\w+)", graph_definition)
        direct_columns = []
        indirect_columns = []

        for source, target in relationships:
            if target == outcome_column:
                direct_columns.append(source)
            else:
                indirect_columns.append(source)

        return direct_columns, indirect_columns

    def normalize_name(self, name):
        return name.lower().replace("_", "").replace(" ", "")
