import copy
import logging
import re
import warnings
from typing import Any

import pandas as pd
from dowhy import CausalModel

from fulcrum_core.modules import BaseAnalyzer, SeasonalityAnalyzer

warnings.filterwarnings("ignore")

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
logger = logging.getLogger(__name__)


class CausalModelAnalyzer(BaseAnalyzer):
    def __init__(self, target_metric_id: str, influencers: list[dict[str, Any]], precision: int = 3, **kwargs):
        super().__init__(precision=precision)
        self.target_metric_id = target_metric_id
        self.influencers = influencers
        self.relation_dict: dict = {}
        self.flipped_relation_dict: dict = {}

    def validate_input(self, df: pd.DataFrame, **kwargs):
        """
        Validates the input DataFrame and additional DataFrames provided in kwargs.

        This method checks if the required columns are present in the input DataFrame and any additional
        DataFrames provided in the 'input_dfs' keyword argument. It also ensures that the 'metric_id' column
        in each DataFrame contains unique values. Additionally, it verifies that the influencers list is not empty.

        Parameters:
        df (pd.DataFrame): The primary input DataFrame to validate.
        **kwargs: Additional keyword arguments that may contain 'input_dfs', a list of DataFrames to validate.

        Raises:
        ValueError: If any of the required columns are missing in the input DataFrame or any additional DataFrames.
        ValueError: If the 'metric_id' column in any DataFrame contains non-unique values.
        ValueError: If the influencers list is empty.
        """
        required_columns = ["metric_id", "date", "value"]

        # Check if the required columns are present in the primary input DataFrame
        if not set(required_columns).issubset(df.columns):
            missing_columns = set(required_columns) - set(df.columns)
            raise ValueError(f"Invalid input dataframe. Missing columns: {', '.join(missing_columns)}")

        # Retrieve additional DataFrames from kwargs
        input_dfs = kwargs.get("input_dfs", [])
        for input_df in input_dfs:
            # Check if the required columns are present in each additional DataFrame
            if not set(required_columns).issubset(input_df.columns):
                missing_columns = set(required_columns) - set(input_df.columns)
                raise ValueError(f"Invalid input dataframe. Missing columns: {', '.join(missing_columns)}")

            # Ensure that the 'metric_id' column contains unique values in each additional DataFrame
            if input_df["metric_id"].nunique() != 1:
                raise ValueError("Invalid input dataframe. 'metric_id' should be unique.")

        # Check if the influencers list is not empty
        if not self.influencers:
            raise ValueError("Influencers list is empty. Please provide at least one influencer.")

    def _integrate_yearly_seasonality(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Integrates yearly seasonality into the provided DataFrame.

        This method uses the SeasonalityAnalyzer to fit a model on the target metric's values and predict
        the yearly seasonal effect. The predicted yearly effect is then merged back into the original DataFrame.

        Parameters:
        df (pd.DataFrame): The input DataFrame containing the target metric and date columns.

        Returns:
        pd.DataFrame: A DataFrame with the yearly seasonal effect integrated.

        Notes:
        - The input DataFrame must contain the target metric column specified by self.target_metric_id.
        - The 'date' column in the input DataFrame is used for merging the seasonal effect.
        - If the target metric column is not found in the input DataFrame,
        an error is logged and the original DataFrame is returned.
        """
        # Create a deep copy of the input DataFrame to avoid modifying the original data
        seasonal_df = df.copy(deep=True)

        # Initialize the SeasonalityAnalyzer with yearly seasonality enabled and weekly seasonality disabled
        ps = SeasonalityAnalyzer(yearly_seasonality=True, weekly_seasonality=False)

        # Check if the target metric column exists in the DataFrame
        if self.target_metric_id not in seasonal_df.columns:
            logger.error(f"Target metric '{self.target_metric_id}' not found in DataFrame")
            return seasonal_df

        # Prepare a temporary DataFrame with 'date' and target metric columns, renaming the target metric to 'value'
        temp_df = seasonal_df[["date", self.target_metric_id]].rename(columns={self.target_metric_id: "value"})

        # Fit the SeasonalityAnalyzer model on the temporary DataFrame
        ps.fit(temp_df)

        # Predict the yearly seasonal effect
        forecast = ps.predict(periods=0)

        # Extract the yearly effect and rename columns for merging
        yearly_effect = forecast[["ds", "yearly"]].rename(
            columns={"ds": "date", "yearly": f"yearly_{self.target_metric_id}"}
        )

        # Merge the yearly effect back into the original DataFrame on the 'date' column
        seasonal_df = seasonal_df.merge(yearly_effect, on="date", how="left")

        return seasonal_df

    def _extract_columns_from_hierarchy(self, influencers):
        """
        Extracts columns and graph edges from the hierarchy of influencers.

        This method processes the hierarchy of influencers to extract the relevant columns and graph edges
        for building a causal graph. It starts from the target metric and traverses through its influences,
        normalizing the metric names and collecting the necessary information.

        Parameters:
        influencers (list): A list of influencer dictionaries, where each dictionary contains
        'metric_id' and 'influences'.

        Returns:
        tuple: A tuple containing a list of columns and a list of graph edges.

        Notes:
        - The 'influencers' parameter should be a list of dictionaries, each with 'metric_id'
        and optionally 'influences'.
        - The method normalizes metric names using the 'normalize_name' method.
        - The method handles cases where the input structure may be malformed or incomplete.
        """
        columns = set()
        graph_edges = []
        # Initialize the processing queue with the target metric and its influencers
        nodes_to_process = [{"metric_id": self.target_metric_id, "influences": influencers}]

        while nodes_to_process:
            node = nodes_to_process.pop(0)
            # Ensure the node is a dictionary and contains a 'metric_id'
            if not isinstance(node, dict) or "metric_id" not in node:
                continue

            # Normalize the target metric name and add it to the columns set
            target_metric = self.normalize_name(node["metric_id"])
            columns.add(target_metric)

            # Check if the node has 'influences' and if it is a list
            if "influences" not in node or not isinstance(node["influences"], list):
                continue

            for influence in node["influences"]:
                # Ensure the influence is a dictionary and contains a 'metric_id'
                if not isinstance(influence, dict) or "metric_id" not in influence:
                    continue

                # Normalize the source metric name, add it to the columns set, and create a graph edge
                source_metric = influence["metric_id"]
                columns.add(source_metric)
                graph_edges.append(f"{self.normalize_name(source_metric)} -> {target_metric}")
                # Add the influence to the processing queue
                nodes_to_process.append(influence)

        return list(columns), graph_edges

    def _add_direct_connections(self, all_columns, selected_columns):
        """
        Add direct connections from all columns to the target metric, excluding 'date' and selected columns.

        This method creates direct graph edges from each column in 'all_columns' to the target metric,
        except for the 'date' column and any columns that are in 'selected_columns'.

        Parameters:
        all_columns (list): A list of all column names.
        selected_columns (list): A list of column names that should be excluded from direct connections.

        Returns:
        list: A list of direct connection strings in the format 'source_metric -> target_metric'.
        """
        # Convert selected_columns to a set for efficient membership testing
        selected_columns_set = set(selected_columns)

        # Create direct connections for columns not in selected_columns_set and not equal to 'date'
        return [
            f"{column} -> {self.target_metric_id}"
            for column in all_columns
            if column != "date" and column not in selected_columns_set
        ]

    def _prepare_data_and_graph_edges(self, merged_df):
        """
        Prepare the data and graph edges for the causal model.

        This method extracts the selected columns and graph edges from the hierarchy of influencers,
        adds the target metric to the selected columns, and creates additional direct connections
        from all columns in the merged DataFrame to the target metric. It then prepares a copy of the
        merged DataFrame and returns it along with the list of column names and the graph edges.

        Parameters:
        merged_df (DataFrame): The merged DataFrame containing the data.

        Returns:
        tuple: A tuple containing the prepared DataFrame, a list of column names, and a list of graph edges.
        """
        # Extract selected columns and initial graph edges from the hierarchy of influencers
        selected_columns, graph_edges = self._extract_columns_from_hierarchy(self.influencers)

        # Add the target metric to the selected columns
        selected_columns.append(self.target_metric_id)

        # Create additional direct connections from all columns in the merged DataFrame to the target metric
        additional_edges = self._add_direct_connections(merged_df.columns, selected_columns)

        # Extend the initial graph edges with the additional direct connections
        graph_edges.extend(additional_edges)

        # Prepare a copy of the merged DataFrame
        prepared_data = merged_df.copy()

        # Return the prepared DataFrame, list of column names, and graph edges
        return prepared_data, list(prepared_data.columns), graph_edges

    def build_causal_graph(self, graph_edges):
        """
        Build a causal graph definition in the DOT language.

        This method takes a list of graph edges and constructs a string
        representation of a directed graph in the DOT language format.

        Parameters:
        graph_edges (list): A list of strings representing the edges of the graph.
                            Each edge should be in the format 'source_metric -> target_metric'.

        Returns:
        str: A string representing the directed graph in the DOT language format.
        """
        # Join the list of graph edges into a single string with '; ' as the separator
        edges_str = "; ".join(graph_edges)

        # Construct the final graph definition string in the DOT language format
        graph_definition = f"digraph {{ {edges_str} }}"

        return graph_definition

    def _build_hierarchy(self, metric_id):
        """
        Build a hierarchical structure of metrics starting from the given metric_id.

        This method recursively constructs a hierarchy of metrics based on the flipped_relation_dict.
        Each metric in the hierarchy is represented as a dictionary containing its metric_id, model details,
        and its child components.

        Parameters:
        metric_id (str): The ID of the metric to start building the hierarchy from.

        Returns:
        dict: A dictionary representing the hierarchical structure of the metric and its components.
        """
        components = []

        # Check if the metric_id has any child metrics in the flipped_relation_dict
        if str(metric_id) in self.flipped_relation_dict:
            # Recursively build the hierarchy for each child metric
            for child_metric in self.flipped_relation_dict[metric_id]:
                components.append(self._build_hierarchy(child_metric))

        # Return the hierarchical structure for the current metric_id
        return {
            "metric_id": metric_id,
            "model": {"coefficient": 0.0, "relative_impact": 0.0, "coefficient_root": 0.0, "relative_impact_root": 0.0},
            "components": components,
        }

    def calculate_relative_impact(self, metric_id, value_dict, result_output):
        """
        Calculate the relative impact of each child metric on the given metric_id.

        This method computes the relative impact of each child metric in the flipped_relation_dict
        on the specified metric_id. The relative impact is calculated based on the coefficient and value
        of each child metric. The results are updated in the result_output dictionary.

        Parameters:
        metric_id (str): The ID of the metric for which to calculate the relative impact.
        value_dict (dict): A dictionary containing the values of the metrics.
        result_output (dict): A dictionary representing the hierarchical structure of metrics.

        Returns:
        dict: The updated result_output dictionary with the relative impact values.
        """
        # Calculate the product of coefficients and values for each child metric
        coefficients_values = [
            (self.find_metric(result_output, m_id)["model"]["coefficient"], value_dict[m_id])
            for m_id in self.flipped_relation_dict[metric_id]
        ]

        # Sum of the products of coefficients and values
        sum_coefficients_values = sum(c * v for c, v in coefficients_values)

        # Iterate through each child metric to calculate and update relative impact
        for m_id in self.flipped_relation_dict[metric_id]:
            metric_info = self.find_metric(result_output, m_id)
            coefficient = metric_info["model"]["coefficient"]
            value = value_dict[m_id]
            # Calculate relative impact as a percentage
            relative_impact = (coefficient * value) / sum_coefficients_values if sum_coefficients_values != 0 else 0
            metric_info["model"]["relative_impact"] = relative_impact * 100

            # Check if the coefficient is equal to the root coefficient
            if metric_info["model"]["coefficient"] == metric_info["model"]["coefficient_root"]:
                metric_info["model"]["relative_impact_root"] = relative_impact * 100
            else:
                # Calculate the total sum for the root relative impact calculation
                total_sum = self.calculate_total_sum(m_id, value_dict, result_output)
                coefficient_root = metric_info["model"]["coefficient_root"]
                # Calculate root relative impact as a percentage
                relative_impact_root = (coefficient_root * value) / total_sum * 100 if total_sum != 0 else 0
                metric_info["model"]["relative_impact_root"] = relative_impact_root

        return result_output

    def calculate_total_sum(self, metric_id, value_dict, result_output, total_sum=0):
        """
        Calculate the total sum of the product of root coefficients and values for a given metric and its related
        metrics.

        This method recursively traverses the flipped_relation_dict to find all related child metrics for the given
        metric_id. It calculates the total sum of the product of root coefficients and values for these metrics.

        Parameters:
        metric_id (str): The ID of the metric for which to calculate the total sum.
        value_dict (dict): A dictionary containing the values of the metrics.
        result_output (dict): A dictionary representing the hierarchical structure of metrics.
        total_sum (float, optional): The initial total sum value. Defaults to 0.

        Returns: float: The total sum of the product of root coefficients and values for the given metric and its
        related metrics.
        """
        # Iterate through each list of child metric IDs in the flipped_relation_dict
        for id_list in self.flipped_relation_dict.values():
            # Check if the given metric_id is in the current list of child metric IDs
            if metric_id in id_list:
                # Iterate through each child metric ID in the current list
                for child_metric_id in id_list:
                    # Find the metric information for the child metric ID
                    child_metric_info = self.find_metric(result_output, child_metric_id)
                    # Get the root coefficient for the child metric
                    coefficient_root = child_metric_info["model"]["coefficient_root"]
                    # Get the value for the child metric from the value_dict
                    value = value_dict[child_metric_id]
                    # Add the product of the root coefficient and value to the total sum
                    total_sum += coefficient_root * value

                # Check if the parent of the current metric is the target metric
                if self.relation_dict[metric_id][0] == self.target_metric_id:
                    # If the parent is the target metric, break the loop
                    break
                else:
                    # If the parent is not the target metric, recursively calculate the total sum for the parent metric
                    total_sum = self.calculate_total_sum(
                        self.relation_dict[metric_id][0],
                        value_dict,
                        result_output,
                        total_sum,
                    )
        # Return the calculated total sum
        return total_sum

    def find_metric(self, result_output, metric_id):
        """
        Recursively search for a metric within the hierarchical result_output structure.

        This method traverses the result_output structure to find a metric with the specified metric_id.
        It performs a depth-first search to locate the metric within the nested components.

        Parameters:
        result_output (dict): A dictionary representing the hierarchical structure of metrics.
        metric_id (str): The ID of the metric to find.

        Returns:
        dict or None: The dictionary containing the metric information if found, otherwise None.
        """
        # Check if the current result_output's metric_id matches the target metric_id
        if result_output["metric_id"] != metric_id:
            # Iterate through each component in the current result_output's components
            for component in result_output["components"]:
                # Recursively search for the metric in the component
                result = self.find_metric(component, metric_id)
                # If the metric is found, return the result
                if result is not None:
                    return result
            # If the metric is not found in any components, return None
            return None
        # If the current result_output's metric_id matches the target metric_id, return the result_output
        return result_output

    def _update_relationships(self, updated_graph_definition):
        """
        Update the relationships based on the provided graph definition.

        This method parses the updated graph definition to extract relationships between metrics.
        It updates the relation_dict and flipped_relation_dict with the source and target relationships.

        Parameters: updated_graph_definition (str): A string representing the updated graph definition in the format
        "source -> target".

        Returns:
        None
        """
        # Use regular expression to find all relationships in the format "source -> target"
        relationships = re.findall(r"(\w+)\s*->\s*(\w+)", updated_graph_definition)

        # Iterate through each relationship and update the dictionaries
        for source, target in relationships:
            # Add the target to the list of children for the source in relation_dict
            self.relation_dict.setdefault(source, []).append(target)
            # Add the source to the list of parents for the target in flipped_relation_dict
            self.flipped_relation_dict.setdefault(target, []).append(source)

    def causal_analysis(self, prepared_data: pd.DataFrame, columns: list[str], graph_definition: str) -> dict:
        """
        Perform causal analysis on the prepared data using the provided graph definition.

        This method estimates causal effects between metrics based on the relationships defined in the graph.
        It updates the result_output structure with the estimated coefficients and calculates indirect effects.

        Parameters:
        prepared_data (pd.DataFrame): The preprocessed data for causal analysis.
        columns (list[str]): A list of column names to be considered in the analysis.
        graph_definition (str): A string representing the graph definition in the format "source -> target".

        Returns:
        dict: A dictionary containing the updated result_output structure with causal effect estimates.
        """
        try:
            # Create a copy of the prepared data to avoid modifying the original data
            data = prepared_data.copy()

            # Identify factor columns excluding the target metric and date columns
            factor_columns = [col for col in columns if col != self.target_metric_id and col != "date"]
            factor_columns.append(self.target_metric_id)

            # Normalize column names and update the graph definition accordingly
            normalized_factor_columns = {self.normalize_name(col): col for col in factor_columns}
            updated_graph_definition = self._update_graph_definition(graph_definition, normalized_factor_columns)

            # Extract relationships from the updated graph definition
            self._update_relationships(updated_graph_definition)

            # Calculate the mean value for each column except the date column
            value_dict = {col: data[col].mean() for col in data.columns if col != "date"}

            # Initialize the hierarchical result_output structure
            result_output = self._build_hierarchy(self.target_metric_id)

            # Estimate causal effects for all relationships defined in the graph
            for target, treatments in self.flipped_relation_dict.items():
                # Create a CausalModel for the current target and treatments
                model = CausalModel(data=data, treatment=treatments, outcome=target, graph=updated_graph_definition)

                # Identify the causal effect using the backdoor criterion
                identified_estimate = model.identify_effect(proceed_when_unidentifiable=True)
                if identified_estimate is None:
                    continue

                # Estimate the causal effect using linear regression
                causal_estimate = model.estimate_effect(identified_estimate, method_name="backdoor.linear_regression")
                if not causal_estimate or not causal_estimate.estimator or not causal_estimate.estimator.model:
                    continue

                # Extract the effect summary (coefficients) from the model
                effect_summary = causal_estimate.estimator.model.params
                if effect_summary is None:
                    continue

                # Update the result_output structure with the estimated coefficients
                effect_names = effect_summary.index[1:]
                for treatment, effect_name in zip(treatments, effect_names):
                    coefficient = effect_summary[effect_name]
                    self._update_result_output(result_output, target, treatment, coefficient, key="coefficient")

            # Calculate indirect effects for columns with indirect relationships
            indirect_columns = self.get_indirect_columns(updated_graph_definition)

            for indirect_column in indirect_columns:
                # Create a CausalModel for the indirect effect
                model_in = CausalModel(
                    data=data, treatment=indirect_column, outcome=self.target_metric_id, graph=updated_graph_definition
                )

                # Identify the nonparametric natural indirect effect
                identified_estimate_in = model_in.identify_effect(
                    estimand_type="nonparametric-nie", proceed_when_unidentifiable=True
                )

                # Estimate the indirect effect using two-stage regression
                causal_estimate_in = model_in.estimate_effect(
                    identified_estimate_in, method_name="mediation.two_stage_regression"
                )

                # Extract the root coefficient of the indirect effect
                coefficient_root = causal_estimate_in.value

                # Update the result_output structure with the indirect effect coefficient
                self._update_result_output(
                    result_output,
                    self.relation_dict[indirect_column][0],
                    indirect_column,
                    coefficient_root,
                    key="coefficient_root",
                )

            # Update coefficient_root values where not calculated
            self._update_coefficient_root(data)

            # Calculate relative impacts for all relationships
            updated_result_output = copy.deepcopy(result_output)
            for key in self.flipped_relation_dict:
                updated_result_output = self.calculate_relative_impact(key, value_dict, updated_result_output)

            return updated_result_output

        except Exception as e:
            # Log the exception and return an error message
            logger.error(f"Exception occurred in causal_analysis method: {e}")
            return {"error": str(e)}

    def _update_graph_definition(self, graph_definition: str, normalized_factor_columns: dict) -> str:
        """
        Updates the graph definition by replacing the source and destination nodes with their normalized counterparts.

        Args:
            graph_definition (str): The original graph definition in DOT format.
            normalized_factor_columns (dict): A dictionary mapping original node names to their normalized names.

        Returns:
            str: The updated graph definition with normalized node names.
        """
        # Extract the edges from the graph definition
        graph_edges = graph_definition.split("digraph { ")[1].strip(" }").split("; ")

        # Update the edges with normalized node names
        updated_edges = [
            f"{normalized_factor_columns.get(src, src)} -> {normalized_factor_columns.get(dst, dst)}"
            for edge in graph_edges
            if "->" in edge  # Ensure the edge contains a relationship
            for src, dst in [edge.split(" -> ")]  # Split the edge into source and destination nodes
        ]

        # Return the updated graph definition in DOT format
        return f"digraph {{ {'; '.join(updated_edges)} }}"

    def _update_coefficient_root(self, data):
        """
        Recursively updates the 'coefficient_root' value in the data dictionary.

        This method checks if the 'coefficient_root' value in the 'model' dictionary is zero and if the 'coefficient'
        value is non-zero. If both conditions are met, it sets the 'coefficient_root' value to the 'coefficient'
        value. The method then recursively applies the same logic to any nested 'components' dictionaries within the
        data.

        Args:
            data (dict): The data dictionary containing 'model' and 'components' keys.

        Returns:
            None
        """
        # Check if 'model' key exists in the data dictionary
        if "model" in data:
            # If 'coefficient_root' is zero and 'coefficient' is non-zero, update 'coefficient_root'
            if data["model"]["coefficient_root"] == 0 and data["model"]["coefficient"] != 0:
                data["model"]["coefficient_root"] = data["model"]["coefficient"]

        # Check if 'components' key exists in the data dictionary
        if "components" in data:
            # Recursively update 'coefficient_root' for each component in 'components'
            for component in data["components"]:
                self._update_coefficient_root(component)

    def _update_result_output(self, result_output, metric_id, treatment, coefficient, key="coefficient"):
        """
        Recursively updates the specified key in the 'model' dictionary of the result output components.

        This method traverses the nested structure of the result output to find the component with the matching
        metric_id. If a match is found, it updates the specified key in the 'model' dictionary with the given
        coefficient value. The method continues to search recursively through the 'components' of each result output.

        Args:
            result_output (dict): The result output dictionary containing 'metric_id' and 'components' keys.
            metric_id (str): The metric ID to match in the result output.
            treatment (str): The treatment metric ID to match in the components.
            coefficient (float): The coefficient value to update in the 'model' dictionary.
            key (str, optional): The key in the 'model' dictionary to update. Defaults to "coefficient".

        Returns:
            None
        """
        # Check if the metric_id of the result_output matches the given metric_id
        if result_output["metric_id"] == metric_id:
            # Iterate through the components of the result_output
            for component in result_output["components"]:
                # Check if the metric_id of the component matches the treatment
                if component["metric_id"] == treatment:
                    # Update the specified key in the 'model' dictionary with the given coefficient
                    component["model"][key] = coefficient
                    break
        else:
            # Recursively update the components of the result_output
            for component in result_output["components"]:
                self._update_result_output(component, metric_id, treatment, coefficient, key)

    def analyze(self, df: pd.DataFrame, *args, **kwargs) -> dict[str, Any] | pd.DataFrame:
        """
        Analyzes the given dataframe by merging it with additional input dataframes, integrating yearly seasonality,
        preparing data and graph edges, building a causal graph, and performing causal analysis.

        Args:
            df (pd.DataFrame): The primary dataframe to be analyzed.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments, including:
                - input_dfs (list[pd.DataFrame]): List of additional dataframes to be merged with the primary dataframe.

        Returns:
            dict[str, Any] | pd.DataFrame: The result of the causal analysis, either as a dictionary or a dataframe.
        """
        try:
            # Retrieve additional input dataframes from keyword arguments
            input_dfs = kwargs.get("input_dfs", [])

            # Merge the primary dataframe with additional input dataframes
            merged_df = self.merge_dataframes([df] + input_dfs)

            # Integrate yearly seasonality into the merged dataframe
            merged_df = self._integrate_yearly_seasonality(merged_df)

            # Prepare data and graph edges for causal analysis
            prepared_data, selected_columns, graph_edges = self._prepare_data_and_graph_edges(merged_df)

            # Build the causal graph from the graph edges
            graph_definition = self.build_causal_graph(graph_edges)

            # Perform causal analysis using the prepared data, selected columns, and graph definition
            result = self.causal_analysis(prepared_data, selected_columns, graph_definition)

            # Return the result of the causal analysis
            return result
        except Exception as e:
            # Log any exceptions that occur during the analysis process
            logger.error(f"Exception occurred in analyze method: {e}")

            # Return an error message in case of an exception
            return {"error": str(e)}

    def merge_dataframes(self, dataframes: list[pd.DataFrame]) -> pd.DataFrame:
        """
        Merges a list of dataframes on the 'date' column. Each dataframe is expected to have 'date', 'metric_id',
        and 'value' columns. The function performs the following steps: 1. Strips whitespace from column names. 2.
        Converts the 'date' column to datetime format. 3. Pivots each dataframe to have 'date' as the index and
        'metric_id' values as columns. 4. Merges all dataframes on the 'date' column, keeping only the common dates.

        Args:
            dataframes (list[pd.DataFrame]): List of dataframes to be merged.

        Returns:
            pd.DataFrame: A single merged dataframe.
        """
        processed_dfs = []

        for df in dataframes:
            df = df.copy()  # Create a copy of the dataframe to avoid modifying the original
            df.columns = df.columns.str.strip()  # Strip whitespace from column names
            df["date"] = pd.to_datetime(df["date"])  # Convert 'date' column to datetime format
            # Pivot the dataframe to have 'date' as the index and 'metric_id' values as columns
            df_pivoted = df.pivot(index="date", columns="metric_id", values="value").reset_index()
            df_pivoted.columns.name = None  # Remove the name of the columns index
            processed_dfs.append(df_pivoted)  # Add the processed dataframe to the list

        if len(processed_dfs) == 1:
            return processed_dfs[0]  # If there's only one dataframe, return it as is

        # Find common dates across all dataframes
        common_dates = set.intersection(*[set(df["date"]) for df in processed_dfs])
        # Filter each dataframe to keep only the common dates
        filtered_dfs = [df[df["date"].isin(common_dates)] for df in processed_dfs]

        merged_df = filtered_dfs[0]  # Start with the first filtered dataframe
        for df in filtered_dfs[1:]:
            # Merge the dataframes on the 'date' column, using an outer join
            merged_df = merged_df.merge(df, on="date", how="outer")

        return merged_df  # Return the final merged dataframe

    def get_indirect_columns(self, graph_definition):
        return [
            source
            for source, target in re.findall(r"(\w+)\s*->\s*(\w+)", graph_definition)
            if target != self.target_metric_id
        ]

    def normalize_name(self, name):
        return name.lower().replace("_", "").replace(" ", "")
