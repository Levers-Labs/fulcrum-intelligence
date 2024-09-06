import pandas as pd
import re
from dowhy import CausalModel
from prophet_seasonality import ProphetSeasonality
import warnings

warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

class CausalInference:
    def __init__(self, dataframes, hierarchy_list):
        if len(dataframes) < 1:
            raise ValueError("At least one dataframe must be provided.")
        self.dataframes = dataframes
        self.hierarchy_list = hierarchy_list

    def normalize_name(self, name):
        return name.replace('_', '').lower()

    def integrate_yearly_seasonality(self, df):
        ps = ProphetSeasonality(yearly_seasonality=True, weekly_seasonality=False)
        seasonal_effects = pd.DataFrame({'ds': df['ds']})
        col = df.columns[-1]
        temp_df = df[['ds', col]].rename(columns={'ds': 'date', col: 'value'})
        ps.fit(temp_df)
        forecast = ps.predict(periods=0)
        yearly_effect = forecast[['ds', 'yearly']].rename(columns={'yearly': f'yearly_{col}'})
        seasonal_effects = seasonal_effects.merge(yearly_effect, on='ds')
        final_df = pd.concat([df.drop(columns=[col]), seasonal_effects.drop(columns=['ds']), df[df.columns[-1]]], axis=1)
        return final_df

    def merge_dataframes(self):
        if len(self.dataframes) == 1:
            df = self.dataframes[0]
            df.columns = df.columns.str.strip()
            df['ds'] = pd.to_datetime(df.iloc[:, 0])
            df.drop(columns=[df.columns[0]], inplace=True)
            df = df[['ds'] + list(df.columns[0:-1])]
            df = self.integrate_yearly_seasonality(df)
            return df

        dfs = []
        for df in self.dataframes:
            df.columns = df.columns.str.strip()
            df['ds'] = pd.to_datetime(df.iloc[:, 0])
            df.drop(columns=[df.columns[0]], inplace=True)
            df = df[['ds'] + [col for col in df.columns if col != 'ds']]
            dfs.append(df)

        common_dates = dfs[0]['ds']
        for df in dfs[1:]:
            common_dates = common_dates[common_dates.isin(df['ds'])]

        dfs = [df[df['ds'].isin(common_dates)] for df in dfs]
        merged_df = dfs[0]
        for df in dfs[1:]:
            merged_df = merged_df.merge(df, on='ds')
        merged_df = self.integrate_yearly_seasonality(merged_df)
        return merged_df

    def prepare_data(self, merged_df):
        outcome_column = merged_df.columns[-1]
        selected_columns, graph_edges = self.extract_columns_from_hierarchy(self.hierarchy_list)
        selected_columns.append(outcome_column)
        additional_edges = self.add_direct_connections(merged_df.columns, selected_columns, outcome_column)
        graph_edges.extend(additional_edges)
        return merged_df, selected_columns, graph_edges

    def extract_columns_from_hierarchy(self, hierarchy_list):
        columns = set()
        graph_edges = []

        def traverse_hierarchy(node):
            if isinstance(node, dict):
                if 'metric_id' in node:
                    target_metric = self.normalize_name(node['metric_id'])
                    columns.add(target_metric)
                    if 'influences' in node and isinstance(node['influences'], list):
                        for influence in node['influences']:
                            if isinstance(influence, dict) and 'metric_id' in influence:
                                source_metric = self.normalize_name(influence['metric_id'])
                                columns.add(source_metric)
                                graph_edges.append(f"{source_metric} -> {target_metric}")
                                traverse_hierarchy(influence)
                            else:
                                print(f"Invalid influence node, expected a dictionary with 'metric_id': {influence}")
                    else:
                        print(f"Node has no valid 'influences' list: {node}")
                else:
                    print(f"Invalid node detected, missing 'metric_id': {node}")
            else:
                print(f"Invalid node detected, expected a dictionary but got: {type(node)} - {node}")

        for node in hierarchy_list:
            traverse_hierarchy(node)

        return list(columns), graph_edges

    def add_direct_connections(self, all_columns, selected_columns, outcome_column):
        additional_edges = []
        selected_columns_set = {self.normalize_name(col) for col in selected_columns}
        normalized_outcome = self.normalize_name(outcome_column)

        for column in all_columns:
            normalized_column = self.normalize_name(column)
            if normalized_column != 'ds' and normalized_column not in selected_columns_set:
                additional_edges.append(f"{column} -> {outcome_column}")

        return additional_edges

    def build_causal_graph(self, graph_edges):
        graph_definition = f"digraph {{ {'; '.join(graph_edges)}; }}"
        return graph_definition

    def parse_graph_definition(self, graph_definition):
        edges = []
        graph_definition = graph_definition.strip().replace('digraph {', '').replace('}', '')
        for line in graph_definition.strip().split(";"):
            line = line.strip()
            if "->" in line:
                parts = line.split("->")
                source = parts[0].strip()
                target = parts[1].strip()
                edges.append((source, target))
        return edges

    def categorize_columns(self, graph_definition, outcome):
        edges = self.parse_graph_definition(graph_definition)
        direct_columns = set()
        indirect_columns = set()
        all_columns = set()

        from collections import defaultdict, deque

        graph = defaultdict(list)
        for source, target in edges:
            graph[source].append(target)
            all_columns.add(source)
            all_columns.add(target)

        direct_columns = {source for source, target in edges if target == outcome}
        indirect_columns = {source for source, target in edges if target != outcome}

        return list(direct_columns), list(indirect_columns)

    def effect_to_percentage(self, effect, total_absolute_effect):
        return (abs(effect) / total_absolute_effect) * 100

    def normalize_effects_row(self, row, effects):
        total_absolute_effect = sum(abs(row[effect]) for effect in effects)
        if total_absolute_effect == 0:
            return {effect: 0 for effect in effects}
        return {effect: (abs(row[effect]) / total_absolute_effect) * 100 for effect in effects}

    def causal_analysis(self, data, selected_columns, graph_definition):
        try:
            factor_columns = [col for col in data.columns if col not in ['ds', data.columns[-1]]]
            outcome_column = data.columns[-1]
            factor_columns.append(outcome_column)
            normalized_factor_columns = {self.normalize_name(col): col for col in factor_columns}
            graph_edges = graph_definition.split('digraph { ')[1].strip(' }').split('; ')
            updated_edges = []
            for edge in graph_edges:
                if '->' in edge:
                    src, dst = edge.split(' -> ')
                    src_norm = self.normalize_name(src)
                    dst_norm = self.normalize_name(dst)
                    src_replaced = normalized_factor_columns.get(src_norm, src)
                    dst_replaced = normalized_factor_columns.get(dst_norm, dst)
                    updated_edges.append(f"{src_replaced} -> {dst_replaced}")

            updated_graph_definition = f"digraph {{ {'; '.join(updated_edges)} }}"

            # using graph dag find relationships 
            relationships = re.findall(r'(\w+)\s*->\s*(\w+)', updated_graph_definition)

            nodes = sorted(set([node for relation in relationships for node in relation]))

            # make initial result dataframe matrix with all values 0
            result = pd.DataFrame(0.0, index=nodes, columns=nodes)

            # make relation dict to know relations of every source and target
            relation_dict = {}
            for source, target in relationships:
                if source in relation_dict:
                    relation_dict[source].append(target)
                else:
                    relation_dict[source] = [target]

            # this dict contains every mapping of target to all sources
            flipped_relation_dict = {}

            for treatment, targets in relation_dict.items():
                for target in targets:
                    if target in flipped_relation_dict:
                        flipped_relation_dict[target].append(treatment)
                    else:
                        flipped_relation_dict[target] = [treatment]

            target_columns = list(flipped_relation_dict.keys())

            # for all target columns in hierarchy run causal model and store coefficient got in result
            for outcome_column1 in target_columns:
                treatment_columns = flipped_relation_dict[outcome_column1]
                model1 = CausalModel(
                    data=data,
                    treatment=treatment_columns,
                    outcome=outcome_column1,
                    graph=updated_graph_definition
                )
                identified_estimand1 = model1.identify_effect(proceed_when_unidentifiable=True)
                causal_estimate1 = model1.estimate_effect(identified_estimand1, method_name="backdoor.linear_regression")
                effect_summary1 = causal_estimate1.estimator.model.params
                effect_names1 = effect_summary1.index[1:]
                for treatment1, effect_name in zip(treatment_columns, effect_names1):
                    result.loc[treatment1, outcome_column1] = effect_summary1[effect_name]

            # Find direct and indirect columns
            direct_columns, indirect_columns = self.categorize_columns(updated_graph_definition, outcome_column)
            direct_columns = sorted(direct_columns, key=lambda x: factor_columns.index(x))
            indirect_columns = sorted(indirect_columns, key=lambda x: factor_columns.index(x))

            # for every indirect column find indirect coefficient which is stored under indirect_column,new_biz_deals/target
            for i in range(len(indirect_columns)):
                model_in = CausalModel(
                    data=data,
                    treatment=indirect_columns[i],
                    outcome=outcome_column,
                    graph=updated_graph_definition
                )
                identified_estimand_in = model_in.identify_effect(estimand_type='nonparametric-nie',proceed_when_unidentifiable=True)
                causal_estimate_in = model_in.estimate_effect(identified_estimand_in, method_name="mediation.two_stage_regression")
                result.loc[indirect_columns[i], outcome_column] = causal_estimate_in.value
            
            return result, updated_graph_definition
        except Exception as e:
            print(f"Exception occurred: {e}")
            return None, None

    def calculate_percentage_impact(self, coefficients, outcome_column):
        outcome_coefficients = coefficients[outcome_column]
        total_absolute_effect = outcome_coefficients.abs().sum()
        percentage_impacts = (outcome_coefficients.abs() / total_absolute_effect * 100).round(2)
        percentage_impacts_df = pd.DataFrame({
            'Column': outcome_coefficients.index,
            'Percentage Impact': percentage_impacts
        }).reset_index(drop=True)
        return percentage_impacts_df

    def run(self):
        merged_df = self.merge_dataframes()
        if merged_df.empty:
            print("Merged DataFrame is empty.")
            return None

        prepared_data, selected_columns, graph_edges = self.prepare_data(merged_df)
        
        if not graph_edges:
            print("Graph edges are empty. Check the hierarchy JSON and column mappings.")
            return None

        graph_definition = self.build_causal_graph(graph_edges)
        contributions, graph = self.causal_analysis(prepared_data, selected_columns, graph_definition)
        
        if contributions is not None:
            outcome_column = prepared_data.columns[-1]
            percentage_impacts = self.calculate_percentage_impact(contributions, outcome_column)
            return contributions, percentage_impacts
        else:
            print("Causal analysis failed. Please check logs for details.")
            return None


if __name__ == "__main__":
    hierarchy_list = [
      {
        "metric_id": "NewBizDeals",
        "influences": [
          {
            "metric_id": "AcceptOpps",
            "influences": [
              {
                "metric_id": "OpenNewBizOpps",
                "influences": []
              },
              {
                "metric_id": "SQORate",
                "influences": []
              }
            ]
          },
          {
            "metric_id": "SQOToWinRate",
            "influences": []
          }
        ]
      }
    ]

    merged_output = pd.read_csv(r"C:\Users\anubhav\Desktop\leverslabs\data_model\merged_output.csv")
    hierarchy_output = pd.read_csv(r"C:\Users\anubhav\Desktop\leverslabs\data_24_04\merged_output_hierarchy_all.csv", usecols=['date', 'open_new_biz_opps', 'sqo_rate', 'sqo_to_win_rate'])

    dataframes = [hierarchy_output, merged_output]
    causal_analyzer = CausalInference(dataframes=dataframes, hierarchy_list=hierarchy_list)
    coefficients, percentage_impacts = causal_analyzer.run()

    if coefficients is not None:
        print("Coefficients data:\n")
        print(coefficients.head(13))
        print("\nPercentage impacts with column names:\n")
        print(percentage_impacts)
    else:
        print("Causal analysis failed. Please check logs for details.")
