# for both direct and indirect combined
import pandas as pd
import json
from dowhy import CausalModel
import warnings

warnings.filterwarnings('ignore')

pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)

class CausalInference:
    def __init__(self, dataframes, hierarchy_json):
        if len(dataframes) < 1:
            raise ValueError("At least one dataframe must be provided.")
        self.dataframes = dataframes
        self.hierarchy_json = hierarchy_json

    def normalize_name(self, name):
        return name.replace('_', '').lower()

    def merge_dataframes(self):
        if len(self.dataframes) == 1:
            df = self.dataframes[0]
            df.columns = df.columns.str.strip()
            df['ds'] = pd.to_datetime(df.iloc[:, 0])
            df.drop(columns=[df.columns[0]], inplace=True)
            df = df[['ds'] + list(df.columns[0:-1])]
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

        return merged_df

    def prepare_data(self, merged_df):
        outcome_column = merged_df.columns[-1]  # Last column is the outcome
        selected_columns, graph_edges = self.extract_columns_from_hierarchy(self.hierarchy_json)
        selected_columns.append(outcome_column)  # Include outcome column

        additional_edges = self.add_direct_connections(merged_df.columns, selected_columns, outcome_column)
        graph_edges.extend(additional_edges)

        return merged_df, selected_columns, graph_edges

    def extract_columns_from_hierarchy(self, hierarchy):
        columns = set()
        graph_edges = []

        def traverse_hierarchy(node):
            if 'expression' in node:
                expression = node['expression']
                if expression['type'] == 'expression':
                    target_metric = self.normalize_name(node['metric_id'])
                    for operand in expression['operands']:
                        if operand['type'] == 'metric':
                            source_metric = self.normalize_name(operand['metric_id'])
                            columns.add(source_metric)
                            graph_edges.append(f"{source_metric} -> {target_metric}")
                        if 'expression' in operand and operand['expression'] is not None:
                            traverse_hierarchy(operand)

            else:
                columns.add(self.normalize_name(node['metric_id']))

        traverse_hierarchy(hierarchy)
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
    

    def parse_graph_definition(self,graph_definition):
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

    def categorize_columns(self,graph_definition,outcome):
        edges = self.parse_graph_definition(graph_definition)

        direct_columns = set()
        indirect_columns = set()
        all_columns = set()

        # Build adjacency list for the graph
        from collections import defaultdict, deque

        graph = defaultdict(list)
        for source, target in edges:
            graph[source].append(target)
            all_columns.add(source)
            all_columns.add(target)

        # Identify direct columns
        direct_columns = {source for source, target in edges if target == outcome}
        indirect_columns = {source for source, target in edges if target != outcome}

        return list(direct_columns), list(indirect_columns)
    
    def effect_to_percentage(self,effect, total_absolute_effect):
        return (abs(effect) / total_absolute_effect) * 100
    
    def normalize_effects_row(self,row, effects):
        total_absolute_effect = sum(abs(row[effect]) for effect in effects)
        if total_absolute_effect == 0:
            return {effect: 0 for effect in effects}  # Handle cases where total absolute effect is zero
        return {effect: (abs(row[effect]) / total_absolute_effect) * 100 for effect in effects}

    def causal_analysis(self, data, selected_columns, graph_definition):
        try:
            #Getting factor and outcome columns and updating graph edges with original column names
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
            
            # model definintion containing data, treatment variables which are factor columns, outcome and digraph
            model = CausalModel(
                data=data,
                treatment=[col for col in factor_columns if col != outcome_column],  # Exclude the outcome column from treatment
                outcome = outcome_column,
                graph=updated_graph_definition
            )

            # Identifies effect and estimates coefficients 
            identified_estimand = model.identify_effect(proceed_when_unidentifiable=True)
            causal_estimate = model.estimate_effect(identified_estimand, method_name="backdoor.linear_regression")
            effect = causal_estimate.realized_estimand_expr
            effect_summary = causal_estimate.estimator.model.params
            factor_columns.pop(-1)  # remove outcome column from factor columns

            #Coefficients x1, x2 etc. are changed to original column names dynamically
            factor_dict = {f'x{i+1}': factor_columns[i] for i in range(len(factor_columns))}

            # Calculating direct effects of all columns to outcome
            direct_effects = {}
            for generic_name, original_name in factor_dict.items():
                if generic_name in effect_summary.index:
                    direct_effects[original_name] = effect_summary[generic_name]
                else:
                    print(f"Warning: Coefficient for {original_name} (mapped from {generic_name}) not found in the model.")
            
            # Seperating direct impact and indirect impact columns
            direct_columns, indirect_columns = self.categorize_columns(updated_graph_definition, outcome_column)
            direct_columns = sorted(direct_columns, key=lambda x: factor_columns.index(x))
            indirect_columns = sorted(indirect_columns, key=lambda x: factor_columns.index(x))            
            
            # calculating Percantage effect for each column 
            total_absolute_effect = sum(abs(direct_effects[variable]) for variable in factor_columns)
            
            normalized_percentage_impact = {
            variable: self.effect_to_percentage(direct_effects[variable], total_absolute_effect)
            for variable in factor_columns
            }
            # for variable, percentage in normalized_percentage_impact.items():
            #     print(f"Normalized overall effect of '{variable}' on 'Y' (%):", percentage)

            normalized_row_effects = data.apply(lambda row: self.normalize_effects_row(row, factor_columns), axis=1)
            
            # Adding columns coefficient and percantage for each column
            for effect in factor_columns:
                data['coefficient_'+effect] = direct_effects[effect]

            for effect in factor_columns:
                data[effect + '_percentage'] = normalized_row_effects.apply(lambda x: x[effect])

            natural_effect = pd.DataFrame()

            # Calculating direct and indirect impacts of columns which have indirect impact on outcome
            for i in range(len(indirect_columns)):
                model_in = CausalModel(
                data=data,
                treatment=indirect_columns[i],  # Exclude the outcome column from treatment
                outcome = outcome_column,
                graph=updated_graph_definition
                )
                identified_estimand_nde = model_in.identify_effect(estimand_type="nonparametric-nde", proceed_when_unidentifiable=True)
                identified_estimand_nie = model_in.identify_effect(estimand_type="nonparametric-nie", proceed_when_unidentifiable=True)
                estimate_nde = model_in.estimate_effect(identified_estimand_nde, method_name="mediation.two_stage_regression")
                estimate_nie = model_in.estimate_effect(identified_estimand_nie, method_name="mediation.two_stage_regression")

                natural_effect[f'coefficient_NDE_{indirect_columns[i]}'] = [estimate_nde.value]
                natural_effect[f'coefficient_NIE_{indirect_columns[i]}'] = [estimate_nie.value]

                total_effect = abs(estimate_nde.value) + abs(estimate_nie.value)

                if total_effect != 0:
                    natural_effect[f'NDE_{indirect_columns[i]}_percentage'] = [(abs(estimate_nde.value) / total_effect) * 100]
                    natural_effect[f'NIE_{indirect_columns[i]}_percentage'] = [(abs(estimate_nie.value) / total_effect) * 100]
                else:
                    natural_effect[f'NDE_{indirect_columns[i]}_percentage'] = [0]
                    natural_effect[f'NIE_{indirect_columns[i]}_percentage'] = [0]
            
            # Dropping original columns
            data = data.drop(factor_columns,axis=1)
            data = data.drop(outcome_column,axis=1)
            return data,natural_effect

        except Exception as e:
            print(f"An error occurred during causal analysis: {str(e)}")
            return None

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
        contributions,natural_effect = self.causal_analysis(prepared_data, selected_columns, graph_definition)
        return contributions,natural_effect


# Required hierarchy_json which has connection data and a list or a single dataframe as input
if __name__ == "__main__":
    with open(r'C:\Users\anubhav\Desktop\leverslabs\data_24_04\new_biz_deal_metric_expression.json') as f:
        hierarchy_json = json.load(f)

    seasonal_factors = pd.read_csv(r"seasonal_factors.csv", usecols=['date', 'yearly'])
    merged_output = pd.read_csv(r"C:\Users\anubhav\Desktop\leverslabs\data_model\merged_output.csv")
    hierarchy_output = pd.read_csv(r"C:\Users\anubhav\Desktop\leverslabs\data_24_04\merged_output_hierarchy_all.csv", usecols=['date', 'open_new_biz_opps', 'sqo_rate', 'sqo_to_win_rate'])

    # Last dataframe should be one containing output column
    dataframes = [seasonal_factors, hierarchy_output, merged_output]  #you can pass any number(>0) of dataframes in this list 

    causal_analyzer = CausalInference(dataframes=dataframes, hierarchy_json=hierarchy_json)
    contributions,natural_effect = causal_analyzer.run()

    if contributions is not None:
        print("Contributions data:\n")
        print(contributions.head(5))
    else:
        print("Causal analysis failed. Please check logs for details.")

    if natural_effect is not None:
        print("Natural Effect data:\n")
        print(natural_effect.head(5))
    else:
        print("Causal analysis failed. Please check logs for details.")

    
"""
Input: List of dataframes (Inside list no. of dfs > 0) & Hierarchy json(containing connection metadata)
NOTE: For column names that are not present in json but present in dataframe, this case is handled by assuming direct impact of that column to outcome.
NOTE: If there is 1 dataframe then it is assumed that first column is date and last column is output.
NOTE: If multiple dataframes then it is assumed that last dataframe in list contains last column as output column.
NOTE: All dataframes are merged on the basis of common dates.
Run method contains all the method calls and is the first method to be called after main.
1. 1st method is merge_dataframes which handles the dataframe merging on date column and some preprocessing.
2. 2nd method called is prepare_data which further calls :
    a) extract_columns_from_hierarchy method : It finds the graph edges for indirect impacts.
    b) add_direct_connections : Provides graph edges for direct impact.
3. 3rd method is build_causal_graph which gives the final digraph.
4. 4th method is causal_analysis which is the last method which provides the result dataframes(contributions & natural_effect)
"""