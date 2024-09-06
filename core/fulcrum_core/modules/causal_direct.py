import pandas as pd
from dowhy import CausalModel

class CausalInference:
    def __init__(self, dataframes):
        if len(dataframes) < 1:
            raise ValueError("At least one dataframe must be provided.")
        self.dataframes = dataframes

    def merge_dataframes(self):

        #If number of dataframes == 1
        if len(self.dataframes) == 1:
            df = self.dataframes[0]
            df.columns = df.columns.str.strip()
            df['ds'] = pd.to_datetime(df.iloc[:, 0])
            df.drop(columns=[df.columns[0]], inplace=True)
            df = df[['ds'] + list(df.columns[0:-1])]
            return df
        
        #If no. of dataframes>1 
        
        dfs = []
        for df in self.dataframes:
            df.columns = df.columns.str.strip()
            df['ds'] = pd.to_datetime(df.iloc[:, 0]) #name changed to ds
            df.drop(columns=[df.columns[0]], inplace=True) #drop original date column
            df = df[['ds'] + [col for col in df.columns if col != 'ds']] #Ensures 1st column is date 
            dfs.append(df)

        #Find all common dates in all dataframes
        common_dates = dfs[0]['ds']
        for df in dfs[1:]:
            common_dates = common_dates[common_dates.isin(df['ds'])]

        
        dfs = [df[df['ds'].isin(common_dates)] for df in dfs]

        merged_df = dfs[0]
        for df in dfs[1:]:
            merged_df = merged_df.merge(df, on='ds')

        return merged_df

    # Build digraph to pass in causal model
    def build_causal_graph(self, merged_df):
        factor_columns = merged_df.columns[1:-1]  # Exclude 'ds' and outcome column
        graph_edges = [f"{factor} -> {merged_df.columns[-1]}" for factor in factor_columns]
        graph_definition = f"digraph {{ {'; '.join(graph_edges)} }}"
        return graph_definition, factor_columns

    #Intakes graph and columns and prepares contribution dataframe 
    def causal_analysis(self, merged_df, factor_columns, graph_definition):
        try:
            outcome_column = merged_df.columns[-1]
            model = CausalModel(
                data=merged_df,
                treatment=[col for col in factor_columns],  # Exclude 'ds' and outcome column from treatment
                outcome=outcome_column,
                graph=graph_definition
            )

            identified_estimand = model.identify_effect(proceed_when_unidentifiable=True)
            causal_estimate = model.estimate_effect(identified_estimand, method_name="backdoor.linear_regression")
            effect_summary = causal_estimate.estimator.model.params

            contributions = pd.DataFrame({
                'Factor': effect_summary.index,
                'Coefficient': effect_summary.values
            })

            factor_dict = {f'x{i+1}': factor_columns[i] for i in range(len(factor_columns))}
            contributions['Factor'] = contributions['Factor'].map(lambda x: factor_dict.get(x, x))
            contributions['Percentage Contribution'] = (contributions['Coefficient'] / contributions['Coefficient'].sum()) * 100

            return contributions

        except Exception as e:
            print(f"An error occurred during causal analysis: {str(e)}")
            return None

    # runs all methods 
    def run(self):
        merged_df = self.merge_dataframes()
        if merged_df.empty:
            print("Merged DataFrame is empty.")
            return None

        graph_definition, factor_columns = self.build_causal_graph(merged_df)
        contributions = self.causal_analysis(merged_df, factor_columns, graph_definition)
        return contributions

if __name__ == "__main__":
    seasonal_factors = pd.read_csv(r"seasonal_factors.csv", usecols=['date', 'yearly'])
    merged_output = pd.read_csv(r"C:\Users\anubhav\Desktop\leverslabs\data_model\merged_output.csv")
    hierarchy_output = pd.read_csv(r"C:\Users\anubhav\Desktop\leverslabs\data_24_04\merged_output_hierarchy_all.csv", usecols=['date', 'open_new_biz_opps', 'sqo_rate', 'sqo_to_win_rate'])

    #List of dataframes
    # Ensure last dataframe in list should be whichever has output column
    dataframes = [seasonal_factors, hierarchy_output, merged_output]
    # dataframes = [merged_output]

    causal_analyzer = CausalInference(dataframes=dataframes)
    contributions = causal_analyzer.run()

    if contributions is not None:
        print("Percentage contributions of each factor (first 30 rows):\n")
        print(contributions.head(30))
    else:
        print("Causal analysis failed. Please check logs for details.")


"""
Takes a list of dataframes as input which have first column as date and last column should be output.
Next it goes to merge dataframes method where if list length is 1 then it just has minor changes done
If list has length>1, then it merges dataframes on date column and combines all dataframes in one with date as 1st
column and output as last.
Next it builds causal graph having only direct connections.
Next runs the model and prepares a dataframe contribution having factor as column name, coefficient and percentage contribution.  
"""