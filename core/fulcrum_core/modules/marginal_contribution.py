import pandas as pd
import numpy as np
 
class SourcingWins:
    def __init__(self, data_path=None, dataframe=None):
        if data_path:
            self.data = pd.read_csv(data_path)
        elif dataframe is not None:
            self.data = dataframe.copy()
        else:
            raise ValueError("Either data_path or dataframe must be provided.")
        self.preprocess_data()
 
    def preprocess_data(self):
        self.data['CREATED_AT'] = pd.to_datetime(self.data['CREATED_AT'].apply(lambda x: x.split(' ')[0]), errors='coerce')
        self.data['CLOSE_DATE'] = pd.to_datetime(self.data['CLOSE_DATE'].apply(lambda x: x.split(' ')[0]), errors='coerce')
        self.data = self.data[self.data['CLOSE_DATE'] >= self.data['CREATED_AT']]
        print("Preprocessing complete")
 
    def create_dynamic_cohorts(self, start_date, grain='W', num_periods=8):
        cohort_data = []
        for i in range(num_periods):
            if grain == 'W':
                cohort_end = start_date - pd.Timedelta(days=i * 7 + 1)
                cohort_start = cohort_end - pd.Timedelta(days=6)
                label = f'Week-{i+1}'
            elif grain == 'M':
                cohort_end = start_date - pd.DateOffset(months=i+1) + pd.DateOffset(days=-1)
                cohort_start = cohort_end - pd.DateOffset(days=cohort_end.days_in_month - 1)
                label = f'Month-{i+1}'
            elif grain == 'D':
                cohort_end = start_date - pd.Timedelta(days=i + 1)
                cohort_start = cohort_end
                label = f'Day-{i+1}'
            else:
                raise ValueError("Grain must be one of 'W', 'M', or 'D'")
            
            cohort_data.append({
                'CohortStart': cohort_start,
                'CohortEnd': cohort_end,
                'Label': label
            })
        
        return pd.DataFrame(cohort_data)
 
    def calculate_metrics(self, start_date, window_length, grain='W', num_periods=8):
        cohorts = self.create_dynamic_cohorts(start_date, grain, num_periods)
        print(cohorts)
        window_data = []
        for i in range(num_periods):
            cohort_start = cohorts.iloc[i]['CohortStart']
            cohort_end = cohorts.iloc[i]['CohortEnd']
            label = cohorts.iloc[i]['Label']
            
            cohort = self.data[(self.data['CREATED_AT'] >= cohort_start) & (self.data['CREATED_AT'] <= cohort_end)]
 
            stock = len(cohort)
            
            # Calculate WonInWindow: Deals created within the cohort and won within the rolling window
            rolling_window_end = start_date + pd.Timedelta(days=window_length)
            won_in_window = len(cohort[(cohort['CLOSE_DATE'] >= start_date) &
                                       (cohort['CLOSE_DATE'] <= rolling_window_end) &
                                       (cohort['OPPORTUNITY_STAGE'] == 'Closed Won')])
            
            # Calculate CumulativeWon: Deals created within the cohort and won before the end of the rolling window
            cumulative_won = len(cohort[(cohort['CLOSE_DATE'] <= rolling_window_end) &
                                        (cohort['OPPORTUNITY_STAGE'] == 'Closed Won')])
 
            conversion_rate = (won_in_window / stock) * 100 if stock > 0 else 0
            cumulative_conversion_rate = (cumulative_won / stock) * 100 if stock > 0 else 0
 
            window_data.append({
                'CohortStart': cohort_start,
                'CohortEnd': cohort_end,
                'Label': label,
                'Stock': stock,
                'WonInWindow': won_in_window,
                'CumulativeWon': cumulative_won,
                'ConversionRate': conversion_rate,
                'CumulativeConversionRate': cumulative_conversion_rate
            })

        return pd.DataFrame(window_data)
 
    def compare(self, start_date, window1_length, window2_length, grain='W', num_periods=8, metric_type='cumulative'):
        window1_data = self.calculate_metrics(start_date, window1_length, grain, num_periods)
        window2_start = start_date + pd.Timedelta(days=window1_length)
        window2_data = self.calculate_metrics(window2_start, window2_length, grain, num_periods)
 
        stock_mc_df = pd.DataFrame(index=range(num_periods), columns=['Stock_MC'])
        conversion_mc_df = pd.DataFrame(index=range(num_periods), columns=['Conversion_MC'])
        pressure_table = pd.DataFrame(index=range(num_periods), columns=['Pressure'])
        contribution_overall_stock = pd.DataFrame(index=range(num_periods), columns=['Contribution_Overall_Stock'])
        contribution_overall_conversion = pd.DataFrame(index=range(num_periods), columns=['Contribution_Overall_Conversion'])
 
        for i in range(num_periods):  # For each period
            stock1 = window1_data.iloc[i]['Stock']
            stock2 = window2_data.iloc[i]['Stock']
            if metric_type.startswith('cumulative'):
                wins1 = window1_data.iloc[i]['CumulativeWon']
                wins2 = window2_data.iloc[i]['CumulativeWon']
                conversion1 = window1_data.iloc[i]['CumulativeConversionRate'] / 100.0
                conversion2 = window2_data.iloc[i]['CumulativeConversionRate'] / 100.0
            else:
                wins1 = window1_data.iloc[i]['WonInWindow']
                wins2 = window2_data.iloc[i]['WonInWindow']
                conversion1 = window1_data.iloc[i]['ConversionRate'] / 100.0
                conversion2 = window2_data.iloc[i]['ConversionRate'] / 100.0
 
            # Calculate StockMC
            delta_stock = stock1 - stock2
            parent_delta = wins1 - wins2
            pct_stock = (delta_stock / stock1) * 100 if stock1 != 0 else 0
            relative_impact_stock = delta_stock / parent_delta if parent_delta != 0 else 0
            stock_mc = relative_impact_stock * pct_stock if stock1 != 0 else 0
 
            # Calculate ConversionMC
            delta_conversion = conversion1 - conversion2
            pct_conversion = (delta_conversion / conversion1) * 100 if conversion1 != 0 else 0
            relative_impact_conversion = delta_conversion / parent_delta if parent_delta != 0 else 0
            conversion_mc = relative_impact_conversion * pct_conversion if conversion1 != 0 else 0
 
            # Handle potential inf or NaN values
            stock_mc_df.at[i, 'Stock_MC'] = stock_mc if pd.notna(stock_mc) and not np.isinf(stock_mc) else 0
            conversion_mc_df.at[i, 'Conversion_MC'] = conversion_mc if pd.notna(conversion_mc) and not np.isinf(conversion_mc) else 0
 
            # Calculate pressure
            pressure_table.at[i, 'Pressure'] = ((wins2 - wins1) / (window2_data['CumulativeWon'].sum() - window1_data['CumulativeWon'].sum())) * 100 if (window2_data['CumulativeWon'].sum() - window1_data['CumulativeWon'].sum()) != 0 else 0
 
            # Calculate Contribution Overall Stock and Conversion
            contribution_overall_stock.at[i, 'Contribution_Overall_Stock'] = stock_mc_df.at[i, 'Stock_MC'] * pressure_table.at[i, 'Pressure']
            contribution_overall_conversion.at[i, 'Contribution_Overall_Conversion'] = conversion_mc_df.at[i, 'Conversion_MC'] * pressure_table.at[i, 'Pressure']
 
        print("\nWindow 1 Data:\n", window1_data)
        print("\nWindow 2 Data:\n", window2_data)
        print("\nStock Marginal Contribution Table:\n", stock_mc_df)
        print("\nConversion Marginal Contribution Table:\n", conversion_mc_df)
        print("\nPressure Table:\n", pressure_table)
        print("\nContribution Overall Stock:\n", contribution_overall_stock)
        print("\nContribution Overall Conversion:\n", contribution_overall_conversion)
 
# Data used
data = pd.read_csv(r"opp_data 1.csv")
 
# Initializing class variables
sourcing_wins = SourcingWins(dataframe=data)
 
# Example usage
# start_date = pd.to_datetime('2021-03-05')  # Example start date
start_date = pd.to_datetime('2021-03-20')  # Example start date
window1_length = 10  # Example window length for Window 1
window2_length = 12  # Example window length for Window 2
 
# Example of dynamic metric comparison
sourcing_wins.compare(start_date=start_date, window1_length=window1_length, window2_length=window2_length, grain='D', num_periods=8)