import pandas as pd
from luminol import anomaly_detector

from datetime import datetime
import matplotlib.pyplot as plt

# reading csv data 
def read_data(file_path):
    data = pd.read_csv(file_path)
    return data

"""
Doing some preprocessing like converting to appropriate data types.
Setting date as index.
Converting date to timestamp.
"""
def preprocessing(data,format_str):
    data['date'] = pd.to_datetime(data['date'], format=format_str)
    data['value'] =  data['value'].astype(float)

    data.set_index('date', inplace=True)

    time_series = {int(d.timestamp()): v for d, v in zip(data.index, data['value'])}

    return time_series

"""
Using luminol library's anomaly detector finding anomalies in this function.
Creating an anomaly dataframe which contains original date and value.
"""
def find_anomalies(time_series,data):
    anomaly_detect = anomaly_detector.AnomalyDetector(time_series)
    anomalies = anomaly_detect.get_anomalies()

    anomaly_data = []

    for anomaly in anomalies:
        start_timestamp = anomaly.start_timestamp
        end_timestamp = anomaly.end_timestamp
        
        start_datetime = datetime.fromtimestamp(start_timestamp)
        end_datetime = datetime.fromtimestamp(end_timestamp)
        
        if start_timestamp == end_timestamp:
            anomaly_value = data.loc[data.index == start_datetime.strftime('%Y-%m-%d'), 'value'].values[0]
            anomaly_data.append([start_datetime.strftime('%Y-%m-%d'), anomaly_value])
        else:
            range_data = data.loc[start_datetime.strftime('%Y-%m-%d'):end_datetime.strftime('%Y-%m-%d')]
            for date, value in range_data.itertuples():
                anomaly_data.append([date.strftime('%Y-%m-%d'), value])
    
    anomaly_df = pd.DataFrame(anomaly_data, columns=['date', 'value'])

    return anomaly_df

# Main function where all functions are called.
def detect():
    data= read_data(r'C:\Users\anubhav\Desktop\leverslabs\data_24_04\new_biz_deals_weekly.csv')
    time_series = preprocessing(data,'%m/%d/%Y') # Pass date format here. Replace / with . if date is in that format
    anomalies = find_anomalies(time_series,data)
    return anomalies

anomaly_df = detect()
print(anomaly_df)

