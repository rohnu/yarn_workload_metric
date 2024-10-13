#!/usr/bin/env python
import pandas as pd
from datetime import datetime, timedelta
from tabulate import tabulate
import numpy as np
df = pd.read_csv('mapr.csv')
pd.set_option('display.max_rows', None)
df['startTime'] = pd.to_datetime(df['startTime'], unit='ms')
df['endTime'] = pd.to_datetime(df['endTime'], unit='ms')
df = df.drop(columns=['applicationTags', 'allocatedVcoreSeconds', 'allocatedMemorySeconds','runningContainers','progress'], errors='ignore')
df_finished = df[((df['state'] == 'FINISHED') & (df['endTime']!='1970-01-01 00:00:00'))]
df_finished = df_finished.drop(columns=['state','duration'])
print(tabulate(df_finished, headers='keys', tablefmt='psql'))
# Prepare an empty DataFrame to store time series data
time_series_data = []
# Iterate through each row in the original DataFrame
for index, row in df_finished.iterrows():
    start_time = row['startTime']
    end_time = row['endTime']
    time_delta = timedelta(minutes=1)

    # Generate time series entries for each minute
    current_time = start_time
    while current_time <= end_time:
        time_series_data.append({
            'application_type': row['application_type'],
            'applicationId': row['applicationId'],
            'name': row['name'],
            'startTime': current_time.floor('T'),
            'endTime': (current_time + time_delta).floor('T'),
            'user': row['user'],
            'pool': row['pool'],
            'avg_allocatedMB': row['avg_allocatedMB'],
            'avg_allocatedVCores': row['avg_allocatedVCores'],
        })
        current_time += time_delta

# Create a new DataFrame from the time series data
time_series_df = pd.DataFrame(time_series_data)
time_series_df = time_series_df.sort_values(by='startTime', ascending=True)
#Save the timeseries data
time_series_df.to_csv('all_wx_timeseries.csv')

# Melt the DataFrame to long format
melted_df = time_series_df.melt(id_vars=['startTime', 'endTime', 'application_type','pool','avg_allocatedMB'], value_vars=['applicationId'])
# Pivot the DataFrame only memory , same logic aplies to Core
pivot_df = melted_df.pivot_table(index=['startTime', 'endTime', 'application_type','pool'], columns='value', values='avg_allocatedMB')
# Reset the index for better readability
pivot_df = pivot_df.reset_index()
pivot_df = pivot_df.fillna(0)
pivot_df['Grand Total avg_allocatedMB'] = pivot_df.sum(axis=1)
final_df = pivot_df[['startTime', 'endTime', 'application_type','pool','Grand Total avg_allocatedMB']]
# Display the transposed DataFrame
#print(tabulate(pivot_df, headers='keys', tablefmt='psql'))
final_df.to_csv('mapr_convert.csv')

# Plotting the graph
plt.figure(figsize=(12, 6))
plt.plot(final_df['startTime'], final_df['Grand Total avg_allocatedMB'], marker='o', linestyle='-', color='b')

# Add title and labels
plt.title('Time Series of Grand Total avg_allocatedMB')
plt.xlabel('Start Time')
plt.ylabel('Grand Total avg_allocatedMB (MB)')
plt.xticks(rotation=45)
plt.grid()

# Annotate each point with the value
for i, value in enumerate(final_df['Grand Total avg_allocatedMB']):
    plt.annotate(f'{value:.1f}',
                 (final_df['startTime'].iloc[i], value),
                 textcoords="offset points",
                 xytext=(0,10),
                 ha='center',
                 fontsize=9)

plt.tight_layout()
plt.show()

#Group By function to identify max and min Vcores and Memory used by Application type or pool or both during peak hours.
memory_melted_df = time_series_df.melt(id_vars=['startTime', 'endTime','application_type','pool','eff_allocatedMB'], value_vars=['applicationId'])
memory_grouped_df = memory_melted_df.groupby(['startTime', 'endTime', 'application_type','pool']).agg({'eff_allocatedMB': 'sum'}).reset_index()
# Melt the DataFrame to long format
core_melted_df = time_series_df.melt(id_vars=['startTime', 'endTime','cluster','application_type','pool','eff_allocatedVCores'], value_vars=['applicationId'])
core_grouped_df = core_melted_df.groupby(['startTime', 'endTime', 'cluster','application_type','pool']).agg({'eff_allocatedVCores': 'sum'}).reset_index()


# Perform groupby on 'application_type' and 'pool' and calculate max of 'Grand Total avg_allocatedMB'
memory_min_df = memory_grouped_df.groupby(['cluster','application_type','pool'])['eff_allocatedMB'].min().reset_index()
memory_min_df = memory_min_df.rename(columns={'eff_allocatedMB': 'min_memory'})
memory_max_df = memory_grouped_df.groupby(['cluster','application_type','pool'])['eff_allocatedMB'].max().reset_index()
memory_max_df = memory_max_df.rename(columns={'eff_allocatedMB': 'max_memory'})
# Perform groupby on 'application_type' and 'pool' and calculate max of 'Grand Total avg_allocatedMB'
core_min_df = core_grouped_df.groupby(['cluster','application_type','pool'])['eff_allocatedVCores'].min().reset_index()
core_min_df = core_min_df.rename(columns={'eff_allocatedVCores': 'min_core'})
core_max_df = core_grouped_df.groupby(['cluster','application_type','pool'])['eff_allocatedVCores'].max().reset_index()
core_max_df = core_max_df.rename(columns={'eff_allocatedVCores': 'max_core'})
core_df = pd.merge(core_min_df, core_max_df, on=[ 'cluster','application_type','pool'], how='inner')
memory_df= pd.merge(memory_min_df, memory_max_df, on= ['cluster','application_type','pool'], how='inner')
edl_prd_m01_wxm=pd.merge(core_df, memory_df, on=[ 'cluster','application_type','pool'], how='inner')
# Display the resulting DataFrame
print(tabulate(edl_prd_m01_wxm, headers='keys', tablefmt='psql'))
