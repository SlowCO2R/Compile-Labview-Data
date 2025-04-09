# -*- coding: utf-8 -*-
"""
Created on Tue Apr  8 11:11:14 2025

@author: tpham + ChatGPT
"""

import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

#%% === CONFIGURATION ===
csv_folder = 'Y:/5900/HydrogenTechFuelCellsGroup/CO2R/Nhan P/Experiments/CO2 Cell Testing/TS7/1NP91'  # Folder where your CSV files are stored
keywords = ['CELL_V_FB', 'PS_CURRENT_DENSITY', 'C_CO2_HI_FB', 'C_OUTLET_FB']            #This can be upper-case. There is a code that changes everything to lowercase while searching
timestamp_column = 'Timestamp'
time_column = 'Time' #This column is for sorting
comment_column = 'MATRIX_COMMENT'

time_window_default = pd.Timedelta('5 minutes')
time_window_short = pd.Timedelta('1 minute')
export_output = True
plot_summary = False

export_folder = csv_folder + '/output'
os.makedirs(export_folder, exist_ok=True)

#%% === STEP 1: Load CSV Files ===
csv_files = glob.glob(os.path.join(csv_folder, '*.csv'))
all_data = []

if not csv_files:
    raise ValueError(f"❌ No CSV files found in folder: {csv_folder}")

print(f"✅ Found CSV files: {csv_files}")
all_data = []
all_columns = set()  # collect all needed column names

# First pass: figure out all possible columns (for reindexing)
for file in csv_files:
    df = pd.read_csv(file)
    df[timestamp_column] = pd.to_datetime(df[timestamp_column], errors='coerce')
    df = df.dropna(subset=[timestamp_column])
    df['SourceFile'] = os.path.basename(file)
    all_data.append(df)

combined_df = pd.concat(all_data, ignore_index=True)
#print(combined_df)

all_columns = list(all_columns) + ['SourceFile', 'IsEmptyOrAllNA', 'GroupID']

# Sort data by timestamp and comment
combined_df = combined_df.sort_values(by=[timestamp_column, time_column]).reset_index(drop=True)

print(combined_df)

#%% === STEP 2: Group by Consecutive Comments ===
combined_df['CommentShifted'] = combined_df[comment_column].shift()     #identify changes from the current row with the previous row
combined_df['TimeDiff'] = combined_df[timestamp_column].diff().dt.total_seconds().fillna(0)         #identify changes in the time difference
combined_df['NewGroup'] = (combined_df[comment_column] != combined_df['CommentShifted']) | (combined_df['TimeDiff'] > 300)      #create new group based on the comment row and the time difference > 5 minutes
combined_df['GroupID'] = combined_df['NewGroup'].cumsum()       #create new Group ID for each group

#print(combined_df)
#%% === === STEP 3: Process Each Group ===
summary_rows = []

#For each group_id assigned to the group in dataframe
for group_id, group in combined_df.groupby('GroupID'):
    group = group.sort_values(by=timestamp_column)
    group_comment = group[comment_column].iloc[0]

    group_duration = group[timestamp_column].max() - group[timestamp_column].min()
    if group_duration <= time_window_default:       #Check if timestamp is within 5 minutes
        window_df = group                           #The dataframe we work with is within ONE SELECTED group in the loop comment.
        window_start = group[timestamp_column].min()
        window_end = group[timestamp_column].max()          #Then the ALL data within this window will be used for average and export
    else:
        window_end = group[timestamp_column].max()          
        window_start = window_end - time_window_default  #(time_window_default if group_duration >= time_window_default else time_window_short)
        window_df = group[group[timestamp_column] >= window_start] #Shorten dataframe within only last 5 minutes within the group
        #Call row to insert new header in the final dataframe
    row = {
        'GroupID': group_id,
        'Comment': group_comment,
        'WindowStart': window_start,
        'WindowEnd': window_end
    }
            
    for col in group.columns:       #group.columns return an Index object that contains the names of all columns in the dataframe 'group'
        if any(keyword.lower() in col.lower() for keyword in keywords):     #make sure everything is in lowercase while searching
            row[f'{col}_Mean'] = window_df[col].mean()                      #return average
            row[f'{col}_StdDev'] = window_df[col].std()                     #return standard deviation

    summary_rows.append(row)                                                #append to existing dataframe before looping to the next group

summary_df = pd.DataFrame(summary_rows)                                     #produce dataframe from appendages above
summary_df = summary_df.sort_values(by='GroupID')                       #sort by GroupID

#%% === STEP 4: Export to Excel ===

# Export to Excel
if export_output:
    summary_df.to_excel(export_folder + "/summary_with_first_last_timestamps_adjusted.xlsx", index=False)
    print("✅ Exported summary to 'summary_with_first_last_timestamps_adjusted.xlsx'.")

# Plot if needed
if plot_summary:
    for col in summary_df.columns:
        if 'mean' in col:
            summary_df.plot(x='Group', y=col, kind='bar', title=f"{col} (Last Adjusted Period per Group)")
            plt.tight_layout()
            plt.show()