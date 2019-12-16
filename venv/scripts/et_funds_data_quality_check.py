import pandas as pd
import sys

'''
from glob import glob

file_loc = "/Users/sharadbagal/BigData/Research/BNY-Code-Challenge/data/"
file_names = glob(fil_loc + "Fund_Data*.csv" )

et_funds = [pd.read_csv(f) for f in file_names]
'''

## Mutual Fund Data Section Starts
# Read the Fund_Data_1.csv File First
file_loc = "/Users/sharadbagal/BigData/Research/BNY-Code-Challenge/data/"
et_funds = pd.read_csv(file_loc + 'Fund_Data_2.csv',
                        sep=',',
                        header=0,
                        index_col=False,
                        dayfirst=True,
                        error_bad_lines=True,
                        warn_bad_lines=True,
                        skip_blank_lines=True
                        )

## ETF Data Section Starts
data_types = pd.DataFrame(et_funds.dtypes,
                          columns=['Data Type'])

#print(data_types)

missing_data_counts = pd.DataFrame(et_funds.isnull().sum(),
                                   columns=['Missing Values'])
#print(missing_data_counts)


present_data_counts = pd.DataFrame(et_funds.count(),
                                   columns=['Present Values'])
#present_data_counts


unique_value_counts = pd.DataFrame(columns=['Unique Values'])
for v in list(et_funds.columns.values):
    unique_value_counts.loc[v] = [et_funds[v].nunique()]
#unique_value_counts

## After that, create a DataFrame with the minimum value in each column:

minimum_values = pd.DataFrame(columns=['Minimum Value'])
for v in list(et_funds.columns.values):
    minimum_values.loc[v] = [et_funds[v].min()]
#minimum_values


## The last DataFrame that we'll create is a DataFrame with the maximum value in each column:
maximum_values = pd.DataFrame(columns=['Maximum Value'])
for v in list(et_funds.columns.values):
    maximum_values.loc[v] = [et_funds[v].max()]
#maximum_values

data_quality_report = data_types.join(present_data_counts).join(missing_data_counts).join(unique_value_counts).join(minimum_values).join(maximum_values)

print("\nData Quality Report")
print("Total records: {}".format(len(et_funds.index)))
#data_quality_report


data_quality_report.to_csv(sys.stdout)

## ETF Data Section Ends



