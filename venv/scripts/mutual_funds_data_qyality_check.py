import pandas as pd
import sys

'''
from glob import glob

file_loc = "/Users/sharadbagal/BigData/Research/BNY-Code-Challenge/data/"
file_names = glob(fil_loc + "Fund_Data*.csv" )

funds = [pd.read_csv(f) for f in file_names]
'''

## Mutual Fund Data Section Starts
# Read the Fund_Data_1.csv File First
file_loc = "~/BigData/Research/BNY-Code-Challenge/data/"
funds = pd.read_csv(file_loc + 'Fund_Data_1.csv',
                        sep=',',
                        header=0,
                        index_col=False,
                        dayfirst=True,
                        error_bad_lines=True,
                        warn_bad_lines=True,
                        skip_blank_lines=True
                        )


columns = pd.DataFrame(list(funds.columns.values))
print(columns)

data_types = pd.DataFrame(funds.dtypes,
                          columns=['Data Type'])


print(data_types)

missing_data_counts = pd.DataFrame(funds.isnull().sum(),
                                   columns=['Missing Values'])
#print(missing_data_counts)


present_data_counts = pd.DataFrame(funds.count(),
                                   columns=['Present Values'])
#present_data_counts


unique_value_counts = pd.DataFrame(columns=['Unique Values'])
for v in list(funds.columns.values):
    unique_value_counts.loc[v] = [funds[v].nunique()]
#unique_value_counts

## After that, create a DataFrame with the minimum value in each column:

minimum_values = pd.DataFrame(columns=['Minimum Value'])
for v in list(funds.columns.values):
    minimum_values.loc[v] = [funds[v].min()]
#minimum_values


## The last DataFrame that we'll create is a DataFrame with the maximum value in each column:
maximum_values = pd.DataFrame(columns=['Maximum Value'])
for v in list(funds.columns.values):
    maximum_values.loc[v] = [funds[v].max()]
#maximum_values

data_quality_report = data_types.join(present_data_counts).join(missing_data_counts).join(unique_value_counts).join(minimum_values).join(maximum_values)

print("\nData Quality Report")
print("Total records: {}".format(len(funds.index)))
#data_quality_report


data_quality_report.to_csv(sys.stdout)
## Mutual Fund Data Section Ends


#print(data_quality_report)
