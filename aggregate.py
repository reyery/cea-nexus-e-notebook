import pandas as pd
import numpy as np
import os
import glob
from multiprocessing import Pool

### config
# Edit these values to point to the project path and scenario name
# Remeber to put an `r` in front of the path to prevent escaping of characters
project_path = r'F:\nexus-e'
scenario_name = 'tobel-tagerschen-switzerland'
###

# scenario path
scenario_path = os.path.join(project_path, scenario_name)
data_path = os.path.join(scenario_path, 'outputs', 'data')
output_path = os.path.join(scenario_path, 'aggregate')

### inputs
# demand inputs
demand_path = os.path.join(data_path, 'demand')
total_demand_path = os.path.join('Total_demand.csv')
# radiaton inputs
radiation_path = os.path.join(data_path, 'solar-radiation')

### outputs
# demand outputs
hourly_demand_output_path = os.path.join(output_path, 'hourly_demand.csv')
total_demand_output_path = os.path.join(output_path, 'total_demand.csv')
# radiation outpus
hourly_radiation_output_path = os.path.join(output_path, 'hourly_solar_radiation.csv')
total_radiation_output_path = os.path.join(output_path, 'total_solar_radiation.csv')

print(demand_path, radiation_path)


def get_radiation_hourly_total(radiation_files):
    df = pd.read_csv(radiation_files[0])
    filter_col = [col for col in df if col.endswith('kW')]
    total = pd.DataFrame()
    for i, file_path in enumerate(radiation_files):
        print(i, file_path)
        radiation_df = pd.read_csv(file_path).set_index('Date')[filter_col]
        if i == 0:
            total = radiation_df
        else:
            total = total + radiation_df
    return total


def get_demand_hourly_total(demand_files):
    df = pd.read_csv(demand_files[0])
    filter_col = [col for col in df if col != 'Name']
    total = pd.DataFrame()
    for i, file_path in enumerate(demand_files):
        print(i, file_path)
        try:
            demand_df = pd.read_csv(file_path)[filter_col].set_index('DATE')
            if i == 0:
                total = demand_df
            else:
                total = total + demand_df
        except Exception as e:
            raise ValueError(file_path)
    return total


def aggregate_total_demand():
    demand_files = glob.glob(os.path.join(demand_path, 'B*.csv'))
    hourly_total = pd.DataFrame()
    num_process = 10

    pool = Pool(processes=num_process)
    for i, x in enumerate(
            pool.imap_unordered(get_demand_hourly_total, np.array_split(demand_files, num_process))):
        if i == 0:
            hourly_total = x
        else:
            hourly_total = hourly_total + x
    hourly_total.to_csv(hourly_demand_output_path)

    df = pd.read_csv(total_demand_path)
    total_demand = df.sum(numeric_only=True)
    total_demand.to_csv(total_demand_output_path)


def aggregate_total_radiation():
    radiation_files = glob.glob(os.path.join(radiation_path, '*_radiation.csv'))
    hourly_total = pd.DataFrame()
    num_process = 4

    pool = Pool(processes=num_process)
    for i, x in enumerate(pool.imap_unordered(get_radiation_hourly_total, np.array_split(radiation_files, num_process))):
        if i == 0:
            hourly_total = x
        else:
            hourly_total = hourly_total + x
    hourly_total.to_csv(hourly_radiation_output_path)

    total = hourly_total.sum()
    total.to_csv(total_radiation_output_path)


if __name__ == "__main__":
    # Comment/uncomment accordingly
    aggregate_total_demand()
    # aggregate_total_radiation()
