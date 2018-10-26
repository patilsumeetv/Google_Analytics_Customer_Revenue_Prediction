import os
import json
import pandas as pd
from pandas.io.json import json_normalize

def data_preprocessing(csv_path, nrows=None):
    """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    Parameters:
        1) csv_path: path to csv files (train/test)
        2) nrows: number of rows in file to process
    Return:
        1) df: data file after seperating json columns to csv format
    """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # Splitting the file name
    read_path = csv_path.rsplit('/', 1)[0]
    file_name = csv_path.rsplit('/', 1)[1]

    # Creating save path is it does not exists
    save_path = os.path.join(read_path, 'preprocessed')
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    # Columns in csv file that contain json format
    json_columns = ['device', 'geoNetwork', 'totals', 'trafficSource']

    # Reading csv file with json columns
    df = pd.read_csv(os.path.join(read_path, file_name),
                     converters={column: json.loads for column in json_columns},
                     dtype={'fullVisitorId': 'str'},
                     nrows=nrows)

    print(f"Loaded {os.path.basename(csv_path)}. Shape: {df.shape}")

    for column in json_columns:
        column_as_df = json_normalize(df[column])
        column_as_df.columns = [f"{column}.{subcolumn}" \
                                for subcolumn in column_as_df.columns]
        df = df.drop(column, axis=1).merge(column_as_df,
                                           right_index=True,
                                           left_index=True)

    # Saving file to preprocessed directory
    save_path = os.path.join(save_path, file_name)
    df.to_csv(save_path, index = False)
    print(f"Preprocessed {os.path.basename(save_path)}. Shape: {df.shape}")

    return df

def main():
    # Reading train data and preprocessing
    train_df = data_preprocessing('Data/train.csv')

    # Reading test data and preprocessing
    test_df = data_preprocessing('Data/test.csv')

    return

if __name__ == '__main__':
    main()
