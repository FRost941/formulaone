Formula One Module Repository
========================

This Ptyhon project downloads and prepares formula one data from the API ergast.com.

This Python projectscript connects to an AWS DynamoDB table, retrieves the data, processes it into a pandas DataFrame,
and handles specific list entries by applying one-hot encoding. This process allows for the transformation of nested and
list-like data structures into a flat, machine-learning-friendly format.
Dependencies

Make sure to have the following Python libraries installed:

    pytest
    sphinx
    ipykernel
    requests
    pandas
    pyarrow
    fastparquet
    scikit-learn
    boto3

You can install these libraries using pip if they are not already installed:

pip install boto3 pandas

Configuration

The script relies on a configuration file (config.json) that contains AWS credentials, DynamoDB table information,
and columns to be processed. Below is an example of what the config.json file might look like:

json

{
    "aws_access_key_id": "your_access_key",
    "aws_secret_access_key": "your_secret_key",
    "region_name": "your_region",
    "table_name": "your_table_name",
    "categorial_columns": ["info.actors", "info.genres", "info.directors"],
    "numerical_columns": ["rating", "year"]
}

Functions

    load_config(config_file)
        Loads configuration settings from a JSON file.
        Parameters: config_file (str): Path to the JSON configuration file.
        Returns: Dictionary containing the configuration settings.

    connect_to_dynamodb(aws_access_key_id, aws_secret_access_key, region_name)
        Establishes a connection to DynamoDB using AWS credentials.
        Parameters:
            aws_access_key_id (str): AWS access key ID.
            aws_secret_access_key (str): AWS secret access key.
            region_name (str): AWS region name.
        Returns: DynamoDB resource object.

    get_table(dynamo_resource, table_name)
        Retrieves a DynamoDB table object.
        Parameters:
            dynamo_resource (boto3.resource): DynamoDB resource object.
            table_name (str): Name of the DynamoDB table.
        Returns: DynamoDB table object.

    scan_table(table)
        Scans a DynamoDB table and retrieves all items.
        Parameters: table (boto3.Table): DynamoDB table object.
        Returns: List of items in the table.

    load_data_into_dataframe(data)
        Loads a list of dictionaries into a pandas DataFrame.
        Parameters: data (list): List of dictionaries representing table data.
        Returns: pandas DataFrame containing the data.

    handle_list_entries(data, column)
        Handles list entries in a specified column and applies one-hot encoding.
        Parameters:
            data (pandas.DataFrame): pandas DataFrame.
            column (str): Column name with list entries to explode and encode.
        Returns: pandas DataFrame with one-hot encoded columns.

Example Usage

Here is an example of how to run the script:

python

config_file = 'config.json'
config = load_config(config_file)

dynamo_resource = connect_to_dynamodb(aws_access_key_id=config["aws_access_key_id"],
                                      aws_secret_access_key=config["aws_secret_access_key"],
                                      region_name=config["region_name"])

table = get_table(dynamo_resource, config["table_name"])
data = scan_table(table)
df = load_data_into_dataframe(data)
df = df[config["categorial_columns"] + config["numerical_columns"]]

df_expanded_v1 = df['info.genres'].apply(pd.Series)

df_expanded_v2 = handle_list_entries(df, "info.actors")
df_expanded_v2 = handle_list_entries(df_expanded_v2, "info.genres")
df_expanded_v2 = handle_list_entries(df_expanded_v2, "info.directors")

df.info()

Test Script

A test script is provided to validate each function. The test script uses the unittest library and unittest.mock to mock
AWS interactions and test the processing functions.
Test Cases

    Test Load Configuration
        Verifies that the configuration file is correctly loaded.

    Test Connect to DynamoDB
        Verifies that a connection to DynamoDB is established using provided AWS credentials.

    Test Get Table
        Verifies that the DynamoDB table object is retrieved.

    Test Load Data into DataFrame
        Verifies that the data retrieved from the table is correctly loaded into a pandas DataFrame.

    Test Handle List Entries
        Verifies that list entries in specified columns are exploded and one-hot encoded correctly.

Example Test Script

python

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

class TestDynamoDBFunctions(unittest.TestCase):

    def setUp(self):
        self.config = {
            "aws_access_key_id": "fake_access_key",
            "aws_secret_access_key": "fake_secret_key",
            "region_name": "us-east-1",
            "table_name": "fake_table",
            "categorial_columns": ["info.actors", "info.genres", "info.directors"],
            "numerical_columns": ["rating", "year"]
        }
        self.sample_data = [
            {
                "info": {
                    "actors": ["Actor1", "Actor2"],
                    "genres": ["Genre1", "Genre2"],
                    "directors": ["Director1"]
                },
                "rating": 8.5,
                "year": 2020
            }
        ]

    @patch('boto3.Session')
    def test_connect_to_dynamodb(self, mock_session):
        mock_boto_session = MagicMock()
        mock_session.return_value = mock_boto_session
        dynamo_resource = connect_to_dynamodb(
            aws_access_key_id=self.config["aws_access_key_id"],
            aws_secret_access_key=self.config["aws_secret_access_key"],
            region_name=self.config["region_name"]
        )
        self.assertTrue(mock_boto_session.resource.called)
        self.assertEqual(dynamo_resource, mock_boto_session.resource.return_value)

    @patch('boto3.resource')
    def test_get_table(self, mock_resource):
        mock_table = MagicMock()
        mock_resource.Table.return_value = mock_table
        dynamo_resource = MagicMock()
        table = get_table(dynamo_resource, "fake_table")
        self.assertTrue(dynamo_resource.Table.called)
        self.assertEqual(table, mock_table)

    def test_load_data_into_dataframe(self):
        df = load_data_into_dataframe(self.sample_data)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("info.actors", df.columns)

    def test_handle_list_entries(self):
        df = pd.DataFrame(self.sample_data)
        df_normalized = pd.json_normalize(df.to_dict(orient='records'))
        df_with_lists = handle_list_entries(df_normalized, "info.actors")
        self.assertIn("info.actors_Actor1", df_with_lists.columns)
        self.assertIn("info.actors_Actor2", df_with_lists.columns)

if __name__ == '__main__':
    unittest.main()

This test script ensures that each function in your script works as expected. It includes mock objects to simulate AWS
services, making the tests run in isolation and without external dependencies.