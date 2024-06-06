import boto3
import pandas as pd
import json


def load_config(config_file):
    """
    Load configuration settings from a JSON file.

    :param config_file: Path to the JSON configuration file.
    :return: Dictionary containing the configuration settings.
    """
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config


def connect_to_dynamodb(aws_access_key_id, aws_secret_access_key, region_name):
    """
    Establish a connection to DynamoDB using AWS credentials.

    :param aws_access_key_id: AWS access key ID.
    :param aws_secret_access_key: AWS secret access key.
    :param region_name: AWS region name.
    :return: DynamoDB resource object.
    """
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    dynamo_resource = session.resource('dynamodb')
    return dynamo_resource


def get_table(dynamo_resource, table_name):
    """
    Retrieve a DynamoDB table object.

    :param dynamo_resource: DynamoDB resource object.
    :param table_name: Name of the DynamoDB table.
    :return: DynamoDB table object.
    """
    table = dynamo_resource.Table(table_name)
    return table


def scan_table(table):
    """
    Scan a DynamoDB table and retrieve all items.

    :param table: DynamoDB table object.
    :return: List of items in the table.
    """
    response = table.scan()
    data = response['Items']
    return data


def load_data_into_dataframe(data):
    """
    Load a list of dictionaries into a pandas DataFrame.

    :param data: List of dictionaries representing table data.
    :return: pandas DataFrame containing the data.
    """
    df = pd.json_normalize(data)
    df = pd.DataFrame(df)
    return df


def handle_list_entries(data, column):
    """
    Handle list entries in a specified column and apply one-hot encoding.

    :param data: pandas DataFrame.
    :param column: Column name with list entries to explode and encode.
    :return: pandas DataFrame with one-hot encoded columns.
    """
    df_expanded = data.explode(column)
    dummies = pd.get_dummies(df_expanded[column], prefix=column)
    df_final = data.drop(column, axis=1).join(dummies.groupby(df_expanded.index).sum())
    return df_final
