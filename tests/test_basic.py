# content of test_sample.py

from formulaone.helpers import get_tidy_data_path
from formulaone.load_db import *
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

def test_check_dataframe_size():
    df = pd.read_parquet(get_tidy_data_path() / 'current_race.parquet')
    assert df.shape[1] == 26


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
