# -*- coding: utf-8 -*-
from formulaone.load_db import *


def get_hmm():
    """Get a thought."""
    return 'hmmm...'



# Main script
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