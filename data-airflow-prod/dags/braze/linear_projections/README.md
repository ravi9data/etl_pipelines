# Braze Linear Projection

The idea of the Linear Projections DAGs is to make it easy to project data from Glue/Athena into Redshift.
The `dags/braze/braze_linear_projections.py` file generates a DAG per configuration item found in `dags/braze/config.py`.

## The SQL pattern:

1. Create a final table and an intermediate table to load the incremetal data sets. This intermediate table is not persisted, so it only exists while the pipeline is running
2. 2 Python operators will check that the table created has the DIST and SORT KEY set in the config. If the KEYs are not the expected they will be updated.
3. Insert the incremental data: By default it loads the previous 2 hours of data
4. Upsert the incremental data into the final table.


## Add a new table:

- Add a json object to `dags/braze/config.py` with the following keys:
```
       {
        "source_schema": "xxxxxx",
        "source_table": "xxxxxx",
        "target_schema": "xxxxxx",
        "target_table": "xxxxxx",
        "partition_column": "xxxxxx",
        "dist_key": "xxxxxx",
        "sort_key": "xxxxxx",
        "dag_schedule": "30 * * * *",
        "column_list": 'xxx, xxxxx, xxxxxx, xxxxxxx, "xxxxx"',
       }
```

### How to easily get the list of columns from Glue:

- Just run this snippet from your Command line changing the name of the DatabaseName and Name arguments:

```
aws-vault exec grover-deng-eu -- python -c "import boto3
client = boto3.client('glue')
response = client.get_table(
    DatabaseName='data_production_braze_currents',
    Name='event_type_users_messages_email_delivery'
)
cols= [i['Name'] for i in response['Table']['StorageDescriptor']['Columns']]
print(', '.join(cols))"
```

## Add particular partitions:

- From Airflow you can set the following variables that will affect to the `{target_table}` tables in `dags/braze/config.py` with this format: `2022-01-20 17:58:07`
    - `{target_table}_start_at`
    - `{target_table}_end_at`

- From Airflow you can also run just a particular table using this DAG. You need to set up the variable `list_of_tables` in the UI as a JSON with this keys:
```
       {
        "source_schema": "xxxxxx",
        "source_table": "xxxxxx",
        "target_schema": "xxxxxx",
        "target_table": "xxxxxx",
        "partition_column": "xxxxxx",
        "column_list": 'xxx, xxxxx, xxxxxx, xxxxxxx, "xxxxx"'
       }
```
Take into account that even if you set `list_of_tables` you still need to pass `{target_table}_start_at` and/or `{target_table}_end_at`. Otherwise you would just insert the last 2 hours of data.
