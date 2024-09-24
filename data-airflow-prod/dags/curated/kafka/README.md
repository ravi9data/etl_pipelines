# CKO: Curated Kafka Transformations

This Dag takes care of those pipelines that perform transformations to data from Raw Kafka Topics using always the same approach:

1. Read hourly partitions from the source table into a Pandas DataFrame
2. Check if those parititions are already written in the target tables and collects the
corresponding paths in S3
3. Writes the data collected in step number 1 into the target table
4. Deletes data from the target table collected in step 2.

## Implications of this pattern:
1. Avoids Null Reads.
2. Does not avoid duplicates.

## How is it works:

There is a python function (`transformation_rewriter`) in `plugins/utils/curated.py` that performs the previously explained pattern.

This function allows you to pass another python function to will perform a custom transformation to your data.
This function is provided in the config `dags/curated/kafka/config.py`, in the key `python_func`.
The only limitation for this function is that it MUST return a pandas DataFrame object.

## How to deploy my transformation?

Just write your python transformation function in the `business_logic/curated/kafka` folder and refer to it in the config with the rest of the details.

## The pipeline is meant to run hourly and insert the current hourly partition and the previous hourly parititon.

But you can run parititon ranges defining a variable name in your config with the key `config_variable_name` and then adding the range like this in Airflow:

```
	{"start_at": "2022-01-24 14:00:00"
    "end_at": "2022-01-24 16:00:00"}
```
