from .extract import extract_data, init_spark_and_schema
from .load import load_data
from .transform import transform_data

if __name__ == "__main__":
    spark, schema = init_spark_and_schema()
    df = extract_data(spark, schema)
    df = transform_data(df)
    load_data(df)
