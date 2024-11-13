import json

import pyspark.sql.functions as F
from pyspark.sql.window import Window


def rename_columns(df):

    df = df.select([F.col(c).alias(c.lower().replace(' ', '_')) for c in df.columns])

    df = df.withColumnRenamed("automatic_extinguishing_sytem_type",
                                "automatic_extinguishing_system_type") \
            .withColumnRenamed("automatic_extinguishing_sytem_failure_reason",
                                "automatic_extinguishing_system_failure_reason") \
            .withColumnRenamed("automatic_extinguishing_sytem_perfomance",
                                "automatic_extinguishing_system_performance")
    return df

def change_inconsistent_labels(df):

    df = df.withColumn('zipcode', F.regexp_replace('zipcode', '\-\w+[0-9]{3,}', ''))
    df = df.withColumn('primary_situation', F.regexp_replace('primary_situation', '[*-]', ''))
    df = df.withColumn('primary_situation', F.trim('primary_situation'))

    mappings_file_path = "/opt/spark/scripts/spark/mappings.json"
    with open(mappings_file_path, 'r', encoding='utf-8') as f:
        mappings = json.load(f)

    mutual_aid_mapping = mappings['mutual_aid']

    df = df.replace(mutual_aid_mapping, subset = ['mutual_aid'])

    action_taken_primary_mapping = mappings['action_taken_primary']
    df = df.withColumn('action_taken_primary', F.regexp_replace('action_taken_primary', ' -', ''))
    df = df.replace(action_taken_primary_mapping, subset = ['action_taken_primary'])

    action_taken_secondary_mapping = mappings['action_taken_secondary']
    df = df.withColumn('action_taken_secondary', F.regexp_replace('action_taken_secondary', ' -', ''))
    df = df.replace(action_taken_secondary_mapping, subset = ['action_taken_secondary'])

    property_use_mapping = mappings['property_use']
    df = df.withColumn('property_use', F.regexp_replace('property_use', ' -', ''))
    df = df.replace(property_use_mapping, subset = ['property_use'])

    ignition_factor_primary_mapping = mappings['ignition_factor_primary']
    df = df.withColumn('ignition_factor_primary', F.regexp_replace('ignition_factor_primary', ' -', ''))
    df = df.replace(ignition_factor_primary_mapping, subset = ['ignition_factor_primary'])

    df = df.replace('UU Undetermined', None, subset = ['ignition_factor_primary'])

    ignition_factor_secondary_mapping = mappings['ignition_factor_secondary']
    df = df.withColumn('ignition_factor_secondary', F.regexp_replace('ignition_factor_secondary', '-', ''))
    df = df.replace(ignition_factor_secondary_mapping, subset = ['ignition_factor_secondary'])

    heat_source_mapping = mappings['heat_source']
    df = df.withColumn('heat_source', F.regexp_replace('heat_source', '-', ''))
    df = df.replace(heat_source_mapping, subset = ['heat_source'])

    item_first_ignited_mapping = mappings['item_first_ignited']
    df = df.withColumn('item_first_ignited', F.regexp_replace('item_first_ignited', '-', ''))
    df = df.replace(item_first_ignited_mapping, subset = ['item_first_ignited'])
    df = df.withColumn('human_factors_associated_with_ignition', F.regexp_replace('human_factors_associated_with_ignition', '[Â§/]', ''))

    structure_type_mapping = mappings['structure_type']
    df = df.withColumn('structure_type', F.regexp_replace('structure_type', '-', ''))
    df = df.replace(structure_type_mapping, subset = ['structure_type'])

    structure_status_mapping = mappings['structure_status']
    df = df.withColumn('structure_type', F.regexp_replace('structure_type', '-', ''))
    df = df.replace(structure_status_mapping, subset = ['structure_type'])

    fire_spread_mapping = mappings['fire_spread']
    df = df.withColumn('fire_spread', F.regexp_replace('fire_spread', '-', ''))
    df = df.replace(fire_spread_mapping, subset = ['fire_spread'])

    no_flame_spread_mapping = mappings['no_flame_spread']
    df = df.replace(no_flame_spread_mapping, subset = ['no_flame_spread'])

    detectors_present_mapping = mappings['detectors_present']
    df = df.withColumn('detectors_present', F.regexp_replace('detectors_present', '-', ''))
    df = df.replace(detectors_present_mapping, subset = ['detectors_present'])

    detector_type_mapping = mappings['detector_type']
    df = df.withColumn('detector_type', F.regexp_replace('detector_type', '-', ''))
    df = df.replace(detector_type_mapping, subset = ['detector_type'])
    df = df.withColumn('detector_operation', F.regexp_replace('detector_operation', '-', ''))

    detector_effectiveness_mapping = mappings['detector_effectiveness']
    df = df.withColumn('detector_effectiveness', F.regexp_replace('detector_effectiveness', '-', ''))
    df = df.replace(detector_effectiveness_mapping, subset = ['detector_effectiveness'])

    detector_failure_reason_mapping = mappings['detector_failure_reason']
    df = df.replace(detector_failure_reason_mapping, subset = ['detector_failure_reason'])
    df = df.withColumn('automatic_extinguishing_system_present', F.regexp_replace('automatic_extinguishing_system_present', '-', ''))

    aes_type_mapping = mappings['aes_type']
    df = df.withColumn('automatic_extinguishing_system_type', F.regexp_replace('automatic_extinguishing_system_type', '-', ''))
    df = df.replace(aes_type_mapping, subset = ['automatic_extinguishing_system_type'])
    df = df.withColumn('automatic_extinguishing_system_performance', F.regexp_replace('automatic_extinguishing_system_performance', '-', ''))

    aes_failure_reason_mapping = mappings['aes_failure_reason']
    df = df.withColumn('automatic_extinguishing_system_failure_reason', F.regexp_replace('automatic_extinguishing_system_failure_reason', '-', ''))
    df = df.replace(aes_failure_reason_mapping, subset = ['automatic_extinguishing_system_failure_reason'])

    return df

def filter_data(df):

    #Filter data that are not fire related incident
    df = df.filter(F.col("primary_situation").startswith("1"))
    return df

def remove_duplicates(df):
    # Drop duplicates based on incident_number and keep latest data only
    w = Window.partitionBy('incident_number').orderBy(F.desc('data_as_of'))
    df = df.withColumn('rank', F.row_number().over(w))
    df = df.filter(df.rank == 1).drop(df.rank)
    df = df.drop('id')
    return df

def feature_engineering(df):
    # Extract latitude and longitude using regex
    df = df.withColumn('latitude', F.regexp_extract('point', '\(-?\d+\.\d+', 0))
    df = df.withColumn('longitude', F.regexp_extract('point', '-?\d+\.\d+\)', 0))

    # Removing open and closing brackets
    df = df.withColumn('latitude', F.regexp_replace('latitude', '\(', ''))
    df = df.withColumn('longitude', F.regexp_replace('longitude', '\)', ''))

    # Casting latitude and longitude into float type
    df = df.withColumn('latitude', F.col("latitude").cast("float"))
    df = df.withColumn('longitude', F.col("longitude").cast("float"))

    #Drop point column
    df = df.drop('point')
    df = df.withColumn('response_time_in_minutes', (F.unix_timestamp(F.col("arrival_dttm")) - F.unix_timestamp(F.col("alarm_dttm")))/60) # In minutes
    df = df.withColumn('response_time_in_minutes', F.round(F.col('response_time_in_minutes'), 2))

    df = df.withColumn('suppression_time_in_minutes',(F.unix_timestamp(F.col("close_dttm")) - F.unix_timestamp(F.col("arrival_dttm")))/60) # In minutes
    df = df.withColumn('suppression_time_in_minutes', F.round(F.col('suppression_time_in_minutes'), 2))

    return df

def transform_data(df):
    df = rename_columns(df)
    df = change_inconsistent_labels(df)
    df = filter_data(df)
    df = remove_duplicates(df)
    df = feature_engineering(df)
    return df
