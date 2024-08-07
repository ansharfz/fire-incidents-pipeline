--Creates the dim_incident_details table
CREATE TABLE dim_incident_details
AS
SELECT
	incident_number,
	call_number,
	suppression_units,
	suppression_personnel,
	battalion,
	ems_units,
    ems_personnel,
    other_units,
    other_personnel,
    first_unit_on_scene,
    primary_situation,
    mutual_aid,
    action_taken_primary,
    action_taken_secondary,
    action_taken_other,
    area_of_fire_origin,
    ignition_cause,
    ignition_factor_primary,
    ignition_factor_secondary,
    heat_source,
    item_first_ignited,
    human_factors_associated_with_ignition,
    floor_of_fire_origin,
    fire_spread,
    no_flame_spread,
    data_loaded_at,
    data_as_of
FROM fire_incident
WITH DATA;

--Create the location dimension table
CREATE TABLE dim_location
AS
SELECT
	incident_number,
  	address,
  	city,
  	zipcode,
  	station_area,
  	box,
  	supervisor_district,
  	neighborhood_district,
  	structure_type,
  	structure_status,
  	longitude,
  	latitude
FROM fire_incident
WITH DATA;

--Create the detectors dimension table
CREATE TABLE dim_detectors
AS
SELECT
	incident_number,
  	detectors_present,
  	detector_alerted_occupants,
  	detector_type,
  	detector_operation,
  	detector_effectiveness,
  	detector_failure_reason
FROM fire_incident
WITH DATA;

--Create the automatic extinguishing system dimension table
CREATE TABLE dim_automatic_extinguishing_system
AS
SELECT
	incident_number,
  	automatic_extinguishing_system_present,
  	automatic_extinguishing_system_type,
  	automatic_extinguishing_system_performance,
  	automatic_extinguishing_system_failure_reason
FROM fire_incident
WITH DATA;

-- Adds the primary key to the dimension tables
ALTER TABLE dim_incident_details ADD incident_details_id 
	INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY;
ALTER TABLE dim_location ADD location_id 
	INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY;
ALTER TABLE dim_detectors ADD detector_id 
	INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY;
ALTER TABLE dim_automatic_extinguishing_system ADD automatic_extinguishing_system_id 
	INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY;

--Creates the fire incident fact table
CREATE TABLE fact_fire_incident
AS
SELECT
	fire_incident.incident_number AS fact_incident_number,
	exposure_number,
  	incident_date,
	alarm_dttm,
	arrival_dttm,
	close_dttm,
	estimated_property_loss,
	estimated_contents_loss,
	fire_fatalities,
	fire_injuries,
	civilian_fatalities,
	civilian_injuries,
	number_of_alarms,
	number_of_floors_with_minimum_damage,
	number_of_floors_with_significant_damage,
	number_of_floors_with_heavy_damage,
	number_of_floors_with_extreme_damage,
	number_of_sprinkler_heads_operating,
	response_time_in_minutes,
	suppression_time_in_minutes,
	dim_incident_details.incident_details_id,
	dim_location.location_id,
	dim_detectors.detector_id,
	dim_automatic_extinguishing_system.automatic_extinguishing_system_id
FROM fire_Incident
INNER JOIN dim_incident_details 
	ON dim_incident_details.incident_number = fire_incident.incident_number
INNER JOIN dim_location 
	ON dim_location.incident_number = fire_incident.incident_number
INNER JOIN dim_detectors
	ON dim_detectors.incident_number = fire_incident.incident_number
INNER JOIN dim_automatic_extinguishing_system
	ON dim_automatic_extinguishing_system.incident_number = fire_incident.incident_number
WITH DATA;

--Drop the incident number column from the dimension tables
ALTER TABLE dim_incident_details DROP incident_number;
ALTER TABLE dim_location DROP incident_number;
ALTER TABLE dim_detectors DROP incident_number;
ALTER TABLE dim_automatic_extinguishing_system DROP incident_number;
