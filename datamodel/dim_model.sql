WITH dim_incident_details AS (
	SELECT
  		MD5(incident_number || 'incident_details') AS incident_details_id,
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
)

WITH dim_location AS (
	SELECT
		MD5(incident_number || 'location') AS location_id,
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
)

