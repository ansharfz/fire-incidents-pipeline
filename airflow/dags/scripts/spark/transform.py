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

    mutual_aid_mapping = {
        'Mutual aid given': '3 Mutual aid given',
        'Mutual aid received': '1 Mutual aid received',
        'Other aid given': '5 Other aid given',
        'Automatic or contract aid received': '2 Automatic aid received',
        'Automatic aid given': '4 Automatic aid given',
        '3 Mutual aid given': '3 Mutual aid given',
        'None' : 'N None'}

    df = df.replace(mutual_aid_mapping, subset = ['mutual_aid'])

    action_taken_primary_mapping = {
        "50 Fires/rescue & Haz conditions, other": "50 Fires, rescues & hazardous conditions, other",
        "85 Enforce code": "85 Enforce codes",
        "93 Cancelled enroute": "93 Cancelled en route",
        "11 Extinguish": "11 Extinguishment by fire service personnel",
        "42 Hazmat detect/monitor/sampling & analysis": "42 HazMat detection, monitoring, sampling, & analysis",
        "80 Information/invest. & enforcement, other": "80 Information, investigation & enforcement, other",
        "87 Investigate Fire out on arrival": "87 Investigate fire out on arrival",
        "44 HazMat leak, control & containment": "44 Hazardous materials leak control & containment",
        "62 Restore sprinkler or fire protection system": "62 Restore fire protection system",
        "43 HazMat spill control and confinement": "43 Hazardous materials spill control and confinement",
        "83 Provide information to public/media": "83 Provide information to public or media"
    }
    df = df.withColumn('action_taken_primary', F.regexp_replace('action_taken_primary', ' -', ''))
    df = df.replace(action_taken_primary_mapping, subset = ['action_taken_primary'])

    action_taken_secondary_mapping = {
        "10 Fire, other": "10 Fire control or extinguishment, other",
        "11 Extinguish": "11 Extinguishment by fire service personnel",
        "42 Hazmat detect/monitor/sampling & analysis": "42 HazMat detection, monitoring, sampling, & analysis",
        "43 HazMat spill control and confinement": "43 Hazardous materials spill control and confinement",
        "44 HazMat leak, control & containment": "44 Hazardous materials leak control & containment",
        "50 Fires/rescue & Haz conditions, other": "50 Fires, rescues & hazardous conditions, other",
        "62 Restore fire alarm system": "62 Restore fire protection system",
        "79 Assess severe weather/Nat dis. Damage": "79 Assess severe weather or natural disaster damage",
        "80 Information/invest. & enforcement, other": "80 Information, investigation & enforcement, other",
        "83 Provide information to public/media": "83 Provide information to public or media",
        "85 Enforce code": "85 Enforce codes",
        "87 Investigate Fire out on arrival": "87 Investigate fire out on arrival",
        "93 Cancelled enroute": "93 Cancelled en route"
    }
    df = df.withColumn('action_taken_secondary', F.regexp_replace('action_taken_secondary', ' -', ''))
    df = df.replace(action_taken_secondary_mapping, subset = ['action_taken_secondary'])

    property_use_mapping = {
        '615 Electric generating plant': '615 Electric-generating plant'
    }

    df = df.withColumn('property_use', F.regexp_replace('property_use', ' -', ''))
    df = df.replace(property_use_mapping, subset = ['property_use'])

    ignition_factor_primary_mapping = {
        "30 Electrical failure, malfunction, othe": "30 Electrical failure, malfunction, other",
        "11 Abandoned or discarded materials or p": "11 Abandoned, discarded materials, or products",
        "13 Cuttin/welding too close to combustib": "13 Cutting or welding too close to combustible",
        "20 Mechanical failure, malfunction, othe": "20 Mechanical failure, malfunction, other",
        "58 Equipment not being operated properly": "58 Equipment not operated properly",
        "33 Short cir. arc, defect/worn insulatio": "33 Short-circuit arc from defective, worn insulation",
        "73 Outside/open fire, debris/waste dispo": "73 Outside/open fire for debris or waste disposal",
        "18 Improper container or storage procedure": "18 Improper container or storage",
        "74 Outside/open fire for warming or cook": "74 Outside/open fire for warming or cooking",
        "31 Water caused short-circuit arc": "31 Water-caused short-circuit arc",
        "51 Collision, knock down, run over, turn over": "51 Collision/knockdown/runover/turnover",
        "30L Lithium-ion battery malfunction": "30 Electrical failure, malfunction, other",
        "NN None": "UU Undetermined",
        "2": "UU Undetermined"
    }

    df = df.withColumn('ignition_factor_primary', F.regexp_replace('ignition_factor_primary', ' -', ''))
    df = df.replace(ignition_factor_primary_mapping, subset = ['ignition_factor_primary'])

    df = df.replace('UU Undetermined', None, subset = ['ignition_factor_primary'])

    ignition_factor_secondary_mapping = {
        '12 Heat source too close to combustibles.': '12 Heat source too close to combustibles.',
        '30 Electrical failure, malfunction, othe': '30 Electrical failure, malfunction, other',
        '11 Abandoned or discarded materials or p': '11 Abandoned or discarded materials or products',
        '32 Short circuit arc from mechanical dam': '32 Short-circuit arc from mechanical damage',
        '20 Mechanical failure, malfunction, othe': '20 Mechanical failure, malfunction, other',
        '18 Improper container or storage': '18 Improper container or storage procedure',
        '00 Factors contributing to ignition, other': '00 Other factor contributed to ignition',
        '33 Short cir. arc, defect/worn insulatio': '33 Short-circuit arc from defective, worn insulation',
        '52 Accidentally turned on, not turned of': '52 Accidentally turned on, not turned off',
        '57 Equipment used for not intended purpo': '57 Equipment not used for purpose intended',
        '13 Cuttin/welding too close to combustib': '13 Cutting/welding too close to combustibles',
        '73 Outside/open fire, debris/waste dispo': '73 Outside/open fire for debris or waste disposal',
        '74 Outside/open fire for warming or cook': '74 Outside/open fire for warming or cooking',
        '18 Improper container or storage procedure': '18 Improper container or storage',
    }
    df = df.withColumn('ignition_factor_secondary', F.regexp_replace('ignition_factor_secondary', '-', ''))
    df = df.replace(ignition_factor_secondary_mapping, subset = ['ignition_factor_secondary'])

    heat_source_mapping = {
        '11 Spark/ember/flame from operating equi': '11 Spark, ember, or flame from operating equipment',
        '12 Radiated/conducted heat operating equ': '12 Radiated or conducted heat from operating equipment',
        '13 Arcing': '13 Electrical arcing',
        '60 Heat; other open flame/smoking materi': '60 Heat from other open flame or smoking materials, other',
        '63 Heat from undetermined smoking materi': '63 Heat from undetermined smoking material',
        '65 Cigarette lighter': '65 Lighter: cigarette, cigar',
        '67 Warning or road flare; fusee': '67 Warning or road flare; fuse',
        '68 Backfire from internal combustion eng': '68 Backfire from internal combustion engine',
        '72 Chemical reaction': '72 Spontaneous combustion, chemical reaction',
        '97 Multiple heat sources including multi': '97 Multiple heat sources including multiple ignitions'
    }
    df = df.withColumn('heat_source', F.regexp_replace('heat_source', '-', ''))
    df = df.replace(heat_source_mapping, subset = ['heat_source'])

    item_first_ignited_mapping = {
        '96 Rubbish, trash, waste': '96 Rubbish, trash, or waste',
        '62 Flammable liquid/gas in/from engine or burner': '62 Flam. liq/gas-in/from engine or burne',
        '21 Upholstered sofa, chair, vehicle seats': '21 Upholstered sofa, chair, vehicle seat',
        '59 Rolled, wound material (paper and fabrics)': '59 Rolled, wound material (paper, fabric',
        '73 Heavy vegetation not crop, including trees': '73 Heavy vegetation no crops, inc. tre',
        '76 Cooking materials, including edible materials': '76 Cooking materials, inc. Edible materi',
        '00 Item first ignited, other': '00 Item First Ignited, Other',
        '14 Floor covering or rug/carpet/mat, surface': '14 Floor covering or rug/carpet/mat',
        '36 Curtain, blind, drapery, tapestry': '36 Curtains, blinds, drapery, tapestry',
        '11 Exterior roof covering, surface, finish': '11 Exterior roof covering or finish',
        '64 Flammable liquid/gas in container or pipe': '64 Flam liq/gas in container or pipe',
        '72 Light vegetation not crop, including grass': '72 Light vegetation no crops, inc. gra',
        '37 Goods not made up, including fabrics and yard goods': '37 Raw Goods, incl. fabrics and yarn',
        '66 Pipe, duct, conduit, hose': '66 Pipe, duct, conduit or hose',
        '61 Atomized liquid, vaporized liquid, aerosol.': '61 Atomized liq., vaporized liq.,aersol',
        '95 Film, residue, including paint & resi': '95 Film, residue, including paint and resin',
        '63 Flammable liquid/gas in/from final container': '63 Flam Liq/gas-in/from final container',
        '94 Dust, fiber, lint, including sawdust and excelsior': '94 Dust/fiber/lint. inc. sawdust, excels',
        '15 Interior wall covering excluding drapes, etc.': '15 Int. Wall cover  exclude drapes, etc.',
        '47 Tarpaulin, tent': '47 Tarpaulin or tent',
        '71 Agricultural crop, including fruits and vegetables': '71 Crop, incl. fruits and vegitables',
        '82 Transformer, including transformer fluids': '82 Transformer, including transformer fl',
        '18 Thermal, acoustical insulation within wall, partition or floor/ceiling space': '18 Insulation within structural area',
        '40 Adornment, recreational material, signs, other': '40 Adornment, recreational mat., signs,',
        '43 Sign, including outdoor signs such as billboards': '43 Sign, inc. outdoor sign/billboards',
        '74 Animal living or dead': '74 Animal, living or dead',
        '77 Feathers or fur, not on bird or anima': '77 Feathers or fur, not on bird or animal',
        '58 Palletized material, material stored on pallets.': '58 Palletized material',
        '54 Cord, rope, twine': '54 Cord, rope, twine, yarn'
        }
    df = df.withColumn('item_first_ignited', F.regexp_replace('item_first_ignited', '-', ''))
    df = df.replace(item_first_ignited_mapping, subset = ['item_first_ignited'])
    df = df.withColumn('human_factors_associated_with_ignition', F.regexp_replace('human_factors_associated_with_ignition', '[Â§/]', ''))

    structure_type_mapping = {'4 Air-supported structure': '4 Air supported structure',
                            '7 Underground structure work area': '7 Underground structure work areas'}
    df = df.withColumn('structure_type', F.regexp_replace('structure_type', '-', ''))
    df = df.replace(structure_type_mapping, subset = ['structure_type'])

    structure_status_mapping = {'0 Building status, other': '0 Other'}
    df = df.withColumn('structure_type', F.regexp_replace('structure_type', '-', ''))
    df = df.replace(structure_status_mapping, subset = ['structure_type'])

    fire_spread_mapping = {
        '00 Item first ignited, other': '00 Item First Ignited, Other',
        '11 Exterior roof covering, surface, finish': '11 Exterior roof covering or finish',
        '66 Pipe, duct, conduit, hose': '66 Pipe, duct, conduit or hose',
        '15 Interior wall covering excluding drapes, etc.': '15 Int. Wall cover  exclude drapes, etc.',
        '76 Cooking materials, including edible materials': '76 Cooking materials, inc. Edible materia',
        '94 Dust/fiber/lint. inc. sawdust, excelsi': '94 Dust/fiber/lint. inc. sawdust, excelsi',
        '96 Rubbish, trash, or waste': '96 Rubbish, trash, waste',
        '61 Atomized liq., vaporized liq.,aersol': '61 Atomized liquid, vaporized liquid, aerosol.'
    }
    df = df.withColumn('fire_spread', F.regexp_replace('fire_spread', '-', ''))
    df = df.replace(fire_spread_mapping, subset = ['fire_spread'])

    no_flame_spread_mapping = {
        'NO': '0',
        'N': '0',
        'False': '0',
        'YES': '1',
        'Y': '1',
        'True': '1'
    }
    df = df.replace(no_flame_spread_mapping, subset = ['no_flame_spread'])

    detectors_present_mapping = {
        'N None present': 'N Not present'
    }
    df = df.withColumn('detectors_present', F.regexp_replace('detectors_present', '-', ''))
    df = df.replace(detectors_present_mapping, subset = ['detectors_present'])

    detector_type_mapping = {
        '3 Combination smoke and heat in a single unit':'3 Combination smoke & heat in single unit'
    }
    df = df.withColumn('detector_type', F.regexp_replace('detector_type', '-', ''))
    df = df.replace(detector_type_mapping, subset = ['detector_type'])
    df = df.withColumn('detector_operation', F.regexp_replace('detector_operation', '-', ''))

    detector_effectiveness_mapping = {'2 Alerted occupants-occ. failed to resond' : '2 Detector alerted occupants, occupants failed to respond',
                                    '4 Failed to alert occupants' : '4 Detector failed to alert occupants'}
    df = df.withColumn('detector_effectiveness', F.regexp_replace('detector_effectiveness', '-', ''))
    df = df.replace(detector_effectiveness_mapping, subset = ['detector_effectiveness'])

    detector_failure_reason_mapping = {
        '0 -Detector failure reason, other': '0 Detector failure reason, other',
        '6 -Battery discharged or dead': '6 Battery discharged or dead',
        '5 -Battery missing or disconnected': '5 Battery missing or disconnected',
        '1 -Power fail/shutoff or disconnected dete': '1 Power failure, hardwired det. shut off, disconnect',
        '3 -Defective': '3 Defective',
        '4 -Lack of maintenance, inc. not cleaning': '4 Lack of maintenance, includes not cleaning',
        '2 -Improper installation or placement': '2 Improper installation or placement of detector'
    }
    df = df.replace(detector_failure_reason_mapping, subset = ['detector_failure_reason'])
    df = df.withColumn('automatic_extinguishing_system_present', F.regexp_replace('automatic_extinguishing_system_present', '-', ''))

    aes_type_mapping = {
        '1 Wet-pipe sprinkler' : '1 Wet-pipe sprinkler system',
        'Halogen type system': '6 Halogen-type system'
    }
    df = df.withColumn('automatic_extinguishing_system_type', F.regexp_replace('automatic_extinguishing_system_type', '-', ''))
    df = df.replace(aes_type_mapping, subset = ['automatic_extinguishing_system_type'])
    df = df.withColumn('automatic_extinguishing_system_performance', F.regexp_replace('automatic_extinguishing_system_performance', '-', ''))

    aes_failure_reason_mapping = {
        'Reason system not effective, other': '0 Reason system not effective, other',
        'System shut off' : '1 System shut off',
        'Not enough agent discharged to control the fire':'2 Not enough agent discharged to control the fire',
        'Agent discharged, but did not reach the fire': '3 Agent discharged, did not reach the fire',
        '3 Agent discharged, did not reach the fir': '3 Agent discharged, did not reach the fire',
        'Inappropriate system for the type of fire': '4 Inappropriate system for the type of fire',
        'Fire not in area protected by the system': '5 Fire not in area protected by the system',
    }
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