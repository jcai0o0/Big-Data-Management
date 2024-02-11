-- Load the datasets
faceinpage_data = LOAD '/home/ds503/shared_folder/mypageshort.txt' USING PigStorage(',')
                  AS (facein_id:int, name:chararray, nationality:chararray, country_code:int, hobby:chararray);
associates = LOAD '/home/ds503/shared_folder/friendsshort.txt' USING PigStorage(',')
             AS (associate_id:int, personA_ID:int, personB_ID:int, date_of_friendship:int, description:chararray);

-- nameMapper: Produce (personID, personName)
nameMapped = FOREACH faceinpage_data GENERATE facein_id AS personID, name;

-- friendMapper: Produce symmetric relationships with a count increment of 1
friendMappedA = FOREACH associates GENERATE personA_ID AS personID, 1 AS relationshipIncrement;
friendMappedB = FOREACH associates GENERATE personB_ID AS personID, 1 AS relationshipIncrement;

friends_union = UNION friendMappedA, friendMappedB;

-- Combine the mapped relationships
allMapped = JOIN nameMapped BY personID LEFT, friends_union BY personID;
allMapped = FOREACH allMapped GENERATE nameMapped::personID AS personID, nameMapped::name AS personName, friends_union::relationshipIncrement AS relationshipIncrement;

-- SumRelationship: Group by personID and sum up the relationship count
grouped = GROUP allMapped BY personID;
summed = FOREACH grouped GENERATE group AS personID,
             FLATTEN(allMapped.personName) AS personName,
             SUM(allMapped.relationshipIncrement) AS totalRelationships;

grouped_results = GROUP summed BY (personID, personName, totalRelationships);
distinct_results = FOREACH grouped_results GENERATE FLATTEN(group);


-- Store the result
STORE distinct_results INTO 'd_out' USING PigStorage(',');
