-- Load the data
associates = LOAD '/home/ds503/shared_folder/friendsshort.txt' USING PigStorage(',')
             AS (associate_id:int, personA_ID:int, personB_ID:int, date_of_friendship:int, description:chararray);

-- Break down relationships by personA_ID and personB_ID
A_Relationships = FOREACH associates GENERATE personA_ID AS person_id;
B_Relationships = FOREACH associates GENERATE personB_ID AS person_id;

-- Union both sets of relationships
All_Relationships = UNION A_Relationships, B_Relationships;

-- Count relationships per user
GroupedRelationships = GROUP All_Relationships BY person_id;
CountedRelationships = FOREACH GroupedRelationships GENERATE group AS person_id, COUNT(All_Relationships) AS relationship_count;

-- Calculate the global average of relationships
TotalRelationships = GROUP CountedRelationships ALL;
GlobalStats = FOREACH TotalRelationships GENERATE AVG(CountedRelationships.relationship_count) AS global_avg;

-- Extract the single global_avg value from the bag
GlobalAvgValue = FOREACH GlobalStats GENERATE FLATTEN(global_avg) AS global_avg_flat;

-- Load user details
faceinpage_data = LOAD '/home/ds503/shared_folder/mypageshort.txt' USING PigStorage(',') AS (facein_id:int, name:chararray, nationality:chararray, country_code:int, hobby:chararray);

-- Join the counts with user details
JoinAboveAvgWithDetails = JOIN CountedRelationships BY person_id, faceinpage_data BY facein_id;

-- Filter users with above-average relationships
AboveAverageUsers = FILTER JoinAboveAvgWithDetails BY CountedRelationships::relationship_count > GlobalAvgValue.global_avg_flat;

-- Project the desired output
FinalOutput = FOREACH AboveAverageUsers GENERATE CountedRelationships::person_id AS associate_id, faceinpage_data::name AS name, CountedRelationships::relationship_count AS relationship_count;

-- Store the final output
STORE FinalOutput INTO 'h_out' USING PigStorage(',');
