-- Load data into a relation
faceinpage_data = LOAD '/home/ds503/shared_folder/MyPage.txt' USING PigStorage(',') AS (facein_id:int, name:chararray, nationality:chararray, country_code:int, hobby:chararray);

-- Filter based on nationality. Let's say we pick 'American'.
filtered_users = FILTER faceinpage_data BY nationality == 'Korean';

-- Project only name and hobby
result = FOREACH filtered_users GENERATE name, hobby;

-- Display the result (for testing purposes)
--DUMP result;

STORE result INTO 'a_out' USING PigStorage(',');
