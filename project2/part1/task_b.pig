-- Load the data
AccessLog_data = LOAD '/home/ds503/shared_folder/accesslogshort.txt' USING PigStorage(',') AS (access_id:int, by_who:int, what_page:int, type_of_access:chararray, access_time:int);
faceinpage_data = LOAD '/home/ds503/shared_folder/mypageshort.txt' USING PigStorage(',') AS (facein_id:int, name:chararray, nationality:chararray, country_code:int, hobby:chararray);

-- Count the number of accesses per FaceIn page
AccessCounts = FOREACH (GROUP AccessLog_data BY what_page) GENERATE group AS PageId, COUNT(AccessLog_data) AS Count;

-- Join the counts with the user details
JoinData = JOIN AccessCounts BY PageId, faceinpage_data BY facein_id;

-- Get the desired fields: Id, Name, and Nationality along with count
ResultData = FOREACH JoinData GENERATE faceinpage_data::facein_id AS facein_id, faceinpage_data::name AS name, faceinpage_data::nationality AS nationality, AccessCounts::Count AS AccessCount;

-- Order the data by count in descending order and limit to 10 records
Top10Pages = LIMIT (ORDER ResultData BY AccessCount DESC) 10;

-- Store the result
STORE Top10Pages INTO 'b_out' USING PigStorage();
