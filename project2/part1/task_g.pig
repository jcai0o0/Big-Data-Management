-- Load access log and print first few records
access_log = LOAD '/home/ds503/shared_folder/accesslogshort.txt' USING PigStorage(',') AS (AccessId:int, ByWho:int, WhatPage:chararray, TypeOfAccess:chararray, AccessTime:int);

-- Load mypage and print first few records
mypage = LOAD '/home/ds503/shared_folder/mypageshort.txt' USING PigStorage(',') AS (user_id:int, name:chararray, nationality:int, countryCode:int, hobby:int);

-- Extract the timestamp and group by user_id
grouped_data = GROUP access_log BY ByWho;
min_access_time = FOREACH grouped_data GENERATE group AS ByWho, MIN(access_log.AccessTime) AS min_time;

-- Set the THRESHOLD for 90 days.
%default THRESHOLD 100000;

-- Filter users based on the THRESHOLD
filtered_users = FILTER min_access_time BY min_time > $THRESHOLD;

-- Join with mypage to get the names of the users
final_result = JOIN filtered_users BY ByWho, mypage BY user_id;

-- Extract relevant fields
output_data = FOREACH final_result GENERATE filtered_users::ByWho AS ByWho, mypage::name AS name;

dump output_data;

-- the problem is below
-- Store the result
STORE output_data INTO 'g_out' USING PigStorage(',');
