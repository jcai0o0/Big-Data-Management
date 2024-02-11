-- Load the datasets
AccessLog_data = LOAD '/home/ds503/shared_folder/accesslogshort.txt' USING PigStorage(',') AS (access_id:int, by_who:int, what_page:int, type_of_access:chararray, access_time:int);

associates = LOAD '/home/ds503/shared_folder/friendsshort.txt' USING PigStorage(',')
             AS (associate_id:int, personA_ID:int, personB_ID:int, date_of_friendship:int, description:chararray);


FaceInPage = LOAD '/home/ds503/shared_folder/mypageshort.txt' USING PigStorage(',')
             AS (PersonID:int, PersonName:chararray, Nationality:chararray, CountryCode:chararray, Hobby:chararray);

-- Join Associates with FaceInPage to get names
AssociatesWithNames = JOIN associates BY personA_ID, FaceInPage BY PersonID;

-- Filter out those who accessed their friend's FaceInPage
FilteredAccesses = JOIN AssociatesWithNames BY (personA_ID, personB_ID) LEFT, AccessLog_data BY (by_who, what_page);
NoAccessToFriends = FILTER FilteredAccesses BY access_id IS NULL;

-- Project required columns
Result = FOREACH NoAccessToFriends GENERATE personA_ID, PersonName;

-- Remove duplicates
DistinctResult = DISTINCT Result;

-- Store the result
STORE DistinctResult INTO 'f_out' USING PigStorage(',');
