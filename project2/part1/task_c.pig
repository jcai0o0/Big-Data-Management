-- Load the FaceInPage data
faceinpage_data = LOAD '/home/ds503/shared_folder/mypageshort.txt' USING PigStorage(',') AS (facein_id:int, name:chararray, nationality:chararray, country_code:int, hobby:chararray);

-- Map each user's nationality with a value of 1
MappedData = FOREACH faceinpage_data GENERATE nationality, 1 AS Count;

-- Group the data by Nationality (this acts as a combiner in MapReduce)
GroupedByNationality = GROUP MappedData BY nationality;

-- Sum the values for each nationality to get the total count of FaceInPage per country
NationalityCounts = FOREACH GroupedByNationality GENERATE group AS nationality, SUM(MappedData.Count) AS TotalFaceInPages;

-- Store the result
STORE NationalityCounts INTO 'c_out' USING PigStorage();
