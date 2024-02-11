-- Load the dataset
AccessLog = LOAD '/home/ds503/shared_folder/accesslogshort.txt' USING PigStorage(',')
            AS (record_id:int, ByWho_ID:int, WhatPage_ID:int, TypeOfAccess:chararray, AccessTime:chararray);

-- Map access to a key-value pair with ByWho_ID as key and WhatPage_ID as value
MappedAccess = FOREACH AccessLog GENERATE ByWho_ID, WhatPage_ID;

-- Group by ByWho_ID to get a bag of pages that the user has accessed
GroupedByViewer = GROUP MappedAccess BY ByWho_ID;

-- Calculate total number of accesses and distinct accesses
Favorites = FOREACH GroupedByViewer {
    allPages = MappedAccess.WhatPage_ID;
    distinctPages = DISTINCT allPages;
    GENERATE group AS ByWho_ID, COUNT(allPages) AS total_accesses, COUNT(distinctPages) AS unique_accesses;
}

-- Store the result
STORE Favorites INTO 'e_out' USING PigStorage(',');
