
rmf X_FilterSourceGuids

register 'ZipSourceSequence.py' using jython as py
register 'GuidSourceSequencer.py' using jython as py

-- LOAD ALL GUIDS AND IMBALANCEDUSERS TO EXTRACT SELECTED GUIDS
SourceZips = LOAD 'X_ImbalancedUsers' AS (operation:chararray,zip:chararray, pg:int,count:int);
SourceGuids = LOAD 'X_AllGuids' AS (operation1:chararray,pg1:int,zip1:chararray,guid:chararray);

--ZIP SEQUENCER JYTHON SCRIPT BASICALLY ITERATES THROUGH THE COUNT OF USERS THAT ZIP CAN HAVE FROM 0 - ZIPCOUNT.
Generator = FOREACH SourceZips GENERATE (py.zipsourcesequence(operation,zip,pg,count));
GenFlat = FOREACH Generator GENERATE FLATTEN($0);
GenFlat2 = FOREACH GenFlat GENERATE $0, FLATTEN($1),FLATTEN($2),FLATTEN($3);


-- GUID SEQUENCER JYTHON SCRIPT BASICALLY ITERATES THROUGH THE COUNT OF GUIDS THAT ZIP AND PG CAN HAVE FROM 0 - GROUP CAPACITY.
GuidGenerator1 = GROUP SourceGuids by (operation1,pg1,zip1);
GuidGenerator2 = FOREACH GuidGenerator1 GENERATE (group),(SourceGuids.guid) as countguids;
GuidGenerator3 = FOREACH GuidGenerator2 {
generate group,py.guidsourcesequencer(countguids) as record: {group: (pg1:int,rank: int,guid:chararray)};
};
GuidGenerator4 = FOREACH GuidGenerator3 GENERATE FLATTEN ($0),FLATTEN ($1);

-- SPREAD THE SOURCE GUIDS ACCORDING TO BREAKS AS CALCULATED BY % SPLIT OF SOURCE IMBALANCE
SpreadSource = FOREACH (JOIN GuidGenerator4 by ($0,$2,$1,$3), GenFlat2 by (operation,zip,pg,count)) GENERATE $0,$1,$2,$3,$4;

STORE SpreadSource into 'X_FilterSourceGuids';


fs -getmerge X_FilterSourceGuids X_FilterSourceGuids
--RUN NEXT SCRIPT
