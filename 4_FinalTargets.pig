-- PLUG GUIDS TO ZIPS THEY NEED TO BE MOVED TO

register 'Iterator.py' using jython as py


rmf X_MEDS_Final
rmf X_MEDS_SummaryStats

-- LOAD % BREAK OF SOURCE GUIDS TBE PLUGGED INTO THE TARGETS 
SourceGuids = LOAD 'X_FilterSourceGuids' AS (operation:chararray,pg:int,zip:chararray,rank:int,guid:chararray);
SprayZips = LOAD 'X_FinalTargets' AS (operation1:chararray,pg1:int,zip1:chararray,count:long);

--SEQUENCER JYTHON SCRIPT ITERATES THROUGH THE COUNT OF USERS THAT A ZIP CAN HAVE FROM 0- ZipCount.
SprayZips1 = FOREACH SprayZips GENERATE (py.iterator(operation1,pg1,zip1,count));
SprayZips2 = FOREACH SprayZips1 GENERATE FLATTEN($0);
SprayZips3 = FOREACH SprayZips2 GENERATE $0, ($1),($2),FLATTEN($3);
--STORE SprayZips3 into 'SprayZips3';

-- ROUNDABOUT WAY TO PLUG EACH GUID INTO A ZIP AS PIG DOES NOT HAVE A FUNCTION TO DISTRIBUTE BASED ON COUNTS. USING RANK OPERATOR TO RANK USERS AND ZIP FROM 1- MAX AND THEN DOING A JOIN WITH SEQUENCER OUTPUT
ranked_users = RANK SourceGuids;
ranked_zips = RANK SprayZips3;

-- FINAL OUTPUT
SpreadTargets = JOIN ranked_users by  ($0,$1), ranked_zips by ($0,$1);
STORE SpreadTargets INTO 'X_MEDS_Final';
SpreadTargets2= LOAD 'X_MEDS_Final' as (rank:int,op:chararray,pg:int,source:chararray,count:int,guid:chararray,rank2:int,op2:chararray,pg2:int,targetzip:chararray,count2:int);
SpreadTargets3 = FOREACH SpreadTargets2 GENERATE ($0,$1,$3,$5,$9);
STORE SpreadTargets3 INTO 'MEDS_Final';
fs -getmerge MEDS_Final MEDS_Final
rmf MEDS_Final

-- SOURCES / NUMBER OF GUIDS
SpreadTargets4 = FOREACH SpreadTargets3 GENERATE FLATTEN(($0));
SpreadTargets5 = GROUP SpreadTargets4 BY (source);
SpreadTargets6 = FOREACH SpreadTargets5 GENERATE FLATTEN(group), COUNT(SpreadTargets4.source)as Cnt;
SummaryStats1 = ORDER SpreadTargets6 by Cnt DESC;

-- SOURCE /NUMBER OF GUIDS /TARGET
SpreadTargets7 = GROUP SpreadTargets4 BY (source,targetzip);
SpreadTargets8 = FOREACH SpreadTargets7 GENERATE FLATTEN(group), COUNT(SpreadTargets4.guid) as Cnt;
SummaryStats2 = ORDER SpreadTargets8 by source ASC,Cnt DESC;

-- SUMMARY STATS
SummaryStats = UNION SummaryStats2,SummaryStats1;
STORE SummaryStats INTO 'X_MEDS_SummaryStats';
fs -getmerge X_SummaryStats X_MEDS_SummaryStats