
--vishwanath 212/2013

--PART A CALCULATE AVERAGES,PERFORMANCE GROUPS AND IMBALANCES

--PARAMETERS
--pig -param OPERATIONTYPE='ImapGetFolderOperation' -param  NUMBEROFGUIDS=10000 2_MEDS_Main.pig

--SET DEFAULTS
%default NUMBEROFGUIDS  '.*';
%default OPERATIONTYPE 'ImapGetFolderOperation';

--REMOVE PREVIOUS OUTPUT

rmf X_Sample
rmf X_UserList
rmf X_Performance_Groups
rmf X_Averages
rmf X_perfgroup
rmf X_AllGuids
rmf X_Imbalanced
rmf SourceGuids
rmf X_SourceGuids
rmf X_ImbalancedUsers
rmf X_FinalTargets
rmf MEDS_Final

--LOAD CONVERTED DATA
a = LOAD 'testdataload' USING PigStorage (' ') AS (
		Guid:chararray,
		Zip:chararray,
		Date:chararray,
		Operation:chararray,
		OpT:float,
		OpC:int,
		OpX:int
	);

--SAMPLE FOR TESTING
b = SAMPLE a 0.01;

--GUID ZIP
Uniques = GROUP a by (Guid,Zip);
Uniques2 = FOREACH Uniques GENERATE FLATTEN(group);
Uniques3 = FOREACH Uniques2 GENERATE Guid;
Uniques4 = GROUP Uniques3 by Guid;
Uniques5 = FOREACH Uniques4 GENERATE group as UGuid, COUNT_STAR(Uniques3)as dupes;
Uniques6 = FILTER Uniques5 by dupes == 1;


--REMOVE GUIDS ON MULTIPLE ZIPS
--b1 = FILTER a by Zip matches '$ZIPMATCH';
NoDupes = JOIN a by Guid,Uniques6 by UGuid;
NoDupes1 = FOREACH NoDupes GENERATE Guid,Zip,Date,Operation,OpT,OpC,OpX;


--LOAD NUMBER OF CORES
c = LOAD 'Core.txt' AS(
CZip:chararray,
NumCores:float
);

--ADD COUNT OF CORES TO ZIPS
Performance_GroupswithCore = JOIN NoDupes1 by Zip,c by CZip;

--CALCULATE PERFORMANCE GROUPS
calc_perfgroup = FOREACH Performance_GroupswithCore generate Guid,Zip,Date,Operation,FLOOR(LOG10(OpT+1)/LOG10(2)) as perfomgroup,OpC as zipcount,OpT as ziptime,NumCores as NumCores1;
group_calc_perfgroup = group calc_perfgroup by (Zip,Operation,perfomgroup,NumCores1);
--STORE calc_perfgroup into 'X_UserList';
totals = FOREACH group_calc_perfgroup generate group,COUNT(calc_perfgroup) as count,SUM(calc_perfgroup.zipcount)as zipcount,SUM(calc_perfgroup.ziptime)as ziptime;
PG = FOREACH totals GENERATE FLATTEN(group),count,zipcount,ziptime;
Performance_Groups = ORDER PG by Zip,Operation,perfomgroup;
--STORE Performance_Groups into 'X_Performance_Groups';

--COMPUTE AVERAGES
Core_Load = JOIN Performance_Groups by Zip,c by CZip;
Core_Join = FOREACH Core_Load GENERATE Zip,Operation,perfomgroup,count,zipcount,ziptime,NumCores;
Core_Join_Grp = group Core_Join by (Operation,perfomgroup);
Core_Total = FOREACH Core_Join_Grp GENERATE group,COUNT(Core_Join.count) as tot_zips,SUM(Core_Join.count) as tot_users,SUM(Core_Join.NumCores)as Tot_Cores,SUM(Core_Join.count)/SUM(Core_Join.NumCores) as users_per_core,SUM(Core_Join.ziptime)/SUM(Core_Join.NumCores) as time_per_core,SUM(Core_Join.zipcount)/SUM(Core_Join.NumCores) as cnt_per_core,SUM(Core_Join.ziptime)/SUM(Core_Join.count) as time_per_user,SUM(Core_Join.zipcount)/SUM(Core_Join.count) as count_per_user;
Averages = FOREACH Core_Total GENERATE FLATTEN(group),tot_zips,tot_users,Tot_Cores,users_per_core,time_per_core,cnt_per_core,time_per_user,count_per_user;
Flats3 = ORDER Averages by Operation,perfomgroup;
--STORE Flats3 into 'X_Averages';

Imbalance2 = FOREACH (JOIN Performance_Groups by (PG_operation,PG_PerformanceGroup),Flats3 by (operation,pg)) GENERATE Load_PG::PG_operation,Load_PG::PG_PerformanceGroup,Load_PG::zip,Load_PG::count,Load_PG::ziptime,Load_PG::count - Load_Averages::users_per_core *Load_PG::PG_NumCores as Imbal_users,Load_PG::ziptime - Load_Averages::time_per_core  * Load_PG::PG_NumCores as 
Imbal_tm,(Load_Averages::time_per_core*Load_PG::PG_NumCores)/(Load_PG::ziptime+1) as weight,Load_PG::PG_NumCores as total_cores;

Load_PG =  FOREACH Performance_Groups GENERATE $0 AS zip:chararray,$1 AS PG_operation:chararray,$2 AS PG_PerformanceGroup:int,$3 AS PG_NumCores:float,$4 AS count:int,$5 AS zipcount:int,$6 AS ziptime:float;
Load_Averages = FOREACH Flats3 GENERATE $0 AS operation:chararray,$1 AS pg:int,$2 AS tot_zips:int,$3 AS tot_users:float,$4 AS tot_cores:int,$5 AS 	users_per_core:float,$6 AS time_per_core:float,$7 AS count_per_core:float,$8 AS time_per_user:float,$9 AS count_per_user:float;


--COMPUTE IMBALANCES BY COMPARING PG TO AVERAGES
Load_PG = LOAD 'X_Performance_Groups'  AS (

		zip:chararray,
		PG_operation:chararray,
		PG_PerformanceGroup:int,
		PG_NumCores:float,
		count:int,
		zipcount:int,
		ziptime:float
);

Load_Averages = LOAD 'X_Averages' AS (

		operation:chararray,
		pg:int,
		tot_zips:int,
		tot_users:float,
		tot_cores:int,
		users_per_core:float,
		time_per_core:float,
		count_per_core:float,
		time_per_user:float,
		count_per_user:float
);

Imbalance = FOREACH (JOIN Load_PG by (PG_operation,PG_PerformanceGroup), Load_Averages by (operation,pg)) GENERATE Load_PG::PG_operation,Load_PG::PG_PerformanceGroup,Load_PG::zip,Load_PG::count,Load_PG::ziptime,Load_PG::count - Load_Averages::users_per_core *Load_PG::PG_NumCores as Imbal_users,Load_PG::ziptime - Load_Averages::time_per_core  * Load_PG::PG_NumCores as 
Imbal_tm,(Load_Averages::time_per_core*Load_PG::PG_NumCores)/(Load_PG::ziptime+1) as weight,Load_PG::PG_NumCores as total_cores;

--FILTER OPERATION TYPE
Imbalance_Filter = FILTER Imbalance by $0 == '$OPERATIONTYPE';

--STORE IMBALANCES


Imbalance2 = ORDER Imbalance_Filter by  $0,$2,$1;
STORE Imbalance2 INTO 'X_Imbalanced';


--TOP IMBALANCES
GroupImbalance = GROUP Imbalance2 by (PG_operation,zip);


--PART B. SOURCE AND TARGET COMPUTATION

--IDENTIFY SOURCE BY APPLYING SOURCE CONSTRAINTS
FilterSource = FILTER Imbalance2 by (weight > 0) AND (weight <0.5) AND (PG_PerformanceGroup >5) AND (PG_PerformanceGroup < 15);
SourceGuids = FOREACH calc_perfgroup GENERATE $0,$1,$3,$4;
SourceGuids2 = FOREACH (JOIN FilterSource by (PG_operation,zip,PG_PerformanceGroup),SourceGuids by (Operation,Zip,perfomgroup)) GENERATE $0,$1,$2,$9;
STORE SourceGuids2 INTO 'X_AllGuids';

-- SUM THE NUMBER OF IMBALANCE USERS HERE

-- IF NUBEROFGUIDS 

-- INSERT IF CONDITION HERE

-- REMOVE THE ZIPS WHICH QUALIFY AS SOURCE FROM TARGET CONSIDERATION

TargetZipsFilter1 = FOREACH Imbalance2 GENERATE $2;
TargetZipsFilter2 = DISTINCT TargetZipsFilter1;
TargetZipsFilter3 = FOREACH FilterSource GENERATE $2;
TargetZipsFilter4 = DISTINCT TargetZipsFilter3;
TargetZipsFilter5 = UNION TargetZipsFilter2,TargetZipsFilter4;
TargetZipsFilter6 = GROUP TargetZipsFilter5 by $0;
TargetZipsFilter7 = FOREACH TargetZipsFilter6 GENERATE FLATTEN(group), COUNT($1) as Counter;
TargetZipsFilter8 = FILTER TargetZipsFilter7 BY Counter <2;
TargetImbalances = FOREACH (JOIN Imbalance2 by $2,TargetZipsFilter8 by $0) GENERATE $0,$1,$2,$3,$4,$5,$6,$7,$8;


--IDENTIFY TARGET BY APPLYING TARGET CONSTRAINTS
FilterTarget = FILTER TargetImbalances by (Imbal_users <0) AND (Imbal_tm <1)AND (PG_PerformanceGroup >5) AND (PG_PerformanceGroup < 15);
SumTargetsZip = Group FilterTarget by (PG_operation,zip);
SumTargetsPG = Group FilterTarget by (PG_operation,zip,PG_PerformanceGroup);
SumTargetsZipFlat = FOREACH SumTargetsZip GENERATE FLATTEN (group), SUM(FilterTarget.Imbal_tm);
SumTargetsPGFlat = FOREACH SumTargetsPG GENERATE FLATTEN (group), SUM(FilterTarget.Imbal_tm);

--CALCULATE TARGET CAPACITY BY ZIP
TargetCapacity = FOREACH (JOIN SumTargetsZipFlat by (zip),FilterSource by (zip))GENERATE $0 as Operation,$1 as zip,$2 as Capacity;

--% SPLIT OF TARGET IMBALANCE WITHIN PERFORMANCE GROUPS
PercentSplit = FOREACH (JOIN FilterTarget by (zip,PG_PerformanceGroup),SumTargetsPGFlat by (zip,PG_PerformanceGroup)) GENERATE $0,$1,$6;
PercentSplit2 = GROUP PercentSplit by ($0,$1);
PercentSplit3 = FOREACH PercentSplit2  GENERATE FLATTEN(group),SUM(PercentSplit.Imbal_tm);
SplitTarget = FOREACH (JOIN FilterTarget by PG_PerformanceGroup,PercentSplit3 by PG_PerformanceGroup) GENERATE $0,$1,$2,(long)$6 as Imbal1,(long)$11 as PGImbal,$6*100/$11 as SplitTargetImbalance;

--% SPLIT OF SOURCE IMBALANCE
GroupFilterSource = GROUP FilterSource by PG_operation;
GroupFilterSourceUsers= FOREACH GroupFilterSource GENERATE FLATTEN (group)as Operation, (int)SUM(FilterSource.Imbal_users) as TotUsers;
SplitSource = FOREACH(JOIN GroupFilterSourceUsers by Operation,FilterSource by PG_operation) GENERATE $0,$3,$4,$7*100/$1 as SplitSource;

--IMBALANCED USERS SPLIT BY COUNT 
ImbalancedUsers = FOREACH SplitSource GENERATE Operation,zip,PG_PerformanceGroup,$NUMBEROFGUIDS*SplitSource/100 as UserSplit;
STORE ImbalancedUsers INTO 'X_ImbalancedUsers';

--DISTRIBUTE TARGETS
FinalTargets = JOIN SplitTarget by (PG_operation,PG_PerformanceGroup),ImbalancedUsers by (Operation,PG_PerformanceGroup);
FinalTargets2 = FOREACH FinalTargets GENERATE $0,$1,$2,($5/100*$9) as Users;
FinalTargets3 = GROUP FinalTargets2 by ($0,$1,$2);
FinalTargets4 = FOREACH FinalTargets3 GENERATE FLATTEN(group), (long)SUM(FinalTargets2.Users);
STORE FinalTargets4 INTO 'X_FinalTargets';



--MERGE ALL OUTPUT
fs -getmerge X_Averages X_Averages
fs -getmerge X_Imbalanced X_Imbalanced
fs -getmerge X_FinalTargets X_FinalTargets


--RUN NEXT SCRIPT
RUN 3_MEDS_SourceGuids.pig

