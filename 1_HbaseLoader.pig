register 'udfs.py' using jython as py

rmf HbaseLoad
rmf DataLoad
rmf databag

--Load Hbase data and convert columnar data to bag
data = load 'hbase://dummy' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('A:*', '-loadKey true') AS (id:chararray, data:map[]);

-- data:
--(zzzz111111-mj,[20130106-SX#0,20130106-ST#16,20130106-SC#19])
--(zzzzmichaelzzzz-dm,[20130106-SX#0,20130106-ST#98,20130106-SC#26])

-Flatten UDF returned items
databag = foreach data generate id, FLATTEN(py.bag_of_tuples(data));
STORE databag INTO 'databag';

-- databag:
--(zzzzmichaelzzzz-dm,20130106-SC,26)
--(zzzzmichaelzzzz-dm,20130106-ST,98)

--Load bag abd parse date to get in required format
A = LOAD 'databag' USING PigStorage(',' )AS line:CHARARRAY;

--Use Pig regex to separate out data. 
B = FOREACH A GENERATE FLATTEN(REGEX_EXTRACT_ALL(line, '^(.+?)\\-(.+?)\\s(.+?)\\-(.)(.)\\s(.+)$')) AS (UserID:CHARARRAY,Zip:CHARARRAY,Date:CHARARRAY,Operation:CHARARRAY,Type:CHARARRAY,Value:CHARARRAY);
--STORE databag INTO 'hdfs://server/databag';
STORE B into 'DataLoad';


-- Split Operational columns 
Splitter = load 'DataLoad' as (UserID:chararray, Zip:chararray,Date:CHARARRAY, Operation:chararray, Type:chararray, Value:INT);

--Ensure Types are ordered
A = order Splitter by Type asc;
B = group A by (UserID, Zip, Date, Operation);
-- 3 Indicates number of columns to break out
-- STRSPLIT(string, regex, limit)
-- BagToString(bag, '+') -> 'a+b+c'

C = foreach B {
  Breaker = STRSPLIT(BagToString(A.Value, ','),',',3);
  generate flatten(group) as (UserID, Zip, Date, Operation), 
    Breaker.$2 as OpT:float, Breaker.$0 as OpC:float, Breaker.$1 as OpX:float;
}

--Final Output
STORE C into 'HbaseLoad';
--( zzzzmichaelzzzz,dm,20130106,S,98,26)

fs -getmerge HbaseLoad HbaseLoad
