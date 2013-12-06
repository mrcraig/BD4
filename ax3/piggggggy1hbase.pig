REGISTER hadoop-snapshot/lib/zookeeper-3.4.5-cdh4.4.0.jar
REGISTER hadoop-snapshot/lib/hbase-0.94.6-cdh4.4.0-security.jar;
REGISTER pigUDF/src/pigUDF/pighbase.jar;
DEFINE get_artID pigUDF.ArtIDUDF();
DEFINE get_revID pigUDF.RevID();
DEFINE get_ts pigUDF.TimeUDF();
SET default_parallel 10;

in = LOAD 'hbase://BD4Project2Sample' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('WD:TITLE,WD:TS', '-loadKey true -caster HBaseBinaryConverter') AS (key:bytearray, title:chararray, ts:chararray);

h = FOREACH in GENERATE get_artID(key) as artID, get_revID(key) as revID, get_ts(ts);

filtered = FILTER h BY timestamp>'2005-12-06T17:44:47Z' AND timestamp<'2006-12-06T17:44:47Z' AND artID is not null AND revID is not null;

p2 = FOREACH filtered GENERATE artID, revID;

DUMP p2;
