REGISTER hadoop-snapshot/lib/zookeeper-3.4.5-cdh4.4.0.jar
REGISTER hadoop-snapshot/lib/hbase-0.94.6-cdh4.4.0-security.jar;
SET default_parallel 20;
in = LOAD 'hbase://BD4Project2Sample' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('WD:TITLE' , '-loadKey true -caster HBaseBinaryConverter') AS (id: bytearray, title:chararray);
titles = FOREACH in GENERATE title;
lim = LIMIT titles 10;
DUMP lim;
