in = LOAD '/user/bd4-project1/enwiki-20080103-sample' USING PigStorage(' ') AS (field, artID:long, revID:long, title:chararray, timestamp:chararray);

h = FOREACH in GENERATE artID, revID, title, timestamp;

filtered = FILTER h BY timestamp>'$starttime'  AND timestamp<'$endtime'  AND artID is not null AND revID is not null;

p2 = FOREACH filtered GENERATE (artID, revID);

DUMP p2;
