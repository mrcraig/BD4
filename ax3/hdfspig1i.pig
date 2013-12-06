in = LOAD '/user/1003648b/index' USING PigStorage(' ') AS (timestamp:chararray, revID:long, artID:long);

h = FOREACH in GENERATE artID, revID, timestamp;

filtered = FILTER h BY timestamp>'$starttime'  AND timestamp<'$endtime'  AND artID is not null AND revID is not null;

p2 = FOREACH filtered GENERATE (artID, revID);

DUMP p2;
