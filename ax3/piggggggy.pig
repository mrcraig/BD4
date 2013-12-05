in = LOAD '/user/bd4-project1/enwiki-20080103-sample' USING PigStorage(' ') AS (field, artID:long, revID:long, title:chararray, timestamp:chararray);

h = FOREACH in GENERATE artID, revID, title, timestamp;

filtered = FILTER h BY timestamp>'2005-12-06T17:44:47Z'  AND timestamp<'2006-12-06T17:44:47Z' AND artID is not null AND revID is not null;

p2 = FOREACH filtered GENERATE (artID, revID);

DUMP p2;
