in = LOAD '/user/bd4-project1/enwiki-20080103-sample' USING PigStorage(' ') AS (field, artID:long, revID:long, title:chararray, timestamp:chararray);

h = FOREACH in GENERATE artID, revID, title, timestamp;

filtered = FILTER h BY timestamp>'$time'  AND artID is not null AND revID is not null;

p2 = FOREACH filtered GENERATE artID, timestamp;

g = GROUP p2 BY artID;

p3 = FOREACH g {
	GENERATE group, MAX(p2.timestamp);
};

DUMP p3;
