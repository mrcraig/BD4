in = LOAD '/user/1003648b/index' USING PigStorage(' ') AS (timestamp:chararray, artID:long, revID:long);
h = FOREACH in GENERATE artID, revID, timestamp;

filtered = FILTER h BY timestamp>'$time'  AND artID is not null AND revID is not null;

p2 = FOREACH filtered GENERATE artID, timestamp;

g = GROUP p2 BY artID;

p3 = FOREACH g {
	GENERATE group, MAX(p2.timestamp);
};

DUMP p3;
