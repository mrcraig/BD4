in = LOAD '/user/1003648b/index' USING PigStorage(' ') AS (timestamp:chararray, artID:long, revID:long);
h = FOREACH in GENERATE artID, revID, timestamp;

filtered = FILTER h BY timestamp>'$starttime' AND timestamp<'$endtime' AND artID is not null AND revID is not null;

p2 = FOREACH filtered GENERATE artID, revID;

g =  GROUP p2 BY artID;

p3 = FOREACH g {
	count = COUNT (p2.artID);
	GENERATE group, count;
};

o = ORDER p3 BY $1 DESC;

lim = LIMIT o '$maxno';

DUMP lim;
