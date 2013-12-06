in = LOAD '/user/1003648b/index' USING PigStorage(' ') AS (timestamp:chararray, artID:long, revID:long);

h = FOREACH in GENERATE artID, revID, title, timestamp;

filtered = FILTER h BY timestamp>'$starttime' AND timestamp<'$endtime' AND artID is not null AND revID is not null;

p2 = FOREACH filtered GENERATE artID, revID;

g =  GROUP p2 BY artID;

p3 = FOREACH g {
	count = COUNT (p2.artID);
	revs = FOREACH p2 GENERATE revID;
	GENERATE group, count, revs;
};

DUMP p3;
