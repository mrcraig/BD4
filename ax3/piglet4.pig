in = LOAD '/user/bd4-project1/enwiki-20080103-sample' USING PigStorage(' ') AS (field, artID:long, revID:long, title:chararray, timestamp:chararray);

h = FOREACH in GENERATE artID, revID, title, timestamp;

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
