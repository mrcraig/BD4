SET default_parallel 20;
in = LOAD '/user/bd4-project1/enwiki-20080103-sample' USING PigStorage(' ');
filtered = FILTER in BY ($0 == 'REVISION');
title = FOREACH filtered GENERATE $3;
lim = LIMIT title 10;
DUMP lim;
