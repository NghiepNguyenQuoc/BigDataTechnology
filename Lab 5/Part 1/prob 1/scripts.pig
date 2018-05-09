file = LOAD '/home/cloudera/Desktop/word_count.txt' AS (line: chararray);
tokenBag= FOREACH file GENERATE TOKENIZE(line);
flagBag= FOREACH tokenBag GENERATE flatten($0);
groupBag = GROUP flagBag by $0;
counts= FOREACH groupBag GENERATE group, COUNT(flagBag);
dump counts
