linesOfText= LOAD '/home/cloudera/test.txt' AS (line:chararray);
tokenBag = FOREACH linesOfText GENERATE TOKENIZE (line);
flatBag = FOREACH tokenBag GENERATE flatten($0);
charGroup = GROUP flatBag by token;
counts = FOREACH charGroup GENERATE group, COUNT(flatBag);
STORE counts INTO '1_output';