users= LOAD '/home/cloudera/lab5/user.csv' USING PigStorage(',') AS (name:chararray, age:int);
flt = FILTER users by (age >=18) and (age <=25);
pages = LOAD '/home/cloudera/lab5/pages.csv' USING PigStorage(',') AS (user:chararray, url:chararray);
joined = JOIN flt by name, pages by user;
urls = GROUP joined by url;
counts = FOREACH urls GENERATE $0, COUNT($1);
top5 = LIMIT counts 5;
STORE top5 INTO '2_output';