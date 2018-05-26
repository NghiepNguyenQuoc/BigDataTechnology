users = LOAD '/home/cloudera/lab5/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
flt = FILTER users by (gender == 'M') and (occupation == 'lawyer');
groups = GROUP flt by occupation;
counts = FOREACH groups GENERATE group, COUNT(flt);
STORE counts INTO '3_output';
