users = LOAD '/home/cloudera/lab5/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
flt = FILTER users by (gender == 'M') and (occupation == 'lawyer');
orders = ORDER flt by age desc;
oldest = LIMIT orders 1;
result = FOREACH oldest GENERATE userId;
STORE result INTO '4_output';