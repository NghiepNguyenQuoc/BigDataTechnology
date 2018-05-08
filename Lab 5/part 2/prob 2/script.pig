USERS = LOAD '/home/cloudera/Desktop/MovieDataSet/users.txt' USING PigStorage('|') AS (UserId: int, age: int, gender: chararray, occupation: chararray, zipCode:chararray);
filterLawyers = FILTER USERS BY occupation == 'lawyer' AND gender=='M';
orderList = ORDER filterLawyers by age;
oldestLawyers = LIMIT orderList 1;
gpoldestLawyers =GROUP oldestLawyers BY UserId;
oldestLawyersID = FOREACH gpoldestLawyers GENERATE group;
dump oldestLawyersID;

