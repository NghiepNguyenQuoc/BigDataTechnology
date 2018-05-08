USERS = LOAD '/home/cloudera/Desktop/MovieDataSet/users.txt' USING PigStorage('|') AS (UserId: int, age: int, gender: chararray, occupation: chararray, zipCode:chararray);
filterLawyers = FILTER USERS BY occupation == 'lawyer' AND gender=='M';
groupLawyers = GROUP filterLawyers BY occupation;
coutLawyers = FOREACH groupLawyers GENERATE group, COUNT($1);
dump coutLawyers;
