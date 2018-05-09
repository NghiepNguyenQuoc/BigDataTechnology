register '/usr/lib/pig/piggybank.jar';
MOVIES = LOAD '/home/cloudera/Desktop/MovieDataSet/movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (movieId:int, title:chararray, genres:chararray);

RATING = LOAD '/home/cloudera/Desktop/MovieDataSet/rating.txt' as (userId: int, movieId:int , rating:int , timestamp:chararray);
filterR = FILTER RATING BY rating ==5;

RATING = LOAD '/home/cloudera/Desktop/MovieDataSet/rating.txt' AS (userId: int, movieId: int, timestamp:chararray);

USERS = LOAD '/home/cloudera/Desktop/MovieDataSet/users.txt' USING PigStorage('|') AS (userId: int, age: int, gender:chararray, occupation: chararray, zipCode: chararray);
filterUser = FILTER USERS BY gender=='M' AND occupation=='programmer';

tokenMOVIES = FOREACH MOVIES GENERATE movieId, title, TOKENIZE(genres,'|');
flattenMOVIES= FOREACH tokenMOVIES GENERATE movieId, title, FLATTEN($2);
filterMOVIES = FILTER flattenMOVIES BY $2 =='Adventure';

joinMR= JOIN filterMOVIES BY $0, filterR BY $1;

result = FOREACH joinMR GENERATE $0 as movieId, $2 as Genre, $5 as Rating, $1 as Title;

distinctResult = DISTINCT result;
orderMR = ORDER distinctResult by movieId;
top20 = LIMIT orderMR 20;

joinUR = JOIN filterUser BY userId, RATING BY userId;
joinUR20 = JOIN joinUR BY RATING::movieId, top20 BY movieId;

newDataSet = FOREACH joinUR20 GENERATE RATING::movieId AS movieId, top20::Title AS title, RATING::timestamp AS timeStamp;
groupAll = GROUP newDataSet ALL;
count = FOREACH groupAll GENERATE COUNT(newDataSet);
dump count



