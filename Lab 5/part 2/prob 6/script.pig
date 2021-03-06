
register '/usr/lib/pig/piggybank.jar';
MOVIES = LOAD '/home/cloudera/Desktop/MovieDataSet/movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (movieId:int, title:chararray, genres:chararray);

RATING = LOAD '/home/cloudera/Desktop/MovieDataSet/rating.txt' as (userId: int, movieId:int , rating:int , timestamp:chararray);
filterR = FILTER RATING BY rating ==5;

tokenMOVIES = FOREACH MOVIES GENERATE movieId, title, TOKENIZE(genres,'|');
flattenMOVIES= FOREACH tokenMOVIES GENERATE movieId, title, FLATTEN($2);
filterMOVIES = FILTER flattenMOVIES BY $2 =='Adventure';



joinMR= JOIN filterMOVIES BY $0, filterR BY $1;
result = FOREACH joinMR GENERATE $0 as MovieId, $2 as Genre, $5 as Rating, $1 as Title;
distinctResult = DISTINCT result;
orderMR = ORDER distinctResult by movieId;
top20 = LIMIT orderMR 20;
STORE top20 INTO '/home/cloudera/Desktop/MovieDataSet/output/' USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');