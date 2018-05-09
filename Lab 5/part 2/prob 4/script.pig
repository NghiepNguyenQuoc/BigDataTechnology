
register '/usr/lib/pig/piggybank.jar';
MOVIES = LOAD '/home/cloudera/Desktop/MovieDataSet/movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (MovieId:int, title:chararray, genres:chararray);

RATING = LOAD '/home/cloudera/Desktop/MovieDataSet/rating.txt' as (userId: int, MovieId:int , rating:int , timestamp:chararray);
filterR = FILTER RATING BY rating ==5;

tokenMOVIES = FOREACH MOVIES GENERATE MovieId, title, TOKENIZE(genres,'|');
flattenMOVIES= FOREACH tokenMOVIES GENERATE MovieId, title, FLATTEN($2);
filterMOVIES = FILTER flattenMOVIES BY $2 =='Adventure';


joinMR= JOIN filterMOVIES BY $0, filterR BY $1;
result = FOREACH joinMR GENERATE $0 as MovieId, $2 as Genre, $5 as Rating, $1 as Title;
distinctResult = DISTINCT result;
orderMR = ORDER distinctResult by MovieId;
top20 = LIMIT orderMR 20;
dump top20;
