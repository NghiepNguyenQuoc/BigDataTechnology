
register '/usr/lib/pig/piggybank.jar';
MOVIES = LOAD '/home/cloudera/Desktop/MovieDataSet/movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (MovieId:int, title:chararray, genres:chararray);
tokenMOVIES = FOREACH MOVIES GENERATE MovieId, title, TOKENIZE(genres,'|');
flattenMOVIES= FOREACH tokenMOVIES GENERATE MovieId, title, FLATTEN($2);
filterMOVIES = FILTER flattenMOVIES BY title MATCHES 'A.*' OR title MATCHES 'a.*';
gpMOVIES = GROUP filterMOVIES by $2;
countMOVIES = FOREACH gpMOVIES GENERATE group, COUNT($1);
dump countMOVIES;
