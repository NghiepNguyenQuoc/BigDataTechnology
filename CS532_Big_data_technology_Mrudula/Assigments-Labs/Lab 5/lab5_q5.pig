A = LOAD '/home/cloudera/lab5/movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (movieId:int, title:chararray, genres:chararray);
flt = FILTER A BY STARTSWITH(title, 'A') OR STARTSWITH(title, 'a');
genres = FOREACH flt GENERATE FLATTEN(TOKENIZE(genres,'|')) as genres;
group_genres = GROUP genres by genres;
result = FOREACH group_genres GENERATE group, COUNT($1);
orders = ORDER result by $0;
STORE orders INTO '5_output';