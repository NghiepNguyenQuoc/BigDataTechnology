user = LOAD '/home/cloudera/Desktop/MovieDataSet/user.csv' AS (name:chararray,age:int);
new_user = FILTER user by age>=18 and age<=25;
page = LOAD '/home/cloudera/Desktop/MovieDataSet/page.csv'  AS (name:chararray,url:chararray);
user_page= JOIN user by $0, page by $0;
group_u_p = GROUP user_page BY url;
counts = FOREACH group_u_p GENERATE group, COUNT(user_page);
count_click = order counts by $1 DESC;
limit_record_5 = LIMIT count_click 5;
dump limit_record_5;
