-- create table
CREATE EXTERNAL TABLE Emp(Name STRING, JobTitles STRING, Department STRING, FullorPartTime CHAR(1), SalaryorHourly  STRING, TypicalHours INT ,AnnualSalary STRING ,HourlyRate STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

-- load data
LOAD DATA LOCAL INPATH '/home/cloudera/workspace/Hive/data/Current_Employee_Names__Salaries__and_Position_Titles2.csv'
OVERWRITE INTO TABLE Emp;

-- validate data
SELECT * FROM Emp LIMIT 10;

-- select who earns more than 150000 in AVIATION dep
SELECT name , AnnualSalary , Department FROM Emp WHERE Department='AVIATION' AND CAST(SUBSTR(AnnualSalary,2) as INT)>150000;

--  select salary staff earns the most in each department
SELECT name , AnnualSalary , JobTitles
FROM Emp
JOIN ( SELECT Department ,MAX(CAST(SUBSTR(AnnualSalary,2) AS INT)) AS MaxSal FROM Emp GROUP BY Department ) AS maxdep on emp.Department = maxdep.Department AND CAST(SUBSTR(emp.AnnualSalary,2) AS INT) = MaxSal;


