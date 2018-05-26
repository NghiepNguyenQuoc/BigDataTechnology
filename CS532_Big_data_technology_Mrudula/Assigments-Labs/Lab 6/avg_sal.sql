select dept, avg(salary) as salary
from 
	( 
	select dept, avg(cast(substr(a_salary,2) as float)) as salary
	from employees
	group by dept
	union all
	select dept, avg(cast(substr(hour_rate,2) as float)*hours*52) as salary
	from employees
	group by dept
	) max
group by dept
order by salary desc;