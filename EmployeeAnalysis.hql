SELECT * FROM employees_partitioned
WHERE year(join_date) > 2015;

SELECT department, AVG(salary) AS avg_salary
FROM employees_partitioned
GROUP BY department;

SELECT * FROM employees_partitioned
WHERE project = 'Alpha';

SELECT job_role, COUNT(*) AS employee_count
FROM employees_partitioned
GROUP BY job_role;

SELECT e1.*
FROM employees_partitioned e1
JOIN (
    SELECT department, AVG(salary) AS avg_salary
    FROM employees_partitioned
    GROUP BY department
) e2
ON e1.department = e2.department
WHERE e1.salary > e2.avg_salary;

SELECT department, COUNT(*) AS employee_count
FROM employees_partitioned
GROUP BY department
ORDER BY employee_count DESC
LIMIT 1;

SELECT * FROM employees_partitioned
WHERE emp_id IS NOT NULL 
AND name IS NOT NULL
AND age IS NOT NULL
AND job_role IS NOT NULL
AND salary IS NOT NULL
AND project IS NOT NULL
AND join_date IS NOT NULL
AND department IS NOT NULL;

SELECT e.emp_id, e.name, e.age, e.job_role, e.salary, e.project, e.join_date, d.location
FROM employees_partitioned e
JOIN departments d
ON e.department = d.department_name;

SELECT emp_id, name, department, salary, 
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank
FROM employees_partitioned;

SELECT emp_id, name, department, salary
FROM (
    SELECT emp_id, name, department, salary, 
           DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
    FROM employees_partitioned
) ranked
WHERE rank <= 3;
