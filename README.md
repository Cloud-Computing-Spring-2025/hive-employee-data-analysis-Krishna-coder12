# Employee and Department Data Analysis

### **Problem Statement**

You are provided with two datasets: employees.csv and departments.csv. The objective of this assignment is to analyze employee and department data using Hive queries.

## Setup and Execution

### 1.**Start the Hadoop Cluster**

Run the following command to start the Hadoop cluster:

```bash
docker compose up -d
```

### 2. **Setup Hive Environment**

```bash
docker exec -it hive-server /bin/bash
hive
```

### 3. **Creating Hive Tables**

Creating Tables:

Employee Table,
```sql
CREATE TABLE temp_employees (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary DOUBLE,
    project STRING,
    join_date STRING,
    department STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

Department Table,
```sql
CREATE TABLE departments (
    dept_id INT,
    department_name STRING,
    location STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

Employee_Partitioned Table,
```sql
CREATE TABLE employees_partitioned (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary DOUBLE,
    project STRING,
    join_date STRING
)
PARTITIONED BY (department STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET;
```

### 4. **Loaded the csv file manually in the hive**

Downloaded both CSV files and upload manually in the hive in the following path '/user/hive/.......'.
And Run the following command in the Query Tab.

```sql
LOAD DATA INPATH '/user/hive/employees.csv' INTO TABLE employees;
LOAD DATA INPATH '/user/hive/departments.csv' INTO TABLE departments;
```

### 5. **Insert Data into Partitioned Table**

Used ALTER TABLE to add partitions dynamically,

```sql
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE employees_partitioned PARTITION(department)
SELECT emp_id, name, age, job_role, salary, project, join_date, department 
FROM temp_employees;
```

To verify partitions, run the following command:

```sql
SHOW PARTITIONS employees_partitioned;
```

### 6. **Performed All  Queries**

1) Query 1:
   
```sql
SELECT * FROM employees_partitioned
WHERE year(join_date) > 2015;
```

2) Query 2:
   
```sql
SELECT department, AVG(salary) AS avg_salary
FROM employees_partitioned
GROUP BY department;
```

3) Query 3:

```sql
SELECT * FROM employees_partitioned
WHERE project = 'Alpha';
```

4) Query 4:
   
```sql
SELECT job_role, COUNT(*) AS employee_count
FROM employees_partitioned
GROUP BY job_role;
```

5) Query 5:
   
```sql
SELECT e1.*
FROM employees_partitioned e1
JOIN (
    SELECT department, AVG(salary) AS avg_salary
    FROM employees_partitioned
    GROUP BY department
) e2
ON e1.department = e2.department
WHERE e1.salary > e2.avg_salary;
```

6) Query 6:
   
```sql
SELECT department, COUNT(*) AS employee_count
FROM employees_partitioned
GROUP BY department
ORDER BY employee_count DESC
LIMIT 1;
```

7) Query 7:
   
```sql
SELECT * FROM employees_partitioned
WHERE emp_id IS NOT NULL 
AND name IS NOT NULL
AND age IS NOT NULL
AND job_role IS NOT NULL
AND salary IS NOT NULL
AND project IS NOT NULL
AND join_date IS NOT NULL
AND department IS NOT NULL;
```

8) Query 8:
   
```sql
SELECT e.emp_id, e.name, e.age, e.job_role, e.salary, e.project, e.join_date, d.location
FROM employees_partitioned e
JOIN departments d
ON e.department = d.department_name;
```

9) Query 9:
   
```sql
SELECT emp_id, name, department, salary, 
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank
FROM employees_partitioned;
```

10) Query 10:
   
```sql
SELECT emp_id, name, department, salary
FROM (
    SELECT emp_id, name, department, salary, 
           DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
    FROM employees_partitioned
) ranked
WHERE rank <= 3;
```

### 8. **Exported Query Results**

Save Output to HDFS:

```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/Query....'
```
And perform similarly for all the Query Analysis .

### 9. **Moving data from hive-server**

Access the Hive Server  container:

```bash
docker exec -it hive-server /bin/bash
```

### 10. **Copy Output from HDFS to Local OS**

To copy the output from HDFS to your local machine:

1. Use the following command to copy from HDFS:
    ```bash
    hdfs dfs -get /user/hive/output /tmp/output
    ```

2. use Docker to copy from the container to your local machine:
   ```bash
   exit 
   ```
    ```bash
    docker cp hive-server:/tmp/output /workspaces/hive-employee-data-analysis-Krishna-coder12/
    ```
3. Commit and push to your repo to the github.
