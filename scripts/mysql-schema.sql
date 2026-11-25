CREATE DATABASE IF NOT EXISTS project3;
USE project3;

CREATE TABLE IF NOT EXISTS dept (
    deptno INT PRIMARY KEY,
    dname VARCHAR(20),
    loc VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS emp (
    empno INT PRIMARY KEY,
    ename VARCHAR(20),
    job VARCHAR(20),
    mgr INT NULL,
    hiredate DATE,
    sal DECIMAL(10, 2),
    comm DECIMAL(10, 2) NULL,
    deptno INT,
    img VARCHAR(50),
    CONSTRAINT fk_emp_dept FOREIGN KEY (deptno) REFERENCES dept(deptno),
    CONSTRAINT fk_emp_manager FOREIGN KEY (mgr) REFERENCES emp(empno)
);

LOAD DATA LOCAL INFILE 'dept.csv'
REPLACE INTO TABLE dept
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@deptno, @dname, @loc)
SET deptno = TRIM(@deptno),
    dname = TRIM(@dname),
    loc = TRIM(@loc);

SET FOREIGN_KEY_CHECKS = 0;

LOAD DATA LOCAL INFILE 'emp.csv'
REPLACE INTO TABLE emp
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@empno, @ename, @job, @mgr, @hiredate, @sal, @comm, @deptno, @img)
SET empno = TRIM(@empno),
    ename = TRIM(@ename),
    job = TRIM(@job),
    mgr = NULLIF(TRIM(@mgr), ''),
    hiredate = STR_TO_DATE(TRIM(@hiredate), '%e-%b-%Y'),
    sal = TRIM(@sal),
    comm = NULLIF(TRIM(@comm), ''),
    deptno = TRIM(@deptno),
    img = TRIM(@img);

SET FOREIGN_KEY_CHECKS = 1;
