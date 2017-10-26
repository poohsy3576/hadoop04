select count(*) from emp;

Select  job,  ename,  sal,  rank( )  over  (order  by  sal  desc) all_rank,
       rank( )  over  (partition  by  job  order  by  sal  desc) job_rank
  from emp;
  
  Select  job,  ename,  sal,  rank( )  over  (order  by  sal  desc) all_rank,
       dense_rank( )  over  (order  by  sal  desc) dense_rank
  from emp;
  
  Select  job,  ename,  sal,  rank( )  over  (order  by  sal  desc) all_rank,
       row_number( )  over  (order  by  sal  desc) row_number
	   from emp;
	   
select mgr, ename, sal, sum(sal) over (partition by mgr) mgr_sum from emp;

SELECT DEPTNO, ENAME, SAL, CUME_DIST() OVER (PARTITION BY DEPTNO ORDER BY SAL DESC) as CUME_DIST FROM EMP;

select mgr, ename, sal, sum(sal) over (partition by mgr order by sal range unbounded preceding) mgr_sum 
  from emp;

 SELECT MGR, ENAME, SAL, MAX(SAL) OVER (PARTITION BY MGR) as MGR_MAX FROM EMP;


SELECT MGR, ENAME, SAL 
FROM (SELECT MGR, ENAME, SAL, MAX(SAL) OVER (PARTITION BY MGR) as IV_MAX_SAL FROM EMP) WHERE SAL = IV_MAX_SAL ;

SELECT DEPTNO, ENAME, SAL, FIRST_VALUE(ENAME) OVER (PARTITION BY DEPTNO ORDER BY SAL DESC, ENAME ASC ROWS UNBOUNDED PRECEDING) as RICH_EMP FROM EMP;

SELECT ENAME, SAL, 
       COUNT(*) OVER (ORDER BY SAL RANGE BETWEEN 50 PRECEDING AND 150 FOLLOWING) as SIM_CNT 
	   FROM EMP;
	   
SELECT DEPTNO, ENAME, SAL, FIRST_VALUE(ENAME) OVER (PARTITION BY DEPTNO ORDER BY SAL DESC ROWS UNBOUNDED PRECEDING) as DEPT_RICH FROM EMP;


SELECT DEPTNO, ENAME, SAL, LAST_VALUE(ENAME) OVER (PARTITION BY DEPTNO ORDER BY SAL DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as DEPT_POOR FROM EMP; 

SELECT ENAME, HIREDATE, SAL, LAG(SAL, 2) OVER (ORDER BY HIREDATE) as PREV_SAL 
  FROM EMP ;
SELECT ENAME, SAL, ROUND(RATIO_TO_REPORT(SAL) OVER (), 2) as R_R FROM EMP;

SELECT DEPTNO, ENAME, SAL, PERCENT_RANK() OVER (PARTITION BY DEPTNO ORDER BY SAL DESC) as P_R FROM EMP;

 SELECT ENAME, SAL, NTILE(4) OVER (ORDER BY SAL DESC) as QUAR_TILE FROM EMP;










