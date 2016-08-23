# libpq_task

Coding task:
 
- Install postgresql latest version (9.5)
- Create two databases `foo` and `bar` in your local machine
- create a table `source` with three integer columns (a,b,c) in `foo`
- and a table `dest` with three integer columns (a,b,c) in `bar`
 
Write a short c programm using libpq (https://www.postgresql.org/docs/current/static/libpq.html) that:
 
- opens a connection to the database `foo`
- fills the table source with 1 million rows 
where column a contains the numbers from 1 to 1e6
column b has a % 3
column c has a % 5
- opens a connection to the database `bar`
- copys the data from table `source` in `foo` to table `dest` in `bar`
using both connections and the postgresql copy command
