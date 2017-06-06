# Incremental-Hive-Update

<b>Author :Abhijeet Sarkar </b>

<b>Incrementally Updating a Hive Table Using Sqoop and an External Table</b>


It is common to perform a one-time ingestion of data from an operational database to Hive and then require incremental updates periodically. Currently, Hive does not support SQL Merge for bulk merges from operational systems. Instead, you must perform periodic updates as described in this section.

Use the following steps to incrementally update Hive tables from operational database systems:

<br>1.**Ingest**: Complete data movement from the operational database (base_table) followed by change or update of changed records only (incremental_table).
<br>2.**Reconcile**: Create a single view of the base table and change records (reconcile_view) to reflect the latest record set.
<br>3.**Compact**: Create a reporting table (reporting_table) from the reconciled view.
<br>4.**Purge**: Replace the base table with the reporting table contents and delete any previously processed change records before the next data ingestion cycle


 
#### INGEST

<br/>Step1: check the existing table in your DBMS database
```sql
I created a dummy source table on mysql
mysql -u root -p 
create database if not exists training;
use training;
CREATE TABLE authors (id INT, name VARCHAR(20), email VARCHAR(20), last_modified date);
INSERT INTO authors (id,name,email,last_modified) VALUES(1,"Vivek","xuz@abc.com",'2017-04-22');
INSERT INTO authors (id,name,email,last_modified) VALUES(3,"Rock","pyz@abc.com",'2017-04-20');
INSERT INTO authors (id,name,email,last_modified) VALUES(2,"Priya","p@gmail.com",'2017-04-21');
INSERT INTO authors (id,name,email,last_modified) VALUES(3,"Oriya","o@gmail.com",'2017-04-22');
INSERT INTO authors (id,name,email,last_modified) VALUES(3,"Tom","tom@yahoo.com",'2017-04-20');
INSERT INTO authors (id,name,email,last_modified) VALUES(1,"Rom","rom@yahoo.com",'2017-04-22');
```


<br />Step2: creating base table in hdfs, using sqoop:\
```
sqoop import-all-tables \
    -m 1 \
    --connect jdbc:mysql://localhost/training \
    --username=root \
    --password=cloudera \
    --warehouse-dir=/user/hive/warehouse \
    --hive-import \
    --map-column-hive last_modified=DATE
```
<br/>RESULT 

authors.id|authors.name	|authors.email	|authors.last_modified
----|----------|------------------|--------------
1|Vivek|xuz@abc.com	|2017-04-22
3|Rock|pyz@abc.com	|2017-04-20
2|Priya|p@gmail.com	|2017-04-21
3|Oriya|o@gmail.com	|2017-04-22
3|Tom|tom@yahoo.com	|2017-04-20
1|Rom|rom@yahoo.com	|2017-04-22
5|Tims|tim@yahoo.com	|NULL
1|Star|som@yahoo.com	|NULL
<br/>Time taken: 0.059 seconds, Fetched: 8 row(s)

------------------------ 
<p>
*** manual mysql update***
```sql
INSERT INTO authors (id,name,email,last_modified) VALUES(5,"Tims","tim@yahoo.com",'2017-04-23');
INSERT INTO authors (id,name,email,last_modified) VALUES(1,"Star","som@yahoo.com",'2017-04-23');
```
<br/>Step3.
** sqoop import of the updates into a new directory called incremental table ***
```
sqoop import --connect jdbc:mysql://localhost/training \
--username root \
-P \
--query "select * from authors where last_modified > '2017-04-22' AND \$CONDITIONS" \
--target-dir /user/hive/incremental_table -m 1
```
** checking the loaded hdfs data**
<br/>[cloudera@quickstart Desktop]$ hadoop fs -cat /user/hive/incremental_table/part*
<br/>5,Tims,tim@yahoo.com,2017-04-23
<br  />1,Star,som@yahoo.com,2017-04-23

<br/>Step4.
<br/>store above data in a hive external table

```sql
CREATE EXTERNAL TABLE incremental_table 
(id int, name string, email string, last_modified date)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/user/hive/incremental_table';
```    
</p>


#### **RECONCILE**

<br/>Step5:Reconcile: Merge both tables to keep only the updated data

```sql
CREATE VIEW reconcile_view AS
SELECT t1.* FROM
  (SELECT * FROM authors
     UNION ALL
     SELECT * from incremental_table) t
     JOIN
    (SELECT id, max(last_modified) max_modified FROM
        (SELECT * FROM authors
         UNION ALL
         SELECT * from incremental_table) tata
     GROUP BY id) t2
ON t1.id = t2.id AND t1.last_modified = t2.max_modified;
```

<br/>RESULT 

reconcile_view.id|	reconcile_view.name|	reconcile_view.email|	reconcile_view.last_modified
------------------|-------------------|---------------------|--------------------------
1|	Star|	som@yahoo.com|	2017-04-23
2|	Priya|	p@gmail.com|	2017-04-21
3|	Oriya|	o@gmail.com|	2017-04-22
5|	Tims|	tim@yahoo.com|	2017-04-23


<br/>The following are result of individual sub-queries*******

```sql
SELECT * FROM authors
         UNION ALL
         SELECT * from incremental_table
 ```        
<br/>Result

_u1|.id|	_u1.name|	_u1.email|	_u1.last_modified
----|---|---------|---------|-------
1|Vivek|	xuz@abc.com|	2017-04-22
3|	Rock|	pyz@abc.com|	2017-04-20
2|	Priya|	p@gmail.com|	2017-04-21
3|	Oriya|	o@gmail.com|	2017-04-22
3|	Tom|	tom@yahoo.com|	2017-04-20
1|	Rom|	rom@yahoo.com|	2017-04-22
5|	Tims|	tim@yahoo.com|	NULL
1|	Star|	som@yahoo.com|	NULL
5|	Tims|	tim@yahoo.com|	2017-04-23
1|	Star|	som@yahoo.com|	2017-04-23
<br/>Time taken: 14.215 seconds, Fetched: 10 row(s)

<br/>second sub query

```sql
SELECT id, max(last_modified) max_modified FROM
        (SELECT * FROM authors
         UNION ALL
         SELECT * from incremental_table) tata
     GROUP BY id;
```   

<br/>Result

|id|	max_modified|
|-----|-------|
|1|	2017-04-23|
|2|	2017-04-21|
|3|	2017-04-22|
|5|	2017-04-23|




#### **COMPACT**


<br />Step6: Compact the data

<br/>The view changes as soon as new data is introduced into the incremental table in HDFS (/user/hive/incremental_table, so create and store a copy of the view as a snapshot in time
```sql
DROP TABLE reporting_table;
CREATE TABLE reporting_table AS
SELECT * FROM reconcile_view;
```


#### **PURGE**

<p>
<br />Step7:Purge data: After you have created a reporting table, clean up the incremental updates to ensure that the same data is not read twice:

<br/>hadoop fs -rmr /user/hive/incremental_table/

<br/>Move the data into the base table -- authors

```sql
DROP table authors;
CREATE TABLE authors 
(id int, name string, email string, last_modified date) 
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY ',' 
    STORED AS TEXTFILE; 
INSERT OVERWRITE TABLE authors SELECT * FROM reporting_table;
```
</p>
<br/> <b>automate the steps to incrementally update data in Hive by using Oozie</b>
