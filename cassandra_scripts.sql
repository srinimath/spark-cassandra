-- create keyspace to add tables
create keyspace spark_interview with replication = {'class':'SimpleStrategy','replication_factor':1};

-- alter keyspace spark_interview with replication = {'class':'SimpleStrategy','replication_factor':1};
-- describe keyspaces

-- create tables
create table spark_tbl_1(tid text primary key, cm15 text);
create table spark_tbl_2(tid text primary key, amount double);
create table spark_tbl_join(tid text primary key, cm15 text, amount double);
-- describe tables

-- insert
insert into spark_tbl_1(tid,cm15) values('tid1','c1');
insert into spark_tbl_1(tid,cm15) values('tid2','c2');

insert into spark_tbl_2(tid,amount) values('tid1',30);
insert into spark_tbl_2(tid,amount) values('tid2',100);