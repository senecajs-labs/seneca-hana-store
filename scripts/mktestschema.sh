#!/bin/bash

SQL_FILE=test.sql
DSN=hana
USR=SYSTEM
PWD=manager
SCHEMA=hana_test

echo "CREATE SCHEMA \"$SCHEMA\"" > $SQL_FILE
echo "DROP TABLE \"$SCHEMA\".\"foo\" CASCADE" >> $SQL_FILE
echo "CREATE ROW TABLE \"$SCHEMA\".\"foo\"( \"id\" VARCHAR (100) not null, \"p1\" VARCHAR (100) null, \"p2\" VARCHAR (100) null, primary key (\"id\"))" >> $SQL_FILE
echo "DROP TABLE \"$SCHEMA\".\"moon_bar\" CASCADE" >> $SQL_FILE
echo "CREATE ROW TABLE \"$SCHEMA\".\"moon_bar\" (\"id\" varchar(100), \"str\" varchar(100), \"int\" INTEGER, \"dec\" DOUBLE, \"bol\" VARCHAR(5), \"wen\" VARCHAR(100), \"arr\" VARCHAR(1000), \"obj\" VARCHAR(1000), \"mark\" varchar(100), \"seneca\" varchar(1000))" >> $SQL_FILE

cat $SQL_FILE | isql $DSN $USR $PWD -w
