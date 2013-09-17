#!/bin/bash

SQL_FILE=test.sql
DSN=hana
USR=SYSTEM
PWD=manager
SCHEMA=hana_test

function create_sql_file {
    echo "CREATE SCHEMA \"$SCHEMA\"" > $SQL_FILE
    echo "DROP TABLE \"$SCHEMA\".\"foo\" CASCADE" >> $SQL_FILE
    echo "CREATE ROW TABLE \"$SCHEMA\".\"foo\"( \"id\" VARCHAR (100) not null, \"p1\" VARCHAR (100) null, \"p2\" VARCHAR (100) null, primary key (\"id\"))" >> $SQL_FILE
    echo "DROP TABLE \"$SCHEMA\".\"moon_bar\" CASCADE" >> $SQL_FILE
    echo "CREATE ROW TABLE \"$SCHEMA\".\"moon_bar\" (\"id\" varchar(100), \"str\" varchar(100), \"int\" INTEGER, \"dec\" DOUBLE, \"bol\" VARCHAR(5), \"wen\" VARCHAR(100), \"arr\" VARCHAR(1000), \"obj\" VARCHAR(1000), \"mark\" varchar(100), \"seneca\" varchar(1000))" >> $SQL_FILE
    echo "DROP SEQUENCE \"$SCHEMA\".\"moon_bar_id\""  >> $SQL_FILE
    echo "CREATE SEQUENCE \"$SCHEMA\".\"moon_bar_id\" START WITH 1"  >> $SQL_FILE
    echo "DROP SEQUENCE \"$SCHEMA\".\"foo_id\""  >> $SQL_FILE
    echo "CREATE SEQUENCE \"$SCHEMA\".\"foo_id\" START WITH 1"  >> $SQL_FILE
}

function execute_sql_file {
    cat $SQL_FILE | isql $DSN $USR $PWD -w
}

function delete_sql_file {
    if [[ -f $SQL_FILE ]]
    then
        rm $SQL_FILE
    fi
}

echo
echo "This script will drop tables and sequences in schema: $SCHEMA"
read -p "Are you sure? [y/n] " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]
then
    create_sql_file
    execute_sql_file
    delete_sql_file
else
    echo "Quiting..."
fi


