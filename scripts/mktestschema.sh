#!/bin/bash

SQL_FILE=test.sql
DSN=hana
USR=SYSTEM
PWD=manager
SCHEMA=hana_test

function create_sql_file {
    echo "CREATE SCHEMA \"$SCHEMA\"" > $SQL_FILE
    echo "DROP TABLE \"$SCHEMA\".\"foo\" CASCADE" >> $SQL_FILE
    echo "CREATE ROW TABLE \"$SCHEMA\".\"foo\"( \"id\" NVARCHAR(36) not null, \"p1\" NVARCHAR (100) null, \"p2\" NVARCHAR (100) null, primary key (\"id\"))" >> $SQL_FILE

    echo "DROP TABLE \"$SCHEMA\".\"moon_bar\" CASCADE" >> $SQL_FILE
    echo "CREATE ROW TABLE \"$SCHEMA\".\"moon_bar\" (\"id\" NVARCHAR(36), \"str\" NVARCHAR(100), \"int\" INTEGER, \"dec\" DOUBLE, \"bol\" NVARCHAR(5), \"wen\" NVARCHAR(100), \"arr\" NVARCHAR(1000), \"obj\" NVARCHAR(1000), \"mark\" NVARCHAR(100), \"seneca\" NVARCHAR(1000))" >> $SQL_FILE

    echo "DROP TABLE \"$SCHEMA\".\"mapping_test\" CASCADE" >> $SQL_FILE
    echo "CREATE ROW TABLE \"$SCHEMA\".\"mapping_test\" (\"id\" NVARCHAR(36), \"ctimestamp\" TIMESTAMP, \"cseconddate\" SECONDDATE, \"cdate\" DATE, \"ctime\" TIME, \"cdouble\" DOUBLE, \"creal\" REAL, \"cdecimal\" DECIMAL, \"csmalldecimal\" SMALLDECIMAL, \"cbigint\" BIGINT, \"cinteger\" INTEGER, \"csmallint\" SMALLINT, \"ctinyint\" TINYINT, \"cnclob\" NCLOB, \"cnvarchar\" NVARCHAR, \"cclob\" CLOB, \"cvarchar\" VARCHAR, \"cblob\" BLOB, \"cvarbinary\" VARBINARY, \"seneca\" NVARCHAROH(1000))" >> $SQL_FILE

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


