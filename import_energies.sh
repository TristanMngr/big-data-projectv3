#!/bin/bash
# path = $1
# csv_file = $2

cd $1
mongoimport --db big_data_project -c "$2" --type csv --file "$2" --headerline
