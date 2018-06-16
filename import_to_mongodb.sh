#!/bin/bash

start=`date +%s`

cd all-data/meta/

mongoimport -d big_data_project -c industries --type csv --file all_sites.csv --headerline

cd ../csv/

for csv_file in *.csv;
do
    mongoimport --db big_data_project -c "$csv_file" --type csv --file "$csv_file" --headerline
done;

cd ../../

end=`date +%s`

runtime=$((end-start))

echo "==> Time elapse $runtime seconde for imports to database"
