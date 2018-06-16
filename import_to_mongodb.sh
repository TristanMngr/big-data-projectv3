#!/bin/bash

start=`date +%s`

cd fake-all-data-small/meta/

if !(mongoimport -d big_data_project -c industries --type csv --file all_sites.csv --headerline); then
    /Users/Thibault/mongodb-osx-x86_64-3.6.5/bin/mongoimport -d big_data_project -c industries --type csv --file all_sites.csv --headerline
fi

cd ../csv/

for csv_file in *.csv;
do
    if !(mongoimport --db big_data_project -c "$csv_file" --type csv --file "$csv_file" --headerline); then
        /Users/Thibault/mongodb-osx-x86_64-3.6.5/bin/mongoimport --db big_data_project -c "$csv_file" --type csv --file "$csv_file" --headerline
    fi
done;

cd ../../

end=`date +%s`

runtime=$((end-start))

echo "==> Time elapse $runtime seconde for imports to database"
