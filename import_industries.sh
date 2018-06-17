#!/bin/bash

cd $1
mongoimport -d big_data_project -c industries --type csv --file all_sites.csv --headerline
