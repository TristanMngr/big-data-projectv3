# Integration to Mongodb

## Requires
+ pymongo : `python -m pip install pymongo` or `pip install pymongo`
+ mongodb

## Getting started
+ Clone this repository
+ Start your MongoDB server `mongod`
+ Go to the project directory and run the migration by typing `python run_migration.py`

## Problems
+ If you can't start the script it is probably because the command `mongoimport` is not in your $PATH
	+ go in `import_energies.sh` and `import_industries.sh` and replace the line import by your path to run `mongoimport`
