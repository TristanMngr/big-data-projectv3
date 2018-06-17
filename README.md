# integration to Mongodb

## Requires
+ pymongo : `python -m pip install pymongo`
+ mongodb

## Getting started
+ Start your server mongodb `mongodb`
+ Go to the folder project and type `python run_migration.py`

## Problems
+ If you can't start the script it is probably because your command `mongoimport` is not in your $PATH
	+ go in `import_energies.sh` and `import_industries` and replace the line import by your path to run `mongoimport`
