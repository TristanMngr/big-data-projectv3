# integration to Mongodb

## Requires
+ pymongo : `python -m pip install pymongo`
+ mongodb

## Getting started
<<<<<<< HEAD
+ Start your server mongodb `mongod`
=======
+ Start your server mongodb `mongodb`
>>>>>>> 5a2960b0c64dd79b5bc6c91634926f6e661ba91f
+ Go to the folder project and type `python run_migration.py`

## Problems
+ If you can't start the script it is probably because your command `mongoimport` is not in your $PATH
	+ go in `import_energies.sh` and `import_industries` and replace the line import by your path to run `mongoimport`
