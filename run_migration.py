import pymongo
import subprocess
import time

from pymongo import MongoClient

def add_energies_to_industry(industry_document_id, energy_collection):
    db.industries.update_one(
        { "SITE_ID": industry_document_id },
        { "$set": { "ENERGIES": list(energy_collection.find({}))}}, upsert=True )

def run_migration():
    # call script to run import to mongo
    subprocess.Popen(["bash", "import_to_mongodb.sh"]).wait()
    print "==> Move energies collections to Industry, wait ..."

    start_timer_all_data = time.time()
    for industry_document in db.industries.find():
        start_timer_one_indutry = time.time()
        industry_document_id = industry_document['SITE_ID']
        energy_collection_name = str(industry_document_id) + ".csv"
        energy_collection = db[energy_collection_name]

        add_energies_to_industry(industry_document_id, energy_collection)
        end_timer_one_indutry = time.time()
        elapsed_time_one_industry = end_timer_one_indutry - start_timer_one_indutry

        print ("==> Energies added to industry site_id : " + str(industry_document_id) +
        " in " + str(elapsed_time_one_industry)[:5])
        # remove collection, we don't need it anymore
        energy_collection.drop()
    # run your code
    end_timer_all_data = time.time()
    elapsed_time_all_data = end_timer_all_data - start_timer_all_data
    print "==> All data added in " + str(elapsed_time_all_data)[:5] + " secondes"



client = MongoClient()
client = MongoClient('localhost', 27017)

db = client.big_data_project

if 'big_data_project' not in client.database_names():
    run_migration()
else:
    print '==> Script already started, delete big_data_project database first'
