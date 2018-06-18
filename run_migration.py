import pymongo
import subprocess
import time
import multiprocessing
import os

from pymongo import MongoClient

# change the value of this variable to choose the directory to work on
DIR_PATH = "fake-data"

def add_energies_to_industry(industry_document_id, energy_collection, db):
    db.industries.update_one(
        { "SITE_ID": industry_document_id },
        { "$set": { "ENERGIES": list(energy_collection.find({}))}}, upsert=True )

def import_industries(dir):
    os.system("./import_industries.sh " + dir + "/meta/")

def import_energies(dir):
    counter = 0
    processes = []
    # change this variable if you want to modify the max number of concurrent import
    limit = 15
    # for every csv file, we launch a process to import it in mongo
    for filename in os.listdir(dir + "/csv"):
        if filename.endswith(".csv"):
            counter = counter + 1
            process = multiprocessing.Process(target=import_energy, args=(dir, filename))
            processes.append(process)
            process.start()

            if (counter > limit):
                counter = 0
                # wait for the already launched processes before launching some more
                for proc in processes:
                    proc.join()

                processes = []

    # wait for all remaining processes to finish
    for proc in processes:
        proc.join()

    processes = []

def import_energy(dir, filename):
    os.system("./import_energies.sh " + dir + "/csv/ " + filename)

def execute_shell_scripts(path):
    start_time = time.time()
    print "importing industries to mongodb..."
    import_industries(path)
    print "industries imported."
    print "importing energies to mongodb..."
    import_energies(path)
    end_time = time.time()
    print "all energies imported..."
    print "took " + str(end_time - start_time)[:5] +"s to import all industries and energies"


def run_migration(limit_count, offset):
    # we need to define a new client for each process to avoid some problems in mongo
    client = MongoClient('localhost', 27017)
    db = client.big_data_project

    industries = db.industries.find().limit(limit_count).skip(offset)

    for industry_document in industries:
        start_timer_one_indutry = time.time()
        industry_document_id = industry_document['SITE_ID']
        energy_collection_name = str(industry_document_id) + ".csv"
        energy_collection = db[energy_collection_name]

        add_energies_to_industry(industry_document_id, energy_collection, db)
        end_timer_one_indutry = time.time()
        elapsed_time_one_industry = end_timer_one_indutry - start_timer_one_indutry

        print ("energies added to industry with site_id : " + str(industry_document_id) +
        " in " + str(elapsed_time_one_industry)[:5] + "s")
        # remove collection, we don't need it anymore
        energy_collection.drop()

def run(path):
    start_total_time = time.time()
    # import all data via our bash scripts
    execute_shell_scripts(path)

    # get the number of industries
    client = MongoClient('localhost', 27017)
    db = client.big_data_project
    nb_industries = db.industries.find().count()

    all_processes = []
    offset = 0

    # change this value to specify the number of industries a single process have to take care of
    step = 20

    timer_start = time.time()

    # while all industries hasn't been processed
    while offset < nb_industries:
        process = multiprocessing.Process(target=run_migration, args=(step, offset))
        all_processes.append(process)
        process.start()
        offset += step

    # wait for all processes to finish
    for proc in all_processes:
        proc.join()

    all_processes = []
    timer_end = time.time()
    time_elapsed = timer_end - timer_start

    print "all data added in " + str(time_elapsed)[:5] + "s"
    print "all program took " + str(timer_end - start_total_time)[:5] + "seconds to execute"


client = MongoClient('localhost', 27017)

if 'big_data_project' not in client.database_names():
    run(DIR_PATH)
else:
    print '==> database already exists, delete big_data_project database first'
