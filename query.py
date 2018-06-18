import pprint
import pymongo
import time

from pymongo import MongoClient

def mprint(list):
    pprint.pprint(list)

def sup(value1, value2):
    if value1 < value2:
        return True
    return False


client = MongoClient('localhost', 27017)
db = client.big_data_project

# order by ascending industry id [TO KEEP]
# mprint(list(db.industries.find().sort([('SITE_ID', 1)])))

# count total of energy [TO KEEP]
# mprint(list(db.industries.aggregate([
#     {'$unwind': '$ENERGIES'},
#     {'$count': 'total_energy'}
# ])))


# ============== KEEP IT WORKING ================
start_time = time.time()
total_energy = 0
previous_timestamp = 0
for index, energy in enumerate(db.industries.aggregate([
    {'$match': {'SITE_ID': 6}},
    {'$project': {'_id': 0, 'ENERGIES': 1}},
    {'$unwind': '$ENERGIES'},
    {'$sort' : {'ENERGIES.timestamp': 1}}])):


    if (energy['ENERGIES']['timestamp'] - previous_timestamp) >= 60 * 5 or index == 0:
        total_energy += energy['ENERGIES']['value']
        previous_timestamp = energy['ENERGIES']['timestamp']

print total_energy
end_time = time.time()
print end_time - start_time
#
# array = []
#
# array.append(4)
# array.append(6)
#
# print array[0]
# print sum(array)


# ===============
# KEEP for exemple

# start_time = time.time()
# mprint(list(db.industries.aggregate([
#     {'$match': {'SITE_ID': 6}},
#     {'$unwind': '$ENERGIES'},
#     {'$project': {'_id': 0, 'ENERGIES.timestamp': 1, 'ENERGIES.value': 1}},
#     {'$sort' : {'ENERGIES.timestamp': 1}},
#     {'$count': 'count'}
#     ])))
# end_time = time.time()
# print end_time - start_time
#
# start_time = time.time()
# mprint(list(db.industries.aggregate([
#     {'$match': {'SITE_ID': 6}},
#     {'$project': {'_id': 0, 'ENERGIES': 1}},
#     {'$unwind': '$ENERGIES'},
#     {'$sort' : {'ENERGIES.timestamp': 1}},
#     ])))
# end_time = time.time()
# print end_time - start_time


#query the first and last data energy from site 6
# mprint(list(db.industries.aggregate([
#     {'$match': {'SITE_ID': 6}},
#     {'$project': {'_id': 0, 'ENERGIES': 1}},
#     {'$unwind': '$ENERGIES'},
#     {'$sort' : {'ENERGIES.timestamp': 1}},
#     {'$group': {'_id' : 0, 'first':{'$first': "$$ROOT"}, 'last': {'$last': "$$ROOT"} }}
#     ])))
