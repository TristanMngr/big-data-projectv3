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


# ============== KEEP IT WORKING ================
# time 5 min : 60 * 5
# time 1 week : 60 * 24 *7
def sum_load_curve_energy(db, energy_site_id, choosen_time):
    total_ld_energies = 0
    previous_timestamp = 0
    for index, energy in enumerate(db.industries.aggregate([
        {'$match': {'SITE_ID': energy_site_id}},
        {'$project': {'_id': 0, 'ENERGIES': 1}},
        {'$unwind': '$ENERGIES'},
        {'$sort' : {'ENERGIES.timestamp': 1}}])):
        if (energy['ENERGIES']['timestamp'] - previous_timestamp) >= choosen_time or index == 0:
            total_ld_energies += energy['ENERGIES']['value']
            previous_timestamp = energy['ENERGIES']['timestamp']

    return total_ld_energies


def sum_load_curve_industry(db, choosen_time):
    total_ld_industries = 0
    start = time.time()
    for industry in db.industries.find():
        print str(industry['SITE_ID']) + " complete"
        print sum_load_curve_energy(db, industry['SITE_ID'], choosen_time)
        total_ld_industries += sum_load_curve_energy(db, industry['SITE_ID'], choosen_time)

    end = time.time()
    print "Elapse time " + str((end - start))
    return total_ld_industries


def sum_load_curve_indutryV2(db):
    total = 0
    start = time.time()
    for industry in db.industries.aggregate( [
       { '$project': { '_id': 0,  'total_energy': {'$sum': '$ENERGIES.value'} }}]):
       total += industry['total_energy']

    end = time.time()
    print "Elapse time " + str((end - start))
    return total

# mprint(list(db.industries.aggregate( [
#     {'$unwind': '$ENERGIES'},
#     {'$match': { 'ENERGIES.timestamp': { '$mod': [300, 0]} }},
#     {'$project': {'_id': 0, 'total_energy': {'$sum': '$ENERGIES.value'}}},
#     {'$group': {'_id': '', 'total': { '$sum': '$total_energy'}}}
#   ])))


def sum_load_curve_indutryV2(choosen_time):
    start = time.time()
    industry = list(db.industries.aggregate( [
        {'$unwind': '$ENERGIES'},
        {'$match': { 'ENERGIES.timestamp': { '$mod': [choosen_time, 0]} }},
        {'$project': {'_id': 0, 'total_energy': {'$sum': '$ENERGIES.value'}}},
        {'$group': {'_id': '', 'total': { '$sum': '$total_energy'}}}
      ]))
    end = time.time()
    print "Elapse time " + str((end - start))
    return industry[0]['total'] if len(industry) >= 1  else "none"




# mprint(list(db.industries.aggregate( [
# {'$group': { '_id': '', 'total_energy': {'$sum': '$ENERGIES.value'} }}
#  {'$project': {'_id': 0, 'total_energy': {'$sum': '$ENERGIES.value'}}},
#  {'$sort': {'ENERGIES.timestamp': 1}},
#  {'$match': { 'ENERGIES.timestamp': { '$mod': [300, 0]} }},
#  {'$group': {'_id': '', 'total': { '$sum': '$total_energy'}}}
# ])))

# mprint(list(db.industries.aggregate( [
#     {'$unwind': '$ENERGIES'},
#     {'$sort': {'ENERGIES.timestamp': 1}}
#   ])))






# mprint(list(db.industries.aggregate( [
#    { '$project': { '_id': 0,  'total_energy': {'$sum': '$ENERGIES.value'} }}
#   ])))




# version 2
# 5 min timestamp
print 'version 2'
print sum_load_curve_indutryV2(300)

# version 2
# one week
print sum_load_curve_indutryV2(60*60*24*7)

#
# # version 1
# # 5 min timestamp
# print 'version 1 5 min timestamp'
# print sum_load_curve_industry(db, 300)
# # one week timestamp
# print 'version 1 one week timestamp'
# print sum_load_curve_industry(db, 60 * 60 * 24 * 7)


# ===================
