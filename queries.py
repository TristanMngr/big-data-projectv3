import pprint
import pymongo
import time

from pymongo import MongoClient

def mprint(list):
    pprint.pprint(list)

client = MongoClient('localhost', 27017)
db = client.big_data_project

FIVE_MINUTE_TIMESTAMP = 60 * 5
ONE_WEEK_TIMESTAMP = 60 * 60 * 24 *7


def sum_load_curve_indutry(choosen_time):
    start = time.time()
    industry = list(db.industries.aggregate( [
        {'$unwind': '$ENERGIES'},
        {'$match': { 'ENERGIES.timestamp': { '$mod': [choosen_time, 0]} }},
        {'$project': {'_id': 0, 'total_energy': {'$sum': '$ENERGIES.value'}}},
        {'$group': {'_id': '', 'total': { '$sum': '$total_energy'}}}
      ]))
    end = time.time()
    print "== >Elapse time " + str((end - start))
    return industry[0]['total'] if len(industry) >= 1  else "none"


def avg_load_curve_indutry(choosen_time):
    start = time.time()
    for industry in list(db.industries.aggregate( [
        {'$unwind': '$ENERGIES'},
        {'$match': { 'ENERGIES.timestamp': { '$mod': [choosen_time, 0]} }},
        {'$group': {'_id': '$SUB_INDUSTRY', 'average': { '$avg': '$ENERGIES.value'}}}])):
        print str(industry['_id']) + ' energy used : ' + str(industry['average']) 
    end = time.time()
    print "==> Elapse time " + str((end - start))




print 'Sum five minute timestamp'
print sum_load_curve_indutry(FIVE_MINUTE_TIMESTAMP)
print 'Sum one week timestamp'
print sum_load_curve_indutry(ONE_WEEK_TIMESTAMP)

print 'Average sub industry five minute timestamp '
print avg_load_curve_indutry(FIVE_MINUTE_TIMESTAMP)
print 'Average sub industry one week timestamp '
print avg_load_curve_indutry(ONE_WEEK_TIMESTAMP)
