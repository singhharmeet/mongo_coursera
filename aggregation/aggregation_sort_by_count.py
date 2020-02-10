"""
Python file for aggregation example on sort and count.

"""
from pprint import pprint
from db_settings import mongo_db


# Method One
"""
A group stage groups its input documents by a specified identifier expression and applies any accumulator expressions 
supplied to each group. 
This identifier expression stipulates that we want each group produced to be identified by a dictionary containing a 
single field. FOr example, here the key given below should be the string, language, and the value should be a distinct 
$language value.
Group stages allow us to apply any number of accumulators that operate on the documents passed through the stage.
$sum here is used to provide incremental counter for each occurrence of distinct $language value.

$sort is another stage in pipeline here which is just sorting the results of first stage in descending order of count
"""
pipeline_1 = [
    {
        '$group': {
            # can also write as "_id": "$language"
            '_id': {"language": "$language"},
            'count': {'$sum': 1}
        }
    },
    {
        '$sort': {'count': -1}
    }
]


# Method Two
"""
The pipeline here is doing the exact task as above using sortByCount
"""
pipeline_2 = [
    {
        '$sortByCount': "$language"
    }
]


pop_1 = list(mongo_db.movies_initial.aggregate(pipeline_1))[:5]
pop_2 = list(mongo_db.movies_initial.aggregate(pipeline_2))[:5]
print(pop_1)
print(pop_2)
assert pop_1 == pop_2
