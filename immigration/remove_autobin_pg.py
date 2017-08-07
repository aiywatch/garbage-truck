import utils
import pymongo
from pymongo import MongoClient
import requests
import pandas as pd


import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import connection

sipp_token = utils.get_sipp_token()
def remove_bin(sipp_token, bid):
    """ remove a bin from Sipp's Django trash API
    """
    headers = {
        'Authorization': 'Bearer '+ sipp_token,
        'Content-Type':'application/json'            
    }       
#    print('https://api.traffy.xyz/v0/trash/'+bid)
    r = requests.delete('https://api.traffy.xyz/v0/trash/'+bid+'/', headers=headers)
    print(r.content)
#    return not ("detail" in r.json() and r.json()["detail"]=='Not found.')


remove_bin(sipp_token, '1946')





#autobin_adapter = connection.connect_mongo_auto_detected_bin()
#
#autobins = []
#for autobin in autobin_adapter.find():
#    autobins += [autobin]
#
#autobins = pd.DataFrame(autobins)
#
##responses = []
#for bin_id in autobins['id']:
#    print(bin_id)
#    remove_bin(sipp_token, str(bin_id))
#    print(res)
#    responses += [res]















