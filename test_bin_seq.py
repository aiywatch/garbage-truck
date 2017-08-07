import requests
import pandas as pd


url = 'http://api2.traffy.in.th/api/smartphuket/garbage/get_bin_route/359486060261458/2017/7/24/'

response = requests.get(url).json()

bin_route = response[0]

route = bin_route['bin_route']
route_df = pd.DataFrame.from_dict(route).sort_index()


route_df.to_csv('test_route.csv')

#bin_sequence = bin_route['bin_sequence']

bin_sequences =  []
for bin in response:
    bin_sequences += [bin['bin_sequence']['bin_id']]
    
bin_sequences_df = pd.DataFrame(bin_sequences)


from pymongo import MongoClient
host='db0.traffy.xyz'
port=27017
user='wichai'#'attapon'
password='traffy-w-nectec-9'#'recent-zippered-pasty'

client = MongoClient(host,port)

client.garbage.authenticate(user, password)

#client.garbage.collection_names()

truck_state = client.garbage.truck_state

