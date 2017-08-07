import requests
import pandas as pd
import numpy as np
import datetime
import connection
from sklearn.cluster import DBSCAN
from shapely.geometry import MultiPoint
from haversine import haversine


from pymongo import MongoClient

MAXIMUM_RECOGNIZED_DISTANCE = 0.015



def _extract_bin_info(bins_dict):
    ids = []
    lat = []
    lon = []
    name = []
    bin_type = []
    for bin in bins_dict:
        ids += [bin['id']]
        lat += [float(bin['coords'][1])]
        lon += [float(bin['coords'][0])]
        name += [bin['name']]
        bin_type += [bin['type']]
    
    bins = pd.DataFrame({'id': ids, 'lat': lat, 'lon': lon, 'name': name, 
                         'type': bin_type})
    return bins

def fetch_auto_bin_from_mongo():
    auto_detected_bin_adapter = connection.connect_mongo_auto_detected_bin()
    auto_bin_dict = []
    for autobin in auto_detected_bin_adapter.find():
        auto_bin_dict += [autobin]
    return _extract_bin_info(auto_bin_dict), auto_bin_dict

def fetch_rep_bin_from_mongo():
    rep_bin_adapter = connection.connect_mongo_representative_auto_detected_bin()
    rep_bin_dict = []
    for autobin in rep_bin_adapter.find():
        rep_bin_dict += [autobin]
    return rep_bin_dict




### Immigrate representative autobin
auto_bin, auto_bin_raw = fetch_auto_bin_from_mongo()
#rep_bin_mg, rep_bin_mg_raw = fetch_rep_bin_from_mongo()
rep_bin_mg_raw = fetch_rep_bin_from_mongo()
rep_bin_mg = pd.DataFrame(rep_bin_mg_raw)


def find_representative_bins(auto_bin):
    coords = auto_bin.as_matrix(columns=['lat', 'lon'])
    
    kms_per_radian = 6371.0088
    epsilon = 0.015 / kms_per_radian
    db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
#    clusters_bin_ids = pd.Series([auto_bin['id'][cluster_labels == n] for n in range(num_clusters)])
    
    
    print('Number of clusters: {}'.format(num_clusters))
    
    def get_centermost_point(cluster):
        centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
        return centroid
    
    centroid_points = clusters.map(get_centermost_point)
    
    
    lats, lons = zip(*centroid_points)
    rep_points = pd.DataFrame({'lon':lons, 'lat':lats})
    rep_points['clustered_from'] = clusters
    rep_points['type'] = 'representative'
    
    return rep_points

found_rep_points = find_representative_bins(auto_bin)

rep_bin_mg['lonlat'] = tuple(zip(rep_bin_mg.lon, rep_bin_mg.lat))
ll = [(found_rep_points.iloc[0]['lon'], found_rep_points.iloc[0]['lat'])] * rep_bin_mg.shape[0]
ll2 = pd.DataFrame({'rep': rep_bin_mg['lonlat'], 'found':ll})
distances = ll2.apply(lambda point: haversine(point['found'], point['rep']), axis=1)
recognized_bin = distances[distances < MAXIMUM_RECOGNIZED_DISTANCE]

for i, found_point in found_rep_points.iterrows():
    print([(found_point['lon'], found_point['lat'])] * rep_bin_mg.shape[0])
    
    print()
    



rep_bin_mg.shape[0]




### save to postgres
#auto_bin_raw_df = pd.DataFrame(auto_bin_raw)
#
#
#rep_points_pg = pd.DataFrame({"coords": list(rep_points[["lon", "lat"]].values),
#                              "name": "auto_detected_bin", 
#                              'name_th': u'ถังถูกตรวจพบอัตโนมัติ',
#                              "type": "auto"})
#
#sipp_token = utils.get_sipp_token()
#def add_new_bin(sipp_token, bin_data):
#    """ add a bin to Sipp's Django trash API
#    """
#    headers = {
#        'Authorization': 'Bearer '+ sipp_token,
#        'Content-Type':'application/json'            
#    }       
#    r = requests.post('https://api.traffy.xyz/v0/trash/', data=bin_data, headers=headers)
#    print(r.content)
#    return r.json()['id']
#
#bin_id = []
#for i in rep_points_pg.index:
#    bin_data = rep_points_pg.loc[i].to_json(orient="columns") #, force_ascii=False)
#    print(bin_data)
#    
#    bin_id += [add_new_bin(sipp_token, bin_data)]
#
#temp_bin_id = bin_id
#rep_points['bin_id'] = bin_id
#rep_points_mg = rep_points[['bin_id', 'lat', 'lon', 'clustered_from', 'type']]
#
#
##rep_points_mg_json = rep_points_mg.to_dict(orient="records")
#
#representative_auto_detected_bin = connection.connect_mongo_representative_auto_detected_bin()
#representative_auto_detected_bin.insert_many(rep_points_mg_json)
#
#for i, rep_point in rep_points_mg.iterrows():
#    dict_mg = {'bin_id': rep_point['bin_id'],
#               'lat': rep_point['lat'],
#               'lon': rep_point['lon'],
#               'clustered_from_bin_id': clusters_bin_ids[i].tolist(),
#               'clustered_from_lat_lon': rep_point['clustered_from'].tolist(),
#               'type': rep_point['type'],
#               'posted_date': datetime.datetime.now(),}
##    rep_point = rep_points_mg.loc[i].to_json(orient="columns")
#    representative_auto_detected_bin.insert_one(dict_mg)
#
#    print(dict_mg)
#
#import datetime
#
#repbins = []
#for repbin in representative_auto_detected_bin.find():
#    repbins += [repbin]
#
#repbins_df = pd.DataFrame(repbins)
#
#for i, rep_point in repbins_df.iterrows():
#    representative_auto_detected_bin.update_one(
#            {'bin_id': rep_point['bin_id']},
#            {'posted_date': datetime.datetime.now()})
#
##representative_auto_detected_bin.delete_many({})
#    
#    
############################ End
    
    