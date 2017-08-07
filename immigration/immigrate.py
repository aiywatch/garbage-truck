import requests
import pandas as pd
import numpy as np
import datetime
import connection

from pymongo import MongoClient


#auto_detected_bin_adapter = connection.connect_mongo_auto_detected_bin()
#autobins = []
#for autobin in auto_detected_bin_adapter.find():
#    autobins += [autobin]
#
#autobins_df = pd.DataFrame(autobins)










## immigration auto_bin
#def fetch_bin_data(bin_type):
#    BIN_TYPE_LIST = ['all', 'metal', 'plastic', 'auto']
#    if bin_type == 'all':
#        url_insert_type = ''
#    elif bin_type in BIN_TYPE_LIST:
#        url_insert_type = 'type={}&'.format(bin_type)
#    else:
#        return 'Incorrect bin_type!'
#    url = 'https://api.traffy.xyz/v0/trash/?{}limit=5000'.format(url_insert_type)
#    response = requests.get(url).json()
#    return response['results']
#
#bin_data = fetch_bin_data('auto')
#
#auto_detected_bin_adapter = connection.connect_mongo_auto_detected_bin()

#auto_detected_bin_adapter.insert_many(bin_data)

####################################end auto bin





#### Immigrate representative autobin
#auto_bin, auto_bin_raw = fetch_auto_bin_from_mongo()
#
#coords = auto_bin.as_matrix(columns=['lat', 'lon'])
#
#kms_per_radian = 6371.0088
#epsilon = 0.015 / kms_per_radian
#db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
#cluster_labels = db.labels_
#num_clusters = len(set(cluster_labels))
#clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
#clusters_bin_ids = pd.Series([auto_bin['id'][cluster_labels == n] for n in range(num_clusters)])
##type(clusters_bin_ids[318])
#
#
#print('Number of clusters: {}'.format(num_clusters))
#
#def get_centermost_point(cluster):
#    centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
##    centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
##    return tuple(centermost_point)
#    return centroid
##centermost_points = clusters.map(get_centermost_point)
#centroid_points = clusters.map(get_centermost_point)
#
#
#lats, lons = zip(*centroid_points)
#rep_points = pd.DataFrame({'lon':lons, 'lat':lats})
#rep_points['clustered_from'] = clusters
#rep_points['type'] = 'representative'
#
#
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
    
    