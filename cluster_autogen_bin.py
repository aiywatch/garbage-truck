import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint
import requests

# http://geoffboeing.com/2014/08/clustering-to-reduce-spatial-data-set-size/

#import fetch_data

def fetch_auto_bin_from_api():
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
    
    url = 'https://api.traffy.xyz/v0/trash/?type=auto&limit=5000'
    response = requests.get(url).json()
    auto_bin_dict = response['results']
    
    return _extract_bin_info(auto_bin_dict)


def cluster_auto_detected_bins(auto_bin):
    coords = auto_bin.as_matrix(columns=['lat', 'lon'])
    
    kms_per_radian = 6371.0088
    epsilon = 0.015 / kms_per_radian
    db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
    print('Number of clusters: {}'.format(num_clusters))
    
    def get_centermost_point(cluster):
        centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
        centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
        return tuple(centermost_point)
    centermost_points = clusters.map(get_centermost_point)
    
    
    lats, lons = zip(*centermost_points)
    rep_points = pd.DataFrame({'lon':lons, 'lat':lats})
    
    rs = rep_points.apply(lambda row: auto_bin[(auto_bin['lat']==row['lat']) & (auto_bin['lon']==row['lon'])].iloc[0], axis=1)
    
    return rs




#auto_bin = pd.read_csv('data/auto_bin.csv')
auto_bin = fetch_auto_bin_from_api()

rs = cluster_auto_detected_bins(auto_bin)
#rs.to_csv('data/representative_bin.csv', index=False)









