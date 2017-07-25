import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint


# http://geoffboeing.com/2014/08/clustering-to-reduce-spatial-data-set-size/

#import fetch_data


auto_bin = pd.read_csv('data/auto_bin.csv')

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


#rs.to_csv('data/representative_bin.csv', index=False)



