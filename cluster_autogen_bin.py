import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint

from mpl_toolkits.basemap import Basemap

# http://geoffboeing.com/2014/08/clustering-to-reduce-spatial-data-set-size/

#import fetch_data


auto_bin = pd.read_csv('data/auto_bin.csv')

coords = auto_bin.as_matrix(columns=['lat', 'lon'])

kms_per_radian = 6371.0088
epsilon = 0.07 / kms_per_radian
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


fig, ax = plt.subplots(figsize=[10, 6])
rs_scatter = ax.scatter(rs['lon'], rs['lat'], c='#99cc99', edgecolor='None', alpha=0.7, s=120)
df_scatter = ax.scatter(auto_bin['lon'], auto_bin['lat'], c='k', alpha=0.9, s=3)
ax.set_title('Full data set vs DBSCAN reduced set')
ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')
ax.legend([df_scatter, rs_scatter], ['Full set', 'Reduced set'], loc='upper right')
plt.show()

#rs.to_csv('data/representative_bin.csv', index=False)



#map = Basemap(projection='mill', llcrnrlat=7.85, urcrnrlat=7.92, llcrnrlon=98.25, 
#              urcrnrlon=98.35, resolution='i')
#
#map.drawcoastlines()
#map.drawcountries(linewidth=0.25)
#map.fillcontinents()
#
#map.drawmapboundary()
#
#
#for index, bin in auto_bin[y_pred==1].iterrows():
#    x,y = map(bin['lon'], bin['lat'])
#    map.plot(x,y, 'ro')
#
#for index, bin in auto_bin[y_pred==2].iterrows():
#    x,y = map(bin['lon'], bin['lat'])
#    map.plot(x,y, 'go')
#
#for index, bin in auto_bin[y_pred==3].iterrows():
#    x,y = map(bin['lon'], bin['lat'])
#    map.plot(x,y, 'bo')
#
#for index, bin in auto_bin[y_pred==4].iterrows():
#    x,y = map(bin['lon'], bin['lat'])
#    map.plot(x,y, 'yo')
#
#for index, bin in auto_bin[y_pred==5].iterrows():
#    x,y = map(bin['lon'], bin['lat'])
#    map.plot(x,y, 'po')
#
#for index, bin in auto_bin[y_pred==6].iterrows():
#    x,y = map(bin['lon'], bin['lat'])
#    map.plot(x,y, 'ro')
#
#for index, bin in auto_bin[y_pred==7].iterrows():
#    x,y = map(bin['lon'], bin['lat'])
#    map.plot(x,y, 'go')
#
#for index, bin in auto_bin[y_pred==8].iterrows():
#    x,y = map(bin['lon'], bin['lat'])
#    map.plot(x,y, 'bo')
#plt.title('Phuket')
#plt.show()




#map = Basemap(projection='mill', llcrnrlat=7.85, urcrnrlat=7.92, llcrnrlon=98.25, 
#              urcrnrlon=98.35, resolution='i')
#
#map.drawcoastlines()
#map.drawcountries(linewidth=0.25)
#map.fillcontinents()
#
#map.drawmapboundary()
#
#
#for index, bin in auto_bin.iterrows():
#    x,y = map(bin['lon'], bin['lat'])
#    map.plot(x,y, 'ro')
#
#plt.title('Phuket')
#plt.show()
#

