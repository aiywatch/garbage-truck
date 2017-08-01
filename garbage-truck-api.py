import connection
import pandas as pd

bin_route_connection = connection.connect_mongo_garbage_bin_route()




for bin in bin_route_connection.find():
    print(bin)
    print('1111111111111111111111111111111111111111111')