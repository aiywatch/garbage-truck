import pandas as pd
import numpy as np
import connection
import arrow
import datetime

MIN_BINS_IN_A_SEQUENCE = 3


truck_state_adapter = connection.connect_mongo_truck_state()

start_date = arrow.get(datetime.datetime(2017,8,3), 'Asia/Bangkok').datetime
end_date = arrow.utcnow().datetime
vid = '359486060261821'



truck_states = []
for state in truck_state_adapter.find({'vid': vid, 'state_begin': {'$gte': start_date, '$lte': end_date}} ):
    truck_states += [state]

truck_states = pd.DataFrame(truck_states)

trip_id = 0
trip_ids = []
for i, point in truck_states.iterrows():
    trip_ids += [trip_id]
    if(point['state'] == 'at_term'):
        trip_id += 1

truck_states['trip_id'] = trip_ids


collecting_truck = truck_states[truck_states['state'] == 'collecting']

bin_sequences = []
last_trip = collecting_truck.iloc[0]['trip_id']
sequence = []
last_bins = []
for i, point in collecting_truck.iterrows():
    if(point['trip_id'] != last_trip):
        last_trip = point['trip_id']
        if len(sequence) > MIN_BINS_IN_A_SEQUENCE:
            bin_sequences += [sequence]
        sequence = []
    if(point['collecting_bin_ids'] != last_bins):
        sequence += [point['collecting_bin_ids']]
        last_bins = point['collecting_bin_ids']

bin_sequences += [sequence]
bin_sequences_df = pd.DataFrame(bin_sequences).T












