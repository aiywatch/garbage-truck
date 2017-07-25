#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Smart garbage management - Truck_tracking
# description:
#   Tracking garbage trucks to determine their state and reset the collected bin
# info:
#   apscheduler: http://apscheduler.readthedocs.io/en/3.0/userguide.html#removing-jobs
#   subprocess: https://docs.python.org/2/library/subprocess.html

from utils import cal_distance, is_in_radius, get_sipp_token
from dynamic_routing import Dynamic_route_GA # for updating truck tasks
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import numpy as np
import pandas as pd
import requests
from subprocess import check_output #call
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps as mongo_dumps
from pymemcache.client.base import Client
import simplejson as json
import pyrebase
import arrow
import copy
import os
import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

# FireBase Config
config = {
  "apiKey": "AIzaSyCSVD4BKZOBq840_Newby-eXVr8WfPl5-Y",
  "authDomain": "traffy-cloud.firebaseapp.com",
  "databaseURL": "https://traffy-cloud.firebaseio.com",
  "storageBucket": "traffy-cloud.appspot.com"
}
email = "saardschool1@gmail.com"
password = 'sujaray2traffy'

MONGO_HOST='central-db'
MONGO_PORT=27017
MONGO_USER='wichai'
MONGO_PASS='traffy-w-nectec-9'
DB_GARBAGE='garbage'
CLT_BIN_COLLECT='bin_collect'
CLT_TRUCK_JOB='truck_job'
CLT_TRUCK_STATE='truck_state'
CLT_TRUCK_MAINTAIN='truck_maintain'
CLT_NO_BIN='no_bin_location'
CLT_STOP_WHILE_COLLECT='truck_stop_while_collect'
CLT_NUM_TRUCK_STATE='num_truck_in_state' #collect number of truck per hour of each state

# correction tester vars
CLT_TRUCK_STATE_ACTUAL='truck_state_actual'

MEMCACHE_HOST='central-db'
MEMCACHE_PORT=11211
MEMCACHE_STATE='pong:truckstate'
# MEMCACHE_TRUCK_STATE_PREFIX='pong:truckstate-'
# MEMCACHE_STATE_TIMESTAMP_PREFIX='pong:truckstate_ts-'
MEMCACHE_STATE_RETAIN_DURATION=15*60 #if this run is diff from the last time longer than this value the init state will be STOP instead

# BIN_GG_SHEET='https://docs.google.com/spreadsheets/d/1Bj4967Tc6I3Ielt6uRDheCCe-RIuzq9YLflEofDBSzc/pub?output=csv'
# BIN_INFO_LINK='http://api2.traffy.in.th/v0/trash/?limit=500'
BIN_INFO_FILE='/opt/plagad/route_construction/to_realtime_db_on_firebase/prev/kaya_bin.json'#'/opt/plagad/route_construction/to_realtime_db_on_firebase/kaya_bin.json'

# Truck behavior vars
TRUCK_CHECK_INTERVAL=2 # sec
TRUCK_INFO_LINK='http://api.traffy.xyz/vehicle/?line=phuket-garbage-truck&limit=50' #JSON format
COLLECT_TIME_THRESHOLD=15   # sec
COLLECT_SPEED_THRESHOLD=0.5   #km/h
COLLECT_RADIUS=15   # metre
IDLING_DURATION=6   # how long stopping called idling
GPS_LOST_CONNECT_DURATION=1   # how long last received msg called lost connection (days), and not in a terminal
TRUCK_MAX_SPEED=90  # used to determine the invalid gps location (location will move faster than this value)
TRUCK_MAINTAIN_CHECK_INTERVAL=120# sec

# Automatic new container detector
NEW_BIN_COLLECT_TIME_THRESHOLD=120   # sec
NEW_BIN_CHECKING_RADIUS=10  # metre
NEW_BIN_CHECKING_OCCURRENCE=5  # num of occurrences to ensure the new point
NEW_BIN_DISCARD_DURATION=2 #days, threshold to clear suspicious points, if no new occurrence 
BIN_UNUSED_DURATION=4 #days, if there is no collecting on this bin in this period, it will be mark as unused


# Locations and radiuses
DUMPSITE_RADIUS=500 # metre
DUMPSITE_LAT=7.863259
DUMPSITE_LON=98.395629
TERM_RADIUS=300 # metre
TERM1_LAT=7.887007
TERM1_LON=98.2964797
TERM2_LAT=7.895780
TERM2_LON=98.336451
CENTER_OF_PATONG=(7.898841, 98.290734)
PATONG_RADIUS=4000 # metre

class Truck_state():
    RUNNING, STOP = 'running', 'stop'               #available
    COLLECTING, RUN2COLLECT, STOP_WHILE_RUN2COLLECT = 'collecting','run2collect','stop_while_run2collect'   #working
    AT_DUMP, AT_TERM = 'at_dump','at_term'          #parking
    INVALID_GPS, NO_GPS = 'invalid_gps','no_gps'    #error
    BEING_MAINTAINED = 'maintain'
    
    # Grouping
    states = (RUNNING, STOP,COLLECTING, RUN2COLLECT, STOP_WHILE_RUN2COLLECT,AT_DUMP, AT_TERM,INVALID_GPS, NO_GPS)
    AVAILABLE = [RUNNING, STOP]
    WORKING = [COLLECTING, RUN2COLLECT, STOP_WHILE_RUN2COLLECT]
    PARKING = [AT_DUMP, AT_TERM]
    ERROR = [INVALID_GPS, NO_GPS]
    
    groups = {'all':states,
              'available':AVAILABLE,
              'working':WORKING,
              'parking':PARKING,
              'error':ERROR,
              'maintain':[BEING_MAINTAINED],}
    

''' TODO list
    * move truck job from Firebase to MongoDB !!
    * Compensate the delay of each state in its duration
    * Fix bug of auto_detected_bin
    * cron for adding num truck in state for every hour
'''


class Truck_tracking():
    ''' Tracking garbage trucks to check garbage collecting status
    '''
    # bin_df = None
    bin_info = None
    truck = None
    truck_prev = None
    is_running = False
    sched = None
    truck_task = None
    state_cache = None  
    maintaining_trucks = {}
    first_run = True
    
    def __init__(self, log_dir='log'):
        ## prepare bin information--
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        logging.basicConfig(filename=log_dir+'/debug.log',level=logging.ERROR)
        # logging.basicConfig(level=logging.ERROR)  
        
        # MongoClient
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        mongo = client[DB_GARBAGE]
        if not mongo.authenticate(MONGO_USER, MONGO_PASS):
            print 'Cannot connect to MongoDB server!'
            raise        
        self.clt_bin = mongo[CLT_BIN_COLLECT]
        self.clt_job = mongo[CLT_TRUCK_JOB]
        self.clt_state = mongo[CLT_TRUCK_STATE]
        self.clt_state_actual = mongo[CLT_TRUCK_STATE_ACTUAL]
        self.clt_maintain = mongo[CLT_TRUCK_MAINTAIN]
        self.clt_no_bin = mongo[CLT_NO_BIN]
        self.clt_stop_while_collect = mongo[CLT_STOP_WHILE_COLLECT]
        self.clt_num_truck_in_state = mongo[CLT_NUM_TRUCK_STATE]
        
        # for caching current state
        self.state_cache = Client((MEMCACHE_HOST, MEMCACHE_PORT), serializer=self.json_serializer, deserializer=self.json_deserializer)
                
        self.truck_task = Dynamic_route_GA(75, rep_best_lim=15)
        
        # fire base data source        
        firebase = pyrebase.initialize_app(config)
        self.auth = firebase.auth()
        self.db = firebase.database()
        
        # get basic info
        self.check_maintaining_truck()
        self.sipp_token = get_sipp_token()
        self.refresh_bin_info()
        
        self.sched = BackgroundScheduler()
        self.sched.start()
                
        # self.start()

    def start(self):
        if not self.is_running:
            self.is_running = True
            self.sched.add_job(self.check_maintaining_truck, 'interval', seconds=TRUCK_MAINTAIN_CHECK_INTERVAL, id='check_maintaining_truck')
            self.sched.add_job(self.refresh_sipp_token, 'interval', hours=6, id='refresh_sipp_token')
            self.sched.add_job(self.refresh_bin_info, 'interval', minutes=3, id='refresh_bin_info')
            self.sched.add_job(self.tracking, 'interval', seconds=TRUCK_CHECK_INTERVAL, id='truck_track')
            self.sched.add_job(self.check_unused_bin, 'interval', hours=3, id='check_unused_bin')            
            self.sched.add_job(self.add_num_truck_in_state, 'cron', minute=0, second=10, id='add_num_truck_in_state')
                        
        else:
            print 'The tracking is already started!'


    def get_fresh(self):
        ''' Get new truck info from TRUCK_INFO_LINK API
        '''
        try:
            r = requests.get(TRUCK_INFO_LINK)
            truck_json = r.json()['results']
        except Exception as e:
            logging.error('requests.get(TRUCK_INFO_LINK) '+str(arrow.now()))
            logging.error(r.text)
            logging.error(e)

            # logging.error('Return from URL: %s \nis %s', TRUCK_INFO_LINK, r.text)
            # cannot decode JSON from P'Sipp's api, so skip this time 
            # raise
            return
            
        truck={}
        for res in truck_json:
            if 'speed' not in res['info']: 
                continue
            vid = res['vehicle_id']
            atruck = {}
            atruck['speed'] = res['info']['speed']
            atruck['lat'] = res['info']['coords'][1]
            atruck['lon'] = res['info']['coords'][0]
            atruck['gps_timestamp'] = arrow.get(res['info']['gps_timestamp'])
            # check wheater the vid is in the maintain truck list,
            # set its state to BEING_MAINTAINED.
            if vid in self.maintaining_trucks:
                atruck['state'] = Truck_state.BEING_MAINTAINED
                atruck['state_begin'] = self.maintaining_trucks[vid]
            elif self.truck==None or vid not in self.truck or 'state' not in self.truck[vid]:
                atruck['state'] = Truck_state.STOP
                atruck['state_begin'] = arrow.now()
            truck[vid] = atruck
        return truck

    def tracking(self):
        ''' update trucks information and state '''

        # get truck information
        if self.first_run:
            if self.state_cache.get(MEMCACHE_STATE) and \
                (arrow.now()-arrow.get(self.state_cache.get(MEMCACHE_STATE)['timestamp'])).seconds \
                <= MEMCACHE_STATE_RETAIN_DURATION:
                truck = self.state_cache.get(MEMCACHE_STATE)['truck_states']
                for vid in truck:
                    for key in truck[vid]:
                        if key in ('gps_timestamp','state_begin', 'timestamp'):
                            truck[vid][key] = arrow.get(truck[vid][key])
            else:
                truck = self.get_fresh()        
                if truck==None: return  
            
            self.truck, truck_prev = truck, truck
            # update the state for tester
            self.push_latest_states_to_firebase(3, init=True) 
            self.first_run = False            
        else:
            truck = self.get_fresh()        
            if truck==None: return
        
        # update trucks information
        if self.truck==None:
            self.truck, truck_prev = truck, truck   
        else:
            truck_prev = copy.deepcopy(self.truck) # keep prev info.
            # update by their id
            for vid in truck:
                if vid in self.truck:
                    # updating
                    self.truck[vid]['speed'] = truck[vid]['speed']
                    self.truck[vid]['lat'] = truck[vid]['lat']
                    self.truck[vid]['lon'] = truck[vid]['lon']
                    self.truck[vid]['gps_timestamp'] = truck[vid]['gps_timestamp']
                    if 'state' in truck[vid]:
                        self.truck[vid]['state'] = truck[vid]['state']
                        self.truck[vid]['state_begin'] = truck[vid]['state_begin']
                        
                else:
                    self.truck[vid] = truck[vid]    # add a new truck

        # update truck state
        for vid in self.truck:
        
            # NO_GPS checking state
            last_data_duration = (arrow.now()-self.truck[vid]['gps_timestamp']).days            
            if self.truck[vid]['state']==Truck_state.NO_GPS and last_data_duration<GPS_LOST_CONNECT_DURATION: 
                self.log_state(vid, Truck_state.STOP)                
            elif last_data_duration > GPS_LOST_CONNECT_DURATION \
                and self.truck[vid]['state'] != Truck_state.BEING_MAINTAINED \
                and not self.is_at_term(vid):
                self.log_state(vid, Truck_state.NO_GPS)
            elif self.is_at_term(vid) and self.truck[vid]['state'] not in (Truck_state.RUN2COLLECT, Truck_state.RUNNING):                
                self.log_state(vid, Truck_state.AT_TERM)
            
            # INVALID_GPS 
            if self.truck[vid]['state']==Truck_state.INVALID_GPS:
                # if it has normal position (no bouncing), the state will return to STOP or STOP_WHILE_RUN2COLLECT state
                timediff = (self.truck[vid]['gps_timestamp']-truck_prev[vid]['gps_timestamp']).seconds
                distance = cal_distance(self.truck[vid]['lat'], self.truck[vid]['lon'], truck_prev[vid]['lat'], truck_prev[vid]['lon'])
                space_speed = (distance/timediff)*3.6 if timediff>0 else 0
                if space_speed <= TRUCK_MAX_SPEED:
                    # get the state before to turn back
                    cs = self.clt_state.find({'vid':vid}, limit=2).sort('state_begin',-1) # get the last 2 states
                    if cs.count()>1:
                        if cs[1]['state'] in (Truck_state.COLLECTING, Truck_state.RUN2COLLECT, Truck_state.STOP_WHILE_RUN2COLLECT):
                            self.log_state(vid, Truck_state.STOP_WHILE_RUN2COLLECT)
                        else:
                            self.log_state(vid, Truck_state.STOP)
                    else:
                        self.log_state(vid, Truck_state.STOP)                    
               
            # BEING_MAINTAINED  
            elif self.truck[vid]['state']==Truck_state.BEING_MAINTAINED:
                if vid not in self.maintaining_trucks:
                    self.log_state(vid, Truck_state.STOP)   
               
            # AT_TERM  
            elif self.truck[vid]['state']==Truck_state.AT_TERM:
                if not self.is_at_term(vid):
                    self.log_state(vid, Truck_state.RUNNING)                
                    # assign new truck task
                    # js_out,job_df = self.truck_task.get_best_route(vid)    
                    # if job_df is not None:
                        # self.truck_task.add_task(vid, job_df)
                        # logging.info('Assign a task to %s', vid)
                    # else: 
                        # logging.warning('Cannot assign a task to %s !', vid)
            
            # AT_DUMP 
            elif self.truck[vid]['state']==Truck_state.AT_DUMP:
                if not self.is_at_dump(vid):
                    self.log_state(vid, Truck_state.RUNNING)

            # COLLECTING 
            elif self.truck[vid]['state']==Truck_state.COLLECTING:
                if self.is_collected(vid):
                    # Collected -> reset the bin(s)
                    # php -f /opt/plagad/route_construction/to_realtime_db_on_firebase/clear_bin_capacity.php bin_id [bin_id]
                            
                    cmd = ['php', '-f', '/opt/plagad/route_construction/to_realtime_db_on_firebase/clear_bin_capacity.php']
                    for bid in self.truck[vid]['collecting_bin_ids']:
                        cmd.append(str(bid))
                    try:
                        output = check_output(cmd)
                        if len(json.loads(output)['fail_bin_id']) == 0:
                            logging.info('Bin_id %s is collected, by %s', self.truck[vid]['collecting_bin_ids'], vid)
                        else:
                            logging.warning('Bin_id %s cannot be collected, by %s !!', json.loads(output)['fail_bin_id'], vid)
                    except Exception as e:
                        logging.error('check_output(cmd): cannot clear bins')
                        logging.error(e)
                        
                     
                    # update the truck task
                    # self.truck_task.update_task(vid,'collected_bin', self.truck[vid]['collecting_bin_ids'])
                        
                    # save garbage collecting log
                    full_waiting = {}
                    for bid in self.truck[vid]['collecting_bin_ids']:
                        if bid in self.bin_info \
                        and 'level' in self.bin_info[bid] \
                        and self.bin_info[bid]['level']>=100 \
                        and 'full_timestamp' in self.bin_info[bid]:        
                            full_waiting[bid] = (arrow.now()-self.bin_info[bid]['full_timestamp']).seconds  
                    self.clt_bin.insert_one({
                        'vid': vid,
                        'timestamp': arrow.utcnow().naive,
                        'collected_bin_ids': self.truck[vid]['collecting_bin_ids'],
                        'collecting_duration': (arrow.now()-self.truck[vid]['state_begin']).seconds,
                        'full_waiting': full_waiting,
                    })
                                        
                    # return running
                    self.log_state(vid, Truck_state.RUN2COLLECT)
                 
            # RUN2COLLECT   
            elif self.truck[vid]['state']==Truck_state.RUN2COLLECT:
                # if the location is bouncing, change state to INVALID_GPS
                timediff = (self.truck[vid]['gps_timestamp']-truck_prev[vid]['gps_timestamp']).seconds
                distance = cal_distance(self.truck[vid]['lat'], self.truck[vid]['lon'], truck_prev[vid]['lat'], truck_prev[vid]['lon'])
                space_speed = (distance/timediff)*3.6 if timediff>0 else 0
                if space_speed > TRUCK_MAX_SPEED:
                    self.log_state(vid, Truck_state.INVALID_GPS)
                else:                    
                    if self.is_at_term(vid):
                        self.log_state(vid, Truck_state.AT_TERM)
                        # TODO update the truck task
        
                    elif self.is_at_dump(vid):  
                        self.log_state(vid, Truck_state.AT_DUMP)              
                    elif self.truck[vid]['speed'] <= COLLECT_SPEED_THRESHOLD:
                        self.log_state(vid, Truck_state.STOP_WHILE_RUN2COLLECT)    
    
            # STOP_WHILE_RUN2COLLECT
            elif self.truck[vid]['state']==Truck_state.STOP_WHILE_RUN2COLLECT:
                # if still stop
                if self.truck[vid]['speed'] <= COLLECT_SPEED_THRESHOLD:
                # if the stop time is >= threshold and in bin radius, change state to collecting
                    stop_duration = (arrow.now()-self.truck[vid]['state_begin']).seconds
                    if stop_duration >= COLLECT_TIME_THRESHOLD: 
                        bin_ids = self.find_collecting_bin(self.truck[vid]['lat'], self.truck[vid]['lon'])                 
                        if len(bin_ids)>0:
                            self.log_state(vid, Truck_state.COLLECTING, {'collecting_bin_ids':bin_ids})
                        elif stop_duration >= NEW_BIN_COLLECT_TIME_THRESHOLD:                      
                            # analyze new container point 
                            new_bin_id = self.determine_new_container(self.truck[vid]['lat'], self.truck[vid]['lon'])
                            # if detected new bin, update the state
                            if new_bin_id != -1:
                                self.log_state(vid, Truck_state.COLLECTING, {'collecting_bin_ids':[new_bin_id]})
                            
                else:
                    self.log_state(vid, Truck_state.RUN2COLLECT)
    
            # STOP
            elif self.truck[vid]['state']==Truck_state.STOP:
                # if still stop
                if self.truck[vid]['speed'] <= COLLECT_SPEED_THRESHOLD:
                # if the stop time is >= threshold and in bin radius, change state to collecting
                    bin_ids = self.find_collecting_bin(self.truck[vid]['lat'], self.truck[vid]['lon'])
                    stop_duration = (arrow.now()-self.truck[vid]['state_begin']).seconds
                    if stop_duration >= COLLECT_TIME_THRESHOLD and len(bin_ids)>0:
                        self.log_state(vid, Truck_state.COLLECTING, {'collecting_bin_ids':bin_ids})
                else:
                    self.log_state(vid, Truck_state.RUNNING)

            # RUNNING
            elif self.truck[vid]['state']==Truck_state.RUNNING:
                # if the location is bouncing, change state to INVALID_GPS
                timediff = (self.truck[vid]['gps_timestamp']-truck_prev[vid]['gps_timestamp']).seconds
                distance = cal_distance(self.truck[vid]['lat'], self.truck[vid]['lon'], truck_prev[vid]['lat'], truck_prev[vid]['lon'])
                space_speed = (distance/timediff)*3.6 if timediff>0 else 0
                if space_speed > TRUCK_MAX_SPEED:
                    self.log_state(vid, Truck_state.INVALID_GPS)
                else:
                    if self.is_at_term(vid):
                        self.log_state(vid, Truck_state.AT_TERM)
                        # update the truck task
                        # self.truck_task.update_task(vid,'reach_terminal')
                        # logging.info('Truck %s reach terminal',vid)
        
                    elif self.is_at_dump(vid):  
                        self.log_state(vid, Truck_state.AT_DUMP)                   
                    elif self.truck[vid]['speed'] <= COLLECT_SPEED_THRESHOLD:
                        self.log_state(vid, Truck_state.STOP) 
        
        # caching trucks state data
        self.state_cache.set(MEMCACHE_STATE, {'truck_states':self.truck,'timestamp':arrow.now()})
        
        
    def stop(self):
        if self.is_running:
            self.sched.remove_job('truck_track')
            self.sched.remove_job('check_maintaining_truck')
            self.sched.remove_job('refresh_sipp_token')
            self.sched.remove_job('refresh_bin_info')
            self.sched.remove_job('check_unused_bin')
            self.sched.remove_job('add_num_truck_in_state')
            self.is_running = False
            self.truck = None
        else:
            print 'The tracking is already stopped!'
    
    def log_state(self, vid, to_state, detail=None):
        ''' Log the changing a truck state
            the state detial will be recorded in mongo, both the prev state (duration, and etc.) and current state
        '''
        # Check whether its state is changing?
        from_state = self.truck[vid]['state']
        if from_state != to_state:
            state_begin = arrow.now()
            self.truck[vid]['state']=to_state  
            self.truck[vid]['state_begin']=state_begin  
            
            # record to mongo by cases     
            
            # update duration to a prev state
            prev_state = self.clt_state.find_one({'vid':vid},sort=[('state_begin',-1)]) # get the last state instance
            if prev_state:
                state_duration = (arrow.now()-arrow.get(prev_state['state_begin'])).seconds
                self.clt_state.find_one_and_update({'vid':vid},{'$set':{'duration':state_duration}},sort=[('state_begin',-1)])
            
            # cs = self.clt_state.find({'vid':vid}, limit=1).sort('state_begin',-1) # get the last state instance
            # if cs.count()>0:
                # state_duration = (arrow.now()-arrow.get(cs[0]['state_begin'])).seconds
                # self.clt_state.update_one({'_id':cs[0]['_id']},{'$set':{'duration':state_duration}})
        
            # insert a new state
            state_data = {
                        'vid': vid,
                        'state': to_state,
                        'state_begin': state_begin.to('utc').naive,
                    }
            if to_state == Truck_state.COLLECTING:  
                self.truck[vid]['collecting_bin_ids'] = detail['collecting_bin_ids'] 
                state_data['collecting_bin_ids']=detail['collecting_bin_ids']                
            elif from_state == Truck_state.COLLECTING: 
                collected_bin_ids = []   # collected bin in this working round
                if prev_state and 'collected_bin_ids' in prev_state:
                    collected_bin_ids.extend(prev_state['collected_bin_ids'])                
                collected_bin_ids.extend(self.truck[vid].pop('collecting_bin_ids'))
                state_data['collected_bin_ids']=collected_bin_ids
            
	    self.clt_state.insert_one(state_data)
            
            # update the state for tester
            self.push_latest_states_to_firebase(3, vid)
        
    
    def check_maintaining_truck(self):
        maintaining_trucks = {}
        for mt in self.clt_maintain.find():
            maintaining_trucks[mt['vid']] = arrow.get(mt['timestamp']) # change type from datetime to arrow
        self.maintaining_trucks = maintaining_trucks        
        return maintaining_trucks
        
    def encode_arrow(self, obj):
        if isinstance(obj, arrow.arrow.Arrow):
            return str(obj)
        raise TypeError(repr(obj) + " is not JSON serializable")
        
    """ def decode_arrow(self, data):
        if ('gps_timestamp','state_begin', 'timestamp') in data:
            return arrow.get(dct['real'], dct['imag'])
        return dct"""
        
    def json_serializer(self, key, value):
        if type(value) == str:
            return value, 1
        return json.dumps(value, default=self.encode_arrow), 2

    def json_deserializer(self, key, value, flags):
        if flags == 1:
            return value
        elif flags == 2:
            return json.loads(value)
        raise Exception("Unknown serialization format")
    
    def find_collecting_bin(self, lat, lon):
        ''' finding bin ids which in the COLLECT_RADIUS
        '''
        # bin_ids = []
        # for bid, b in self.bin_df.iterrows():
            # if cal_distance(lat,lon, b.lat,b.lon) <= COLLECT_RADIUS:
                # bin_ids.append(bid)        
        # return bin_ids
        bin_ids = []
        for bid in self.bin_info:
            if cal_distance(lat,lon, self.bin_info[bid]['lat'],self.bin_info[bid]['lon']) <= COLLECT_RADIUS:
                bin_ids.append(bid)        
        return bin_ids
        
    def is_collected(self, vid):
        ''' Determine the collecting bins of this vid is finished or not
            All collecting bins has to farther than COLLECT_RADIUS
        '''
        dis2bins = []
        for bid in self.truck[vid]['collecting_bin_ids']:
            if bid in self.bin_info:
                abin = self.bin_info[bid]
                dis2bins.append( cal_distance(self.truck[vid]['lat'],self.truck[vid]['lon'], abin['lat'],abin['lon']) )
        return len(dis2bins)==0 or (min(dis2bins) > COLLECT_RADIUS)
          
    """def find_nearest_bin(self, lat, lon):
        ''' finding the nearest bin id from the given coords in circle radius
        '''
        bin_dis = {}
        for bid in self.bin_info:
            abin = self.bin_info[bid]
            bin_dis[bid] = cal_distance(lat,lon, abin['lat'],abin['lon'])

        # find minimum id
        mindis = min(bin_dis.values())
        return [key for key, value in bin_dis.items() if value == mindis][0], mindis"""

    def is_at_term(self, vid):
        ''' Determine whether the truck is at terminal by TERM_RADIUS
        '''
        dis2term1 = cal_distance(
            self.truck[vid]['lat'], self.truck[vid]['lon'],
            TERM1_LAT, TERM1_LON
        )
        dis2term2 = cal_distance(
            self.truck[vid]['lat'], self.truck[vid]['lon'],
            TERM2_LAT, TERM2_LON
        )
        dis2term = min(dis2term1,dis2term2)
        return dis2term <= TERM_RADIUS
        
    def is_at_dump(self, vid):
        ''' Determine whether the truck is at dump by DUMPSITE_RADIUS
        '''        
        dis2dump = cal_distance(
            self.truck[vid]['lat'], self.truck[vid]['lon'],
            DUMPSITE_LAT, DUMPSITE_LON
        )
        return dis2dump <= DUMPSITE_RADIUS
        
    def determine_new_container(self, lat, lon):
        ''' Determine new container lacation 
            by the number of occurences of STOP_WHILE_RUN2COLLECT at the same position for COLLECT_TIME_THRESHOLD.
            The bin must be in PATONG_RADIUS from CENTER_OF_PATONG.
            The invalid position (eg. intersection) will be filtered off manually (list in mongo)
            'stop while collecting' collection
                1. lat
                2. lon
                3. timestamp (list)
                4. radius
                5. num_occurrence
            'no bin location' collection
                1. lat
                2. lon
                3. radius
                4. name
                5. note (if needed)
        '''
        if cal_distance(CENTER_OF_PATONG[0],CENTER_OF_PATONG[1], lat,lon) <= PATONG_RADIUS:
            swcs = list(self.clt_stop_while_collect.find())
            new_point = True
            for swc in swcs:
                avg_lat = sum(swc['lat'])/len(swc['lat'])
                avg_lon = sum(swc['lon'])/len(swc['lon'])
                if is_in_radius(avg_lat,avg_lon,NEW_BIN_CHECKING_RADIUS,lat,lon):
                    daydiff = (arrow.now()-arrow.get(swc['timestamp'][-1])).days
                    if daydiff > NEW_BIN_DISCARD_DURATION:
                        #remove this detection (and add new; see below)               
                        self.clt_stop_while_collect.delete_one({'_id':swc['_id']})                    
                    else:
                        new_point = False
                        # if this is new bin add it to db, then return its id
                        if swc['num_occurrence'] >= NEW_BIN_CHECKING_OCCURRENCE-1:
                            # add this bin to db then get its bin id
                            bin_data = {
                                'coords': [avg_lon,avg_lat],
                                'name': "auto_detected_bin",
                                'name_th':u'ถังถูกตรวจพบอัตโนมัติ',
                                'type':'auto'
                            }
                            #remove this detection                    
                            self.clt_stop_while_collect.delete_one({'_id':swc['_id']})
                            try:
                                pass #bin_id = self.add_new_bin(bin_data)
                            except Exception as e:
                                # Cannot add new bin
                                logging.error('self.add_new_bin(bin_data)')
                                logging.error(e)
                                return -1
                                
                            # refresh bin_info                        
                            cmd = ['php', '-f', '/opt/plagad/route_construction/to_realtime_db_on_firebase/put_bin_capacity.php']
                            try:
                                while True:
                                    output = check_output(cmd)
                                    if 'cannot' not in output:  
                                        break
                            except Exception as e:
                                logging.error("check_output(cmd): cannot update bin in Mu's put_bin_capacity.php")
                                logging.error(e)                        
                            self.refresh_bin_info()
                            return -1#bin_id
                        else:
                            self.clt_stop_while_collect.update_one(
                                {'_id':swc['_id']},
                                {'$inc':{'num_occurrence':1},
                                '$set':{'radius':NEW_BIN_CHECKING_RADIUS},
                                '$push':{'timestamp':arrow.utcnow().naive,'lat':lat,'lon':lon}}
                            )
                    break      
                    
            if new_point:
                # check whether this point is in the no_bin_location
                in_no_bin = False
                no_bin_location = list(self.clt_no_bin.find())  # [] in case there is no data
                for nb in no_bin_location:
                    if is_in_radius(nb['lat'],nb['lon'],nb['radius'],lat,lon):
                        in_no_bin = True
                        break        
                if not in_no_bin:
                    # check whether this point is in the NEW_BIN_CHECKING_RADIUS radius of any others
                    in_any_bin_radius = False
                    for bid in self.bin_info:
                        b_lat = self.bin_info[bid]['lat']
                        b_lon = self.bin_info[bid]['lon']
                        if is_in_radius(b_lat,b_lon,NEW_BIN_CHECKING_RADIUS,lat,lon):
                            in_any_bin_radius = True
                            break
                    if not in_any_bin_radius:
                        # add a new record
                        self.clt_stop_while_collect.insert_one({
                            'lat':[lat],'lon':[lon],
                            'timestamp':[arrow.utcnow().naive],
                            'num_occurrence':1
                        })
        
        return -1
    
    def add_new_bin(self, bin_data):
        """ add a bin to Sipp's Django trash API
        """
        headers = {
            'Authorization': 'Bearer '+self.sipp_token,
            'Content-Type':'application/json'            
        }       
        r = requests.post('https://api.traffy.xyz/v0/trash/', json=bin_data, headers=headers)
        
        return r.json()['id']
        
    def remove_bin(self, bid):
        """ remove a bin from Sipp's Django trash API
        """
        headers = {
            'Authorization': 'Bearer '+self.sipp_token,
            'Content-Type':'application/json'            
        }       
        r = requests.delete('https://api.traffy.xyz/v0/trash/'+bid+'/', headers=headers)
        
        return not ("detail" in r.json() and r.json()["detail"]=='Not found.')
        
    def unuse_bin(self, bid):
        """ mark a bin as unused in Sipp's Django trash API
        """
        headers = {
            'Authorization': 'Bearer '+self.sipp_token,
            'Content-Type':'application/json'            
        }       
        r = requests.patch('https://api.traffy.xyz/v0/trash/'+bid+'/', json={'note':'unused'}, headers=headers)
        
        return r.json()['note']=='unused'
        
    def push_latest_states_to_firebase(self, num_state=1, vid=None, init=False):
        ''' Refresh the latest num_state state(s) in firebase
            for Sujaray display in tester
            Note: must have self.truck before!
        '''
        user = self.auth.sign_in_with_email_and_password(email, password)   # sign-in
        if init:
            # get all truck latest 3 states and set them in firebase 
            for _vid in self.truck:
                cs = self.clt_state.find({'vid':_vid}, limit=num_state).sort('state_begin',-1) # get the last state instance
                if cs.count()>0:
                    # Change ObjectId to str
                    states = list(cs)
                    for st in states:
                        st['_id'] = str(st['_id'])  
                        st['state_begin'] = str(arrow.get(st['state_begin']))                      
                    self.db.child('kaya/truck_recent_states').child(_vid).set(states, user['idToken'])
             
        else:
            # specific vid which just updated
            cs = self.clt_state.find({'vid':vid}, limit=num_state).sort('state_begin',-1) # get the last state instance
            if cs.count()>0:  
                # Change ObjectId to str
                states = list(cs)
                for st in states:
                    st['_id'] = str(st['_id']) 
                    st['state_begin'] = str(arrow.get(st['state_begin']))    
                self.db.child('kaya/truck_recent_states').child(vid).set(states, user['idToken'])
                  
    def add_new_actual_state(self, answer):
        ''' Add an answer of state
            from Sujaray in the tester            
            answer (dict):
                _id
                duration
                state
                state_begin
                vid
        '''
        answer['state_id'] = answer.pop('_id')
        answer['state_begin'] = arrow.get(answer['state_begin']).to('utc').naive
        answer_id = self.clt_state_actual.insert_one(answer).inserted_id
        return str(answer_id)
                        
    def refresh_sipp_token(self):
	try:
            token = get_sipp_token()
	except:
	    print 'cannot get sipp token'
	    return
        self.sipp_token = token

    def refresh_bin_info(self):
        try:
            with open(BIN_INFO_FILE) as kaya_cache_file:    
                kaya_cache = json.load(kaya_cache_file)
            bin_info = {}
            for bid in kaya_cache:            
                bin_info[bid]={
                    'name':kaya_cache[bid]['name'],
                    'lat':kaya_cache[bid]['latitude'],
                    'lon':kaya_cache[bid]['longitude'],
                    'capacity':kaya_cache[bid]['capacity'],
                    'type':kaya_cache[bid]['type'],
                }
                if kaya_cache[bid]['sensor_attatched'] == "yes":
                    bin_info[bid]['level'] = kaya_cache[bid]['sensor']['volume_percentage']
                try:
                    _ts = arrow.get(arrow.get(kaya_cache[bid]['last_garbage_full_timestamp']).datetime, "Asia/Bangkok")
                    bin_info[bid]['full_timestamp'] = _ts
                except:
                    pass
                try:
                    _ts = arrow.get(arrow.get(kaya_cache[bid]['last_collected_timestamp']).datetime, "Asia/Bangkok")
                    bin_info[bid]['collected_timestamp'] = _ts
                except:
                    pass
            if len(bin_info)>0:
                self.bin_info = copy.deepcopy(bin_info)
            else:
                logging.error('cannot get bin info from json file (Mu)')
        except Exception as e:
            if self.bin_info == None:
                raise e
    
    def check_unused_bin(self):
        '''
            if the bins are not be collected for long time, it will be marked as "unused".
        '''
        for bid in self.bin_info:
            if 'collected_timestamp' in self.bin_info[bid]:
                last_collected_duration = (arrow.now()-self.bin_info[bid]['collected_timestamp']).days
                if last_collected_duration > BIN_UNUSED_DURATION:
                    # mark this bin as unused
                    try:
                        if not self.unuse_bin(bid):
                            logging.error("cannot note autobin "+bid)
                    except Exception as e:
                        logging.error("cannot note autobin "+bid)
                        logging.error(e)   
                        
                    # refresh bin_info                        
                    cmd = ['php', '-f', '/opt/plagad/route_construction/to_realtime_db_on_firebase/put_bin_capacity.php']
                    try:
                        if 'cannot' in check_output(cmd):  
                            logging.error("check_output(cmd): cannot update bin in Mu's put_bin_capacity.php")
                    except Exception as e:
                        logging.error("check_output(cmd): cannot run Mu's put_bin_capacity.php")
                        logging.error(e)                        
                    self.refresh_bin_info()
    
    def add_num_truck_in_state(self):
        # get today num truck from mongo
        today = arrow.now().replace(hour=0, minute=0, second=0)
        _from=today.to('utc').naive
        _to=today.shift(days=1).to('utc').naive
        
        today_nts = self.clt_num_truck_in_state.find_one({"from_time": {"$eq": _from}})
            
        if today_nts == None:
            # add new
            num_truck_in_state_group = {}

            for stg in Truck_state.groups:
                if stg == Truck_state.BEING_MAINTAINED:
                    continue
                cs = self.clt_state.aggregate(  
                     [  {"$match": {"state_begin": {"$gte": _from,"$lt": _to}, 'state':{'$in': Truck_state.groups[stg]}}},
                        {"$group":{ 
                               "_id": { 
                                    "h":{"$hour":"$state_begin"}
                               },
                                '_vid':{"$addToSet":'$vid'}
                            }
                        },
                        {'$unwind':"$_vid"},
                        { "$group":{ 
                               "_id": "$_id",
                               "total":{ "$sum": 1}
                            }
                        },
                     ]
                ) 
                num_truck_in_state_group[stg]={}
                for _cs in cs:
                    hod = (_cs['_id']['h'] +7)%24 #to Thai time
                    num_truck_in_state_group[stg][str(hod)] = _cs['total']
                    
            for h in range(arrow.now().hour):
                if str(h) not in num_truck_in_state_group['all']:
                    for stg in Truck_state.groups:
                        if stg == Truck_state.BEING_MAINTAINED:
                            continue
                        num_truck_in_state_group[stg][str(h)] = 0
            
            num_truck_in_state_group[Truck_state.BEING_MAINTAINED]={}
            for hod in range(arrow.now().hour):
                _to=today
                if hod < 23:
                    _to = _to.replace(hour=hod+1) 
                else:
                    _to = _to.replace(hour=hod, minute=59, second=59)  
                _to = _to.to('utc').naive
                cs = self.clt_state.aggregate(  
                         [  {"$match": {"state_begin": {"$lt": _to}}},
                            {"$group":{ 
                                   "_id": { 
                                        'vid':'$vid',
                                   },
                                   'lastStateTime': { '$last': "$state_begin" }, 
                                   'state':{ '$first': '$state'}
                                }
                            },
                            {"$match": {'state':Truck_state.BEING_MAINTAINED}},
                            {'$group': {'_id': None,'count': {'$sum':1}}},
                         ]
                    )
                try:
                    cnt = cs.next()['count']
                except:
                    cnt = 0
                num_truck_in_state_group[Truck_state.BEING_MAINTAINED][str(hod)]=cnt
                
            self.clt_num_truck_in_state.insert_one({
                'from_time': today.to('utc').naive,
                'to_time': today.replace(hour=23, minute=59, second=59).to('utc').naive,
                'num_truck_in_state_group': num_truck_in_state_group,
            })
        else:
            # append a new hour
            prev_hour = arrow.now().shift(hours=-1).replace(minute=0, second=0)
            _from=prev_hour.to('utc').naive
            _to=prev_hour.shift(hours=1).to('utc').naive
            for stg in Truck_state.groups:
                if stg == Truck_state.BEING_MAINTAINED:
                    continue
                cs = self.clt_state.aggregate(  
                     [  {"$match": {"state_begin": {"$gte": _from,"$lt": _to}, 'state':{'$in': Truck_state.groups[stg]}}},
                        {"$group":{ 
                               "_id": { 
                                    "h":{"$hour":"$state_begin"}
                               },
                                '_vid':{"$addToSet":'$vid'}
                            }
                        },
                        {'$unwind':"$_vid"},
                        { "$group":{ 
                               "_id": "$_id",
                               "total":{ "$sum": 1}
                            }
                        },
                     ]
                ) 
                num_truck_in_state_group[stg]={}
                for _cs in cs:
                    hod = (_cs['_id']['h'] +7)%24 #to Thai time
                    num_truck_in_state_group[stg][str(h)] = _cs['total']
            
            # fill cnt == 0
            for h in range(arrow.now().hour):
                if str(h) not in num_truck_in_state_group['all']:
                    for stg in Truck_state.groups:
                        if stg == Truck_state.BEING_MAINTAINED:
                            continue
                        num_truck_in_state_group[stg][str(hod)] = 0
                        
            # for maintain state
            num_truck_in_state_group[Truck_state.BEING_MAINTAINED]={}
            for hod in range(arrow.now().hour):
                _to=today
                if hod < 23:
                    _to = _to.replace(hour=hod+1) 
                else:
                    _to = _to.replace(hour=hod, minute=59, second=59)  
                _to = _to.to('utc').naive
                cs = self.clt_state.aggregate(  
                         [  {"$match": {"state_begin": {"$lt": _to}}},
                            {"$group":{ 
                                   "_id": { 
                                        'vid':'$vid',
                                   },
                                   'lastStateTime': { '$last': "$state_begin" }, 
                                   'state':{ '$first': '$state'}
                                }
                            },
                            {"$match": {'state':Truck_state.BEING_MAINTAINED}},
                            {'$group': {'_id': None,'count': {'$sum':1}}},
                         ]
                    )
                try:
                    cnt = cs.next()['count']
                except:
                    cnt = 0
                num_truck_in_state_group[Truck_state.BEING_MAINTAINED][str(hod)]=cnt
            
            # re-format for appending in mongo
            _set = {}
            h = arrow.now().hour
            for stg in Truck_state.groups:
                nts = num_truck_in_state_group[stg][str(h)]
                _set['num_truck_in_state_group.'+stg+'.'+str(h)] = nts
    
            self.clt_num_truck_in_state.update_one(
                    {'_id':today_nts['_id']},
                    {'$set':_set}
                )
            
    
    
    # def update_truck_status_firebase(self,vid,status):
        # ''' for Sujaray to display in smart patong web
        # '''
        # user = self.auth.sign_in_with_email_and_password(email, password)    
        # self.db.child('kaya/truck').child(vid).update({'state':status}, user['idToken'])

    def __del__(self):
        '''destructor'''
        self.sched.shutdown()
        
