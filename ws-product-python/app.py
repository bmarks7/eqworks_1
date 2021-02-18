# -*- coding: utf-8 -*-

import os
from flask import Flask, jsonify
import sqlalchemy
import datetime as dt

# web app
app = Flask(__name__)

# database engine
engine = sqlalchemy.create_engine(os.getenv('SQL_URI'))


global home_count
home_count = 0

global home_start
home_start = dt.datetime.now() 

@app.route('/')
def index():
    current_time = dt.datetime.now()

    global home_start
    global home_count

    diff = (current_time - home_start).total_seconds()# time since the beginning of current interval

    if diff >= 10:# if we are in a new interval, restart everything
        home_count = 1
        home_start = dt.datetime.now() 

        return 'Welcome to EQ Works 😎'
    
    else:#if we are still within an interval
        if home_count == 5:# if we have already processed to many requests in an interval
            wait_time = 10 - diff
            return 'Too many requests, ' + str(round(wait_time, 2)) + ' seconds until you can request again'

        else:#if we can still process requests
            home_count += 1
            return 'Welcome to EQ Works 😎'
    


global event_hour_count
event_hour_count = 0

global event_hour_start
event_hour_start = dt.datetime.now()

@app.route('/events/hourly')
def events_hourly():
    current_time = dt.datetime.now()

    global event_hour_start
    global event_hour_count

    diff = (current_time - event_hour_start).total_seconds()# time since the beginning of current interval

    if diff >= 10:# if we are in a new interval, restart everything
        event_hour_count = 1
        event_hour_start = dt.datetime.now() 

        return queryHelper('''
        SELECT date, hour, events
        FROM public.hourly_events
        ORDER BY date, hour
        LIMIT 168;
        ''')
    
    else:#if we are still within an interval
        if event_hour_count == 5:# if we have already processed to many requests in an interval
            wait_time = 10 - diff
            return 'Too many requests, ' + str(round(wait_time, 2)) + ' seconds until you can request again'

        else:#if we can still process requests
            event_hour_count += 1
            return queryHelper('''
            SELECT date, hour, events
            FROM public.hourly_events
            ORDER BY date, hour
            LIMIT 168;
            ''')


global event_daily_count
event_daily_count = 0

global event_daily_start
event_daily_start = dt.datetime.now()        

@app.route('/events/daily')
def events_daily():
    current_time = dt.datetime.now()

    global event_daily_start
    global event_daily_count

    diff = (current_time - event_daily_start).total_seconds()# time since the beginning of current interval

    if diff >= 10:# if we are in a new interval, restart everything
        event_daily_count = 1
        event_daily_start = dt.datetime.now() 

        return queryHelper('''
        SELECT date, SUM(events) AS events
        FROM public.hourly_events
        GROUP BY date
        ORDER BY date
        LIMIT 7;
        ''')
    
    else:#if we are still within an interval
        if event_daily_count == 5:# if we have already processed to many requests in an interval
            wait_time = 10 - diff
            return 'Too many requests, ' + str(round(wait_time, 2)) + ' seconds until you can request again'

        else:#if we can still process requests
            event_daily_count += 1
            return queryHelper('''
            SELECT date, SUM(events) AS events
            FROM public.hourly_events
            GROUP BY date
            ORDER BY date
            LIMIT 7;
            ''')


global stat_hourly_count
stat_hourly_count = 0

global stat_hourly_start
stat_hourly_start = dt.datetime.now()   

@app.route('/stats/hourly')
def stats_hourly():

    current_time = dt.datetime.now()

    global stat_hourly_start
    global stat_hourly_count

    diff = (current_time - stat_hourly_start).total_seconds()# time since the beginning of current interval

    if diff >= 10:# if we are in a new interval, restart everything
        stat_hourly_count = 1
        stat_hourly_start = dt.datetime.now() 

        return queryHelper('''
        SELECT date, hour, impressions, clicks, revenue
        FROM public.hourly_stats
        ORDER BY date, hour
        LIMIT 168;
        ''')
    
    else:#if we are still within an interval
        if stat_hourly_count == 5:# if we have already processed to many requests in an interval
            wait_time = 10 - diff
            return 'Too many requests, ' + str(round(wait_time, 2)) + ' seconds until you can request again'

        else:#if we can still process requests
            stat_hourly_count += 1
            return queryHelper('''
            SELECT date, hour, impressions, clicks, revenue
            FROM public.hourly_stats
            ORDER BY date, hour
            LIMIT 168;
            ''')
    

global stat_daily_count
stat_daily_count = 0

global stat_daily_start
stat_daily_start = dt.datetime.now()   

@app.route('/stats/daily')
def stats_daily():
    current_time = dt.datetime.now()

    global stat_daily_start
    global stat_daily_count

    diff = (current_time - stat_daily_start).total_seconds()# time since the beginning of current interval

    if diff >= 10:# if we are in a new interval, restart everything
        stat_daily_count = 1
        stat_daily_start = dt.datetime.now() 

        return queryHelper('''
            SELECT date,
                SUM(impressions) AS impressions,
                SUM(clicks) AS clicks,
                SUM(revenue) AS revenue
            FROM public.hourly_stats
            GROUP BY date
            ORDER BY date
            LIMIT 7;
        ''')
    
    else:#if we are still within an interval
        if stat_daily_count == 5:# if we have already processed to many requests in an interval
            wait_time = 10 - diff
            return 'Too many requests, ' + str(round(wait_time, 2)) + ' seconds until you can request again'

        else:#if we can still process requests
            stat_daily_count += 1
            return queryHelper('''
                SELECT date,
                    SUM(impressions) AS impressions,
                    SUM(clicks) AS clicks,
                    SUM(revenue) AS revenue
                FROM public.hourly_stats
                GROUP BY date
                ORDER BY date
                LIMIT 7;
            ''')
        

global poi_count
poi_count = 0

global poi_start
poi_start = dt.datetime.now() 

@app.route('/poi')
def poi():
    current_time = dt.datetime.now()

    global poi_start
    global poi_count

    diff = (current_time - poi_start).total_seconds()# time since the beginning of current interval

    if diff >= 10:# if we are in a new interval, restart everything
        poi_count = 1
        poi_start = dt.datetime.now() 

        return queryHelper('''
        SELECT *
        FROM public.poi;
        ''')
    
    else:#if we are still within an interval
        if poi_count == 5:# if we have already processed to many requests in an interval
            wait_time = 10 - diff
            return 'Too many requests, ' + str(round(wait_time, 2)) + ' seconds until you can request again'

        else:#if we can still process requests
            poi_count += 1
            return queryHelper('''
            SELECT *
            FROM public.poi;
            ''')


def queryHelper(query):
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return jsonify([dict(row.items()) for row in result])
