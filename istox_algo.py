# %%
'''
changed 24aug- 
main py, 
optionsinterest.py -deployed 29/8
option_chain.py - deployed 29/8
Introduce hash to kite instruments - option_hash, future_hash, symbol_hash for faster comparision -- deployed

6.  flat trade  - NorenApiFT.py flat trade changes -- deployed 9/5
4. NEXT_DAY logic changed to fix fri-mon RE exit errors --deployed 9/5 
3. wnt_strategy_end_time in view vw_reentry_eligible_orders and vw_tp_reentry_eligible_orders 9/5

changed 8 Sep- deployed on Sep 10 night
7. wnt_strat-end_time for all - should fix RE with Upcoming Monday and other cases
4. lot multiplier  -- changes to strategy & strat_history table,vw_activestrategies_v2,vw_combinedsltp_v4
9. Delay of 4-8s is not accepted - Prep order 5s+5s 10s
10. Greek issues - Rishi - more logging

Sep 11- deployed
14. Jithin issue fixed
14. Options interest change - try again if any strike is None

Sep 18- deployed
20. python lib for timescale
21. angel api change
14. Old symbol RBO NoneType Process12 issue - : BANKNIFTY2491151200C
18. telegram for failures - new column in login notifications type and view vw_active_Accounts ##UI
3.  RBO/DTR changes to take last incomplete candle
15. send_notification - after market tasks
16. timescaledb: istox_algo, option_stream.py, timescaledb.py- timescaledb for premium, delta and Candle data in TS db - 
ledb pip install setup
17. cleanup_timescaledb - jobtracker db entry, timescaledb.py file
15. redis_helper changes to docker 

Oct 2- deployed:
1. add events column - capture scheduler names
2. ins None for OHLC - fixed

Oct 24 - deployed
1. Change to wait for only 1 minute before marking rejected
23. Distributed locking v1 - RedLock init setup  
    a. python3 -m pip install redlock-py
    b. env file changes
25. redis and timescale config from env file
    a. env file changes
    b. src redi_helper
    c. src option_stream
26. Dipesh MSLC issue fix
27. db max_packet size changed to 100MB
28. Dipesh TSL % issue fixed

Oct 30 - deployed
12. Deploy to prod - # Reprocess Orders Algo Monitor. Along with Deploy istox_algo py events change.
13. OptionsInterest.py - change to protect coser=1 or 2 cases to be within endRange

Nov 4- deployed
15. OI change -MAX OPINT OptionsInterest.py and istox_algo.py
16. OHLC from TS - environment_config.tx istox_algo.py, old and new folders fro GIS v1 and v2
18. RE immed for MAX Oi cases - vw_reentry and vw_tp_reentry views
19. OI change -MAX OPINT OptionsInterest.py new proivder
20. Dipesh straddle higher

Nov 26- beta
3. Dipesh TSL on sell issue -  TSL for broker SL changes to vw_sl_trail and vw_sl_algo
17. Timescale config changes - /home/ec2-user/timescaledb/data/postgresql.con
 
Open-
1. option_stream and timescaledb - changes to stream to TS via  queue revrted
2. close DB pool connections (issue)

4. Dipesh Advanced SL for RE
11. Move TS outside algo
11. Local SQL runner issue - orders placed but SQL not run in DB because of algo issues. Use Redis Queue and check.

23. Distributed locking v1 - acquire and release locks for functions      
24. Distributed locking v2 - acquire and release locks for functions
16. timescaledb: for indicators - Dipesh RBO late entry bug - known 90s issue
22. Change sdf_1min to take from timescale and sync data to scripltp_1m 
2. kite use hash in instrument_by_symbol and custom_get_instru**
6. Delta neutral
7. Paper Trading 
9. Performance move - manager dicts like instruments and dic_accounts to redis
10. Alpha integration


'''
# %%
from __future__ import annotations

import multiprocessing
from multiprocessing import Manager,Queue, Pool
from multiprocessing import get_context

from redlock import Redlock

import logging 
import traceback
import pytz
import datetime
import logging.handlers
import time
logger_queue=None

ist=pytz.timezone("Asia/Calcutta")
    
def utcnow():
    return datetime.datetime.now(ist)

def listener_configurer():
    root = logging.getLogger()
    
    ist=pytz.timezone("Asia/Calcutta")
    log_file='logs/GenericOptionsTradingFramework'+datetime.datetime.now(ist).strftime("%Y-%m-%d")+'.log'
    formatter = logging.Formatter('%(processName)s, %(process)d, %(threadName)s, %(asctime)s, %(levelname)s, %(message)s')
    
    file_handler = logging.FileHandler(log_file)
    
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
    root.setLevel(logging.INFO)
    #root.setLevel(logging.DEBUG)
    
# logger queue listener
def logger_listener_process(queue):
    listener_configurer()
    #logging.info('logger_listener_process done')
    
    while True:
        while not queue.empty():
            record = queue.get()
            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        time.sleep(1)
        
# to be called in every worker process
def worker_logger_configurer(queue):
    h = logging.handlers.QueueHandler(queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(logging.INFO)    
    #root.setLevel(logging.DEBUG)
        
if __name__ == "__main__":

    logger_queue = Manager().Queue()
    logger_listener = multiprocessing.get_context("fork").Process(
        target=logger_listener_process, args=(logger_queue,), daemon=True)
    logger_listener.start()

    worker_logger_configurer(logger_queue) 
    mainlogger=logging.getLogger('worker')
    
    mainlogger.info('Program Started : '+str(utcnow()))


# %%
# Import for all

#from alice_blue import *
#from pya3 import *
from pya3 import Aliceblue,encrypt_string,Alice_Wrapper
from pya3 import TransactionType as TransactionTypeAlice2
from pya3 import OrderType as OrderTypeAlice2
from pya3 import ProductType as ProductTypeAlice2

#IIFL
from xts.Connect import XTSConnect


import requests
import json
from Cryptodome import Random
from Cryptodome.Cipher import AES
import hashlib
import base64

from  threading import Thread
import time
from datetime import timedelta

import sys
import signal
import threading
import pandas as pd
from threading import Thread
import random

import pyotp
import copy

#from SendNotifications import consume_queue

import numpy as np
np.seterr(divide='ignore', invalid='ignore')
from collections import namedtuple


import requests
import ast

import os
import concurrent
import json

from smartapi import SmartConnect #or from smartapi.smartConnect import SmartConnect
import requests
import math

import re

##KITE
from kiteext_v2 import KiteExt
import kiteconnect
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker

from NorenRestApiPy.NorenApi import  NorenApi
from NorenRestApiPy.NorenApiFT import NorenApi as NorenApiFT
import zipfile
import asyncio
from flattrade_session import get_session_token


from requests.structures import CaseInsensitiveDict
from furl import furl
import boto3
from OptionsInterest import datafeed

import mysql.connector
import pymysql
from dbutils.pooled_db import PooledDB

import configparser as cp

from RedisLtpFetch import RedisLtpFetcher
# %%
#Import for MAIN
if __name__ == "__main__":
    from mysql_to_oracle import ImportDataToOracle
    
    from GetHistoricalData import get_historical_data
    from GetIndicatorSignal import get_indicator_signal
    
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.executors.pool import ThreadPoolExecutor,ProcessPoolExecutor
    from apscheduler.events import ( EVENT_JOB_EXECUTED, EVENT_JOB_SUBMITTED,EVENT_JOB_ERROR)
    
    from OptionsStreamStart import OptionsStreamStart
    
    import psycopg2
    from src.expressoptionchain.timescaledb import TimescalePipeline

# %%
#for multi-processing needs impelementing our own Locks

class MyLock:
    x=0
    expiry_time=0
    
    def locked(self):
        if self.expiry_time<=time.time():
            self.x=0
            
        return self.x
    
    def acquire(self,duration=30):
        self.x=1
        self.expiry_time=time.time()+duration
    
    def release(self):
        self.x=0


# %%
#Variable for all the processes
algoName='istox_algo.py'

group_chat_id =None

dbhost =None
dbport =None
dbuser =None
dbpass =None
dbname =None
#dbname =None

gsheetId =None

primary_alice =None
primary_alice2 =None
keepsl_kite_api_key =None
primary_kite =None
primary_kite2 =None
primary_kite_historical_data=None
primary_angel=None
primary_broker =None
primary_shoonya =None
primary_iifl =None
primary_flat =None


angel_master_instruments_url=None
zerodha_failed_order_hardcoded_id =None

env =None

prod_login_all=None

env_maxconnections =None
env_mincached =None
env_maxcached =None
env_max_workers =None
env_max_order_processes =None
thread_lock_timeout =None

env_mpp=None
shutdown_flag=None

sqsurl_db=None

redis_host =None
redis_port =None
redis_db =None

stream_timescale=None

amo_flag       =None
dic_keywords   =None
ist            =None
amo_dummy_date =None

emoji_warning  =None
emoji_success  =None
emoji_dollar   =None
emoji_profit   =None
emoji_loss     =None


angel_broker_code    =None
zerodha_broker_code  =None
finvasia_broker_code =None
alice_broker_code    =None
iifl_broker_code     =None
flat_broker_code     =None

nse_holdiays =None

sdf                           =None
sdf_1min                      =None
oi_data                       =None
global_instruments            =None
global_instruments_1min       =None
q_var_global_instruments_1min =None
global_candle_durations       =None
global_indicators             =None

global_number_of_candles      =None
limit_price_factor_sell       =None
limit_price_factor_buy        =None

threadlist=None

order_tag_prefix = None

#broker level change
Instrument =namedtuple('Instrument', ['exchange', 'token', 'symbol',
                                           'name', 'expiry', 'lot_size','base_symbol','strike','call_put'])

Instrument.__new__.__defaults__= (None,) * len(Instrument._fields)

symbols=['NFO:NIFTY','BFO:SENSEX','NFO:BANKNIFTY','NFO:FINNIFTY','NFO:MIDCPNIFTY','BFO:BANKEX']
indice_symbols=['NSE:NIFTY 50','BSE:SENSEX','NSE:NIFTY BANK','NSE:NIFTY FIN SERVICE','NSE:NIFTY MID SELECT','BSE:BANKEX']
        
nse_indices=None
global_delta_next_trading_day=None
global_dt_today=None

sqs_client =None

redis_server= None

dic_accounts   =None
instruments_by_symbol=None

kite_instruments    =None
angel_instruments   =None
shoonya_instruments =None
alice_instruments   =None
iifl_instruments    =None
flattrade_instruments=None

pool = None

broker=None

qltp = None
qdb = None
qsms = None
qorders = None

exp_weekly_def          =None
exp_weekly_fin          =None
exp_weekly_midcp        =None
exp_weekly_bnf          =None
exp_weekly_sensex       =None

exp_monthly_def         =None
exp_monthly_fin         =None
exp_monthly_midcp       =None
exp_monthly_bnf         =None
exp_monthly_sensex      =None

exp_monthly_plus_def    =None
exp_monthly_plus_fin    =None
exp_monthly_plus_midcp  =None
exp_monthly_plus_bnf    =None
exp_monthly_plus_sensex =None


exp_future_weekly_def   =None
exp_future_weekly_fin   =None
exp_future_weekly_midcp =None
exp_future_weekly_bnf   =None
exp_future_weekly_sensex=None

exp_future_monthly_def  =None
exp_future_monthly_fin  =None
exp_future_monthly_midcp=None
exp_future_monthly_bnf  =None
exp_future_monthly_sensex=None

EXP_DAY_DEF             =None
EXP_DAY_FIN             =None
EXP_DAY_MIDCP           =None
EXP_DAY_BNF             =None
EXP_DAY_SENSEX          =None

MONTHLY_EXP_DAY_DEF     =None
MONTHLY_EXP_DAY_FIN     =None
MONTHLY_EXP_DAY_MIDCP   =None
MONTHLY_EXP_DAY_BNF     =None
MONTHLY_EXP_DAY_SENSEX  =None


env_config_filename='environment_config.txt'

config = cp.ConfigParser()
config.read(env_config_filename)

dbhost = config.get("env", "dbhost")
dbport = int(config.get("env", "dbport"))
dbuser = config.get("env", "dbuser")
dbpass = config.get("env", "dbpass")
dbname = config.get("env", "dbname")
env_maxconnections = int(config.get("env", "maxconnections"))
env_mincached = int(config.get("env", "mincached"))
env_maxcached = int(config.get("env", "maxcached"))

redis_host=config.get("env", "redis_host")
redis_port=config.get("env", "redis_port")
redis_db=config.get("env", "redis_db")

stream_timescale=int(config.get("env", "stream_timescale"))

#Variable Initialization 
if __name__ == "__main__":
    #env_config_filename='environment_config.txt'

    #config = cp.ConfigParser()
    #config.read(env_config_filename)
    group_chat_id = config.get("env", "group_chat_id")
    start_options_stream=config.get("env", "start_options_stream")
    #dbhost = config.get("env", "dbhost")
    #dbport = int(config.get("env", "dbport"))
    #dbuser = config.get("env", "dbuser")
    #dbpass = config.get("env", "dbpass")
    #dbname = config.get("env", "dbname")
    #env_maxconnections = int(config.get("env", "maxconnections"))
    #env_mincached = int(config.get("env", "mincached"))
    #env_maxcached = int(config.get("env", "maxcached"))
    #dbname = "core"

    gsheetId = config.get("env", "gsheetId")

    primary_alice = int(config.get("env", "primary_alice"))
    primary_alice2 = int(config.get("env", "primary_alice2"))
    
    keepsl_kite_api_key= int(config.get("env", "keepsl_kite_api_key"))
    primary_kite = int(config.get("env", "primary_kite"))
    primary_kite2 = int(config.get("env", "primary_kite2"))
    primary_kite_historical_data=int(config.get("env", "primary_kite_historical_data"))
    primary_angel= int(config.get("env", "primary_angel"))
    primary_broker = int(config.get("env", "primary_broker"))
    primary_shoonya = int(config.get("env", "primary_shoonya"))
    primary_iifl = int(config.get("env", "primary_iifl"))
    primary_flat = int(config.get("env", "primary_flat"))
    

    angel_master_instruments_url= config.get("env", "angel_master_instruments_url")


    zerodha_failed_order_hardcoded_id = int(config.get("env", "zerodha_failed_order_hardcoded_id"))

    env = config.get("env", "env")

    order_tag_prefix = config.get("env", "order_tag_prefix")
    prod_login_all=int(config.get("env", "prod_login_all"))

    
    env_max_workers = int(config.get("env", "max_workers"))
    env_max_order_processes = int(config.get("env", "max_order_processes"))
    thread_lock_timeout = int(config.get("env", "thread_lock_timeout"))

    env_mpp=1

    shutdown_flag= int(config.get("env", "shutdown_flag"))

    sqsurl_db=config.get("env", "sqsurl_db")
    
    timescale_connection=config.get("env", "timescale_connection")
    
    amo_flag       = 0
    
    #dic_accounts_kite={}
    dic_keywords   = {}
    ist            = pytz.timezone("Asia/Calcutta")
    amo_dummy_date = '2000-01-01'

    emoji_warning  = u'\U0000274C'+' '
    emoji_success  = u'\U00002705'+' '
    emoji_dollar   = u'\U0001f4b0'+' '
    emoji_profit   = u'\U0001F53C'+' '
    emoji_loss     = u'\U0001F53B'+' '


    angel_broker_code    = 25
    zerodha_broker_code  = 26
    finvasia_broker_code = 70
    alice_broker_code    = 71
    iifl_broker_code     = 72
    flat_broker_code     = 73

    # Variables are part of Fork process used to download candle data 
    sdf                           = {}
    sdf_1min                      = {}
    oi_data                       = {}
    global_instruments            = []
    global_instruments_1min       = []
    q_var_global_instruments_1min = Queue()
    global_candle_durations       = []
    global_indicators             = []

    global_number_of_candles      = 450 # change is to accomodate 1 min candles. approx 7 hours is 420
    limit_price_factor_sell       = 0.98
    limit_price_factor_buy        = 1.02

    Instrument =namedtuple('Instrument', ['exchange', 'token', 'symbol',
                                           'name', 'expiry', 'lot_size','base_symbol','strike','call_put'])

    Instrument.__new__.__defaults__= (None,) * len(Instrument._fields)


    nse_indices=['NIFTY FIN SERVICE', 'NIFTY 50', 'NIFTY BANK', 'NIFTY MID SELECT']


    global_delta_next_trading_day=1

    global_dt_today=datetime.datetime.now(ist).date()

    threadlist=[] #to store threads for monitoring
    
    aws_access_key_id=config.get("env", "aws_access_key_id")
    aws_secret_access_key=config.get("env", "aws_secret_access_key")
    region=config.get("env", "region")
    
    boto3_session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region
    )

    sqs_client = boto3_session.client("sqs", region_name="ap-south-1")
    
    redis_server = Redlock([{"host": redis_host, "port": int(redis_port), "db": int(redis_db)}])

    #Shared variables across processes
    dic_accounts   = Manager().dict() #dic_accounts   = {} change is for MPP- Will create one proxy process per variable
    instruments_by_symbol = Manager().dict()
    
    kite_instruments    = {}
    angel_instruments   = {}
    shoonya_instruments = {}
    alice_instruments   = {}
    iifl_instruments    = {}
    flattrade_instruments={}
    
    qltp = Manager().Queue()
    qdb = Manager().Queue()
    qdb_alternate= Queue()
    qsms = Manager().Queue()
    qorders = Manager().Queue()


# %%
# # DB Connection

def db_connect(dbName, maxconnections=env_maxconnections):
    mainlogger.info("Function db_connect : dbName : " + str(dbName))
    pool           = PooledDB(
    creator        = pymysql,  #Modules that use linked databases
    maxconnections = maxconnections,  #The maximum number of connections allowed in the connection pool, 0 and None means no limit on the number of connections
    mincached      = env_mincached,  #At least one free link created in the link pool during initialization, 0 means not created
    maxcached      = env_maxcached,  #The most idle link in the link pool, 0 and None are not restricted
    maxshared      = 0,  #The maximum number of links shared in the link pool, 0 and None indicate all shares. PS: Useless, because the threadsafety of modules such as pymysql and MySQLdb is 1. If all values are set, _maxcached will always be 0, so all links will always be shared. 
    blocking       = True,#Whether to block waiting if there is no connection available in the connection pool. True, wait; False, don't wait and then report error
    maxusage       = None, #The number of times a link is reused at most, None means unlimited
    setsession     = ["SET collation_connection = 'latin1_swedish_ci'",
                "SET tx_isolation = 'READ-COMMITTED'"],  #A list of commands to execute before starting a session. Such as: ["set datestyle to...", "set time zone..."]
    ping           = 0,
    host           = dbhost,
    port           = dbport,
    user           = dbuser,
    password       = dbpass,
    database       = dbName
    )
    return pool

if __name__ == "__main__":
    pool = db_connect(dbname)


# # Global Variables


#backend DB tasks from algo
def db_algo_tasks():
    mainlogger.info('Function : db_algo_tasks')
    try:
        dt=datetime.datetime.now(ist)

        if (dt>ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 8,30,0,0)) and 
           dt<ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 15,35,0,0))):
            
            update_ordergroup_status() #update strategy status
            disable_erroneous_startegies() #global check of orders per orderlegID
            mtm_account_alert() #mtm based account alert
            
    except Exception as e:
        mainlogger.exception('db_algo_tasks failed '+ str(e))
    
#db_algo_tasks()


# # Threads and Process Monitor and Restarter

# %%


def thread_monitoring_caller():
    mainlogger.info('function: thread_monitoring_caller')
    
    global threadlist
    
    for i in range(len(threadlist)):
        threadlist[i]['object']=thread_monitoring(threadlist[i]['object'],threadlist[i]['function'])
    
def thread_monitoring(t,funname):
    #mainlogger.info('function: thread_monitoring_caller funname : ' + str(funname))
    if not t.is_alive():
        if 'process_' in funname:
            p=multiprocessing.Process(target=globals()[funname])
            #p.start()
            send_notification(group_chat_id,'process died - '+funname)
            return p
        else:
            t=threading.Thread(target=globals()[funname], daemon=True)
            t.start()
            send_notification(group_chat_id,'thread restarted - '+funname)
            return t
        
    return t
    
#thread_monitoring_caller()


# # Setup Queues

# %%
##local queue to unblock calling thread

def worker_sub_ltp():
    while True:
        try:
            while not qltp.empty():
                try:
                    item = qltp.get()
                    broker.subscribe_to_instrument_final(item['exchange'],item['symbol'])
                except Exception as e:
                    mainlogger.exception('Q worker_sub_ltp failed '+ str(e))

            time.sleep(60)
            
        except Exception as e:
            mainlogger.exception('worker_sub_ltp failed '+ str(e))
            send_notification(group_chat_id,'worker_sub_ltp failed - will be retried')
            time.sleep(60)
            
#Turn-on the worker thread for sub to ltp.
if __name__ == "__main__":
    #qltp = Queue()
    t=threading.Thread(target=worker_sub_ltp, daemon=True)
    t.start()
    threadlist.append({'object':t,'function':'worker_sub_ltp'})


def worker_db_update():
    while True:
        try:
            #Run DB related algo tasks
            db_algo_tasks()
            time.sleep(5)
        except Exception as e:
            mainlogger.exception('worker_db_update failed '+ str(e))
            send_notification(group_chat_id,'worker_db_update failed - will be retried')
            time.sleep(5)
    

# # Notifications Handling Process Like Telegram

# %%

def worker_send_message():
    while True:
        try:
            while not qsms.empty():
                try:
                    item = qsms.get()
                    ##print("worker_send_message", item)
                    sqs_send_message_final(item['chat_id'], item['msg'])
                    #qsms.task_done()
                except Exception as e:
                    mainlogger.exception('Q send notification failed '+ str(e))

            time.sleep(30)

        except Exception as e:
            mainlogger.exception('worker_send_message failed '+ str(e))
            send_notification(group_chat_id,'worker_send_message failed - will be retried')
            time.sleep(60)

#Turn-on the worker thread for messaging.
if __name__ == "__main__":
    #qsms = Queue()
    t=threading.Thread(target=worker_send_message, daemon=True)
    t.start()
    threadlist.append({'object':t,'function':'worker_send_message'})

def sqs_send_message(chat_id, msg):
    mainlogger.info("sqs_send_message chat_id : " + str(chat_id))
    qsms.put({'chat_id':chat_id,'msg':msg})
    
def send_notification(chat_id,msg):
    mainlogger.info('send_notification chat_id : ' + str(chat_id))
    try:
        if chat_id!='NA' and ('##' in chat_id):
            parts=chat_id.split('##')
            chat_id=parts[1]
            if parts[0]=='1':
                sqs_send_message(chat_id,msg)
            elif parts[0]=='2' and emoji_warning in msg:
                sqs_send_message(chat_id,msg)
            #else:
            #    mainlogger.info("msg:"+str(msg)) ##remove before deployment
        
        elif chat_id!='NA':
            sqs_send_message(chat_id,msg)
            
            ##print(msg)
            #q.put({'chat_id':chat_id,'msg':msg})
    except Exception as e:
        mainlogger.exception('send notification failed '+ str(e))

    
##using SQS
def sqs_send_message_final(chat_id, msg):
    mainlogger.info("sqs_send_message_final chat_id : " + str(chat_id))
    #mainlogger.info('send_message')
    try:
        #sqs_client = boto3.client("sqs", region_name="ap-south-1")

        message={"id": chat_id, 'message':msg, "MessageGroupId":1}

        #MessageBody=json.dumps(message)
        ##print("MessageBody", MessageBody)
        response = sqs_client.send_message(
            QueueUrl="https://sqs.ap-south-1.amazonaws.com/497590156273/istox_sqs_telegrams.fifo",
            MessageBody=json.dumps(message, default=str),
            MessageGroupId="1"
        )
        ##print(response)
        mainlogger.info(response)
    except Exception as e:
        mainlogger.exception('send notification failed '+ str(e))
        


# %%

##local queue to DB updates


def send_to_db_queue(message):
    mainlogger.info("send_to_db_queue")
    qdb.put(message)

def db_sql_runner(coredb, cursor, item):
    mainlogger.info("db_sql_runner")
    try:
        cursor.execute(item['sql_stmt'],item['dic'])
        coredb.commit()
    except Exception as e:
        mainlogger.exception('db_sql_runner failed '+ str(e))
        item['wait_time']=datetime.datetime.now() + timedelta(seconds=3)
        qdb_alternate.put(item)

def worker_push_alternate_to_dbq():
    while True:
        try:
            while not qdb_alternate.empty():
                try:
                    item = qdb_alternate.get()
                    
                    while item['wait_time']>datetime.datetime.now():
                        time.sleep(0.5)
                    qdb.put(item)
                    
                except Exception as e:
                    mainlogger.exception('worker_push_alternate_to_dbq failed '+ str(e))

            time.sleep(1)

        except Exception as e:
            mainlogger.exception('worker_push_alternate_to_dbq failed '+ str(e))
            time.sleep(2)
            
def worker_push_to_db_sqs():
    #To-Do: change function name as we are not using sqs anymore
    coredb=pool.connection()
    cursor=coredb.cursor()
    
    while True:
        try:
            while not qdb.empty():
                try:
                    item = qdb.get()
                    #sqs_send_db_updates(item)
                    db_sql_runner(coredb, cursor, item)
                except Exception as e:
                    mainlogger.exception('Q send to db sqs failed '+ str(e))

            time.sleep(1)

        except Exception as e:
            mainlogger.exception('worker_push_to_db_sqs failed '+ str(e))
            send_notification(group_chat_id,'worker_push_to_db_sqs failed - will be retried')
            time.sleep(2)

#Turn-on the worker thread for messaging.
if __name__ == "__main__":
    #qdb = Queue()
    t=threading.Thread(target=worker_push_to_db_sqs, daemon=True)
    t.start()
    threadlist.append({'object':t,'function':'worker_push_to_db_sqs'})
    
    t=threading.Thread(target=worker_push_alternate_to_dbq, daemon=True)
    t.start()
    threadlist.append({'object':t,'function':'worker_push_alternate_to_dbq'})
    

def sqs_send_db_updates(message):
    #mainlogger.info('sqs_send_db_updates')
    try:
        #message={"id": chat_id, 'message':msg, "MessageGroupId":1}

        #MessageBody=json.dumps(message)
        ##print("MessageBody", MessageBody)
        ##print(json.dumps(message))
        response = sqs_client.send_message(
            QueueUrl=sqsurl_db,
            MessageBody=json.dumps(message, default=str),
            MessageGroupId=str(random.randrange(100,1000000)),
            MessageDeduplicationId=str(random.randrange(100,1000000))
        )
        ##print(response)
        mainlogger.info(response)
    except Exception as e:
        mainlogger.exception('push to queue sqs_send_db_updates failed '+ str(e))


# %%

if __name__ == "__main__":
    send_notification(group_chat_id,'Algo Started in mode:'+env)
    mainlogger.info('done')


# # DB load Instruments (Unused)

# %%

'''
def db_load_instruments(broker, df):

    #return 1

    mainlogger.info('Function : db_load_instruments broker : ' + str(broker) + " df : " + str(df))

    coredb=pool.connection()
    cursor=coredb.cursor()

    broker_alice='alice'
    broker_kite='kite'
    broker_angel='angel'
    broker_finv='finv'

    alice_contract_cols={'exchange':'Exch',
     'symbol':'Symbol',
     'token':'Token',
     'instrument_type':'Instrument Type',
     'option_type':'Option Type',
     'strike':'Strike Price',
     'trading_symbol':'Trading Symbol',
     'expiry':'Expiry Date',
     'lot_size':'Lot Size'}

    kite_contract_cols={'exchange':'exchange',
     'symbol':'name',
     'token':'instrument_token',
     'instrument_type':'segment',
     'option_type':'instrument_type',
     'strike':'strike',
     'trading_symbol':'tradingsymbol',
     'expiry':'expiry',
     'lot_size':'lot_size'}


    angel_contract_cols={'exchange':'exch_seg',
     'symbol':'name',
     'token':'token',
     'instrument_type':'instrumenttype',
     'option_type':'symbol',
     'strike':'strike',
     'trading_symbol':'symbol',
     'expiry':'expiry',
     'lot_size':'lotsize'}

    finv_contract_cols={'exchange':'Exchange',
     'symbol':'Symbol',
     'token':'Token',
     'instrument_type':'Instrument',
     'option_type':'OptionType',
     'strike':'StrikePrice',
     'trading_symbol':'TradingSymbol',
     'expiry':'Expiry',
     'lot_size':'LotSize'}

    del_sql = ('delete from broker_contracts where broker=%(broker)s')
    ins_sql = ("INSERT INTO broker_contracts (broker, token, exchange, symbol, trading_symbol, expiry, instrument_type, option_type, strike, lot_size) "
               "VALUES(%(broker)s,%(token)s,%(exchange)s,%(symbol)s,%(trading_symbol)s,%(expiry)s,%(instrument_type)s,%(option_type)s,%(strike)s,%(lot_size)s)"
              )

    if len(df)==0:
        return 0

    mainlogger.info('Function : db_load_instruments delete..')
    dic={'broker':broker}
    cursor.execute(del_sql,dic)
    coredb.commit()

    mainlogger.info('Function : db_load_instruments delete done..')
    broker_cols={}

    if broker==broker_alice:
        broker_cols=alice_contract_cols
    elif broker==broker_kite:
        broker_cols=kite_contract_cols
    elif broker==broker_angel:
        broker_cols=angel_contract_cols
    elif broker==broker_finv:
        broker_cols=finv_contract_cols

    if broker==broker_finv:
        df['Expiry'] = pd.to_datetime(df['Expiry'], format='%d-%b-%Y')

    df = df.apply(lambda x: x.astype(str).str.strip()).replace('', np.nan)
    df = df.apply(lambda x: x.astype(str).str.strip()).replace('NaT', np.nan)
    df = df.apply(lambda x: x.astype(str).str.strip()).replace('nan', np.nan)

    df = df.replace({np.nan: None})
    #df= df.replace({'NaT': None}, inplace=True)


    mainlogger.info('Function : db_load_instruments inserts started..')
    for index,row in df.iterrows():

        try:
            dic={'broker':broker,
            'exchange': row[broker_cols['exchange']],
            'symbol': row[broker_cols['symbol']],
            'token':row[broker_cols['token']],
            'instrument_type':row[broker_cols['instrument_type']],
            'option_type':row[broker_cols['option_type']],
            'strike':row[broker_cols['strike']],
            'trading_symbol':row[broker_cols['trading_symbol']],
            'expiry':row[broker_cols['expiry']],
            'lot_size':row[broker_cols['lot_size']]
            }

            ##print(ins_sql,dic)
            cursor.execute(ins_sql,dic)

        except Exception as e:

            break

    coredb.commit()
'''
# # Angel Class

# %%


# # Handle Program Kills

# %%


def handler_stop_signals(signum, frame):
    mainlogger.info('Function : handler_stop_signals Program Kill Received signum : ' + str(signum) + " frame : " + str(frame))
    return 1

    #in multiprocessing we can't restart the algo so ignore rest of the code
    '''
    ##print('Program Kill Received',signum,frame)
    unlock_db()
    send_notification(group_chat_id, emoji_warning+'Algo Kill Received')
    scheduler.shutdown(wait=False)
    time.sleep(60)
    
    send_notification(group_chat_id, emoji_warning+'Algo restart attempted..')
    ##restart the algo
    subprocess.Popen("cd /home/ec2-user/;nohup python3 istox_algo.py &",
                 shell=True,
                 preexec_fn=os.setpgrp)
    sys.exit(0)
    '''
signal.signal(signal.SIGTERM, handler_stop_signals)
signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGPIPE, handler_stop_signals)


# # Handle Exceptions

# %%


def handle_exception(msg,e,chat_id,both,account_id=False):
    try:
        if account_id==True:
            try:
                chat_id=dic_accounts[chat_id]['chat_id']
            except:
                chat_id='NA'
                
        mainlogger.exception(msg+ ' ' + e)
        send_notification(group_chat_id, emoji_warning+msg+' '+e)
        if both==True and chat_id!="NA":
            send_notification(chat_id, emoji_warning+msg)
    except Exception as e:
        mainlogger.exception(str(e))


# ## IS MARKET OPEN

# %%


def is_market_open():
    dt=datetime.datetime.now(ist) 
    
    if dt.strftime("%m-%d-%y") in nse_holdiays or dt.strftime("%A") in ('Saturday','Sunday'):
        return False
   
    if (dt>ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 9,15,0,0)) and 
               dt<ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 15,29,0,0))):
        return True
    
    return False

##algo time until 5 pm
def is_algo_hours():
    dt=datetime.datetime.now(ist)  
    
    if dt.strftime("%m-%d-%y") in nse_holdiays or dt.strftime("%A") in ('Saturday','Sunday'):
        return False
    
    if (dt>ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 17,0,0,0))):
        return False
    
    return True
#is_algo_hours()


# # Holiday Shutdown Logic

# %%


def ec2_shutdown(my_id):
    mainlogger.info('Function : ec2_shutdown my_id : ' + str(my_id))

    import boto.ec2
    import boto.utils

    conn = boto.ec2.connect_to_region("ap-south-1") # or your region
    # Get the current instance's id
    #my_id = boto.utils.get_instance_metadata()['instance-id']

    conn.stop_instances(instance_ids=[my_id])

def ec2_restart():
    mainlogger.info('Function : ec2_restart')

    import boto.ec2
    import boto.utils

    conn = boto.ec2.connect_to_region("ap-south-1") # or your region
    # Get the current instance's id
    my_id = boto.utils.get_instance_metadata()['instance-id']

    conn.reboot_instances(instance_ids=[my_id])
#ec2_shutdown('i-01566635eabdfeeff')  


# %%

if __name__ == "__main__":
    coredb = pool.connection()

    sql="select date_format(date,'%m-%d-%y') as dates from holidaycalendar"
    df = pd.read_sql(sql, coredb)
    df = df.replace({np.nan: None})

    nse_holdiays=df['dates'].to_list()

    coredb.close()

    mode=env
    if mode=='prod':
        dt=datetime.datetime.now(ist)
        if dt.strftime("%m-%d-%y") in nse_holdiays or dt.strftime("%A") in ('Saturday','Sunday'):
            mainlogger.info('Holiday and Algo is shutting down in 10 mins kill algo to prevent shutdown')
            if shutdown_flag==1:
                send_notification(group_chat_id,'Holiday and Algo is shutting down in 10 mins.')
                time.sleep(600) #15 mins to kill the algo to avoid shutdown of server
                ec2_shutdown('i-01566635eabdfeeff')
                exit(0) ##exit the program


# # Globally Lock DB

# %%


#dblock table lock value 1 is locked and 0 is unlocked
#algo globally locks DB to avoid second instance of algo or accidental starts operating on the same DB
     
if __name__ == "__main__":
    coredb = pool.connection()

    sql="select locked from dblock "
    df = pd.read_sql(sql, coredb)
    df = df.replace({np.nan: None})

    if mode=='prod' and (df.iloc(0)[0][0]==1):
        #print('DB is locked')
        mainlogger.exception('DB is locked so algo cannot be started. Check if another algo is already running!')
        send_notification(group_chat_id,'DB is locked so algo cannot be started. Check if another algo is already running!')
        time.sleep(60)
        exit(0) ##exit the program

    else:
        cursor=coredb.cursor()
        upd_sql=("update dblock set locked=1")
        rows=cursor.execute(upd_sql)
        coredb.commit()
        mainlogger.info('Algo starting so DB is locked')
        coredb.close()
        
        
        

# # Angel Class

# %%


class AngelBroker:

    feed_status=datetime.datetime.now(ist)
    socket_opened = False
    ws_subscribe_list=[]
    
    ##check if token is still valid
    def check_login_token(self,acc_id): 
        mainlogger.info('Function : Angel check_login_token acc_id : ' + str(acc_id))
        
        mainlogger.info(str(dic_accounts[acc_id]))
        dic_accounts[acc_id]['lock'].acquire(thread_lock_timeout)
        
        if (datetime.datetime.now()-dic_accounts[acc_id]['token_refresh']).total_seconds()<5:
            if dic_accounts[acc_id]['lock'].locked()==True:
                dic_accounts[acc_id]['lock'].release()
                
            mainlogger.info(str(dic_accounts[acc_id]))
            return 1
        
        try:
            if dic_accounts[acc_id]['token'] is None:
                self.account_single_login(acc_id)
                dic_accounts[acc_id]['token_refresh']=datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked()==True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                
            elif dic_accounts[acc_id]['token'].ltpData('NSE', 'SBIN-EQ', 3045)['errorcode']=='':
                dic_accounts[acc_id]['token_refresh']=datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked()==True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                
            elif dic_accounts[acc_id]['token'].ltpData('NSE', 'SBIN-EQ', 3045)['errorcode']!='':
                self.account_single_login(acc_id)
                dic_accounts[acc_id]['token_refresh']=datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked()==True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                    
        except Exception as e:
            mainlogger.exception(str(e))
            if dic_accounts[acc_id]['lock'].locked()==True:
                dic_accounts[acc_id]['lock'].release()
            mainlogger.info(str(dic_accounts[acc_id]))     

    ##Single LOGIN to Accounts
    def account_single_login(self,acc_id):
        mainlogger.info('Function : Angel single login acc_id : ' + str(acc_id))
        
        global pool
        global dic_accounts
        global angel_instruments
        #kite_instruments={}
        
        #broker_id=26 #alice broker id
        
        coredb = pool.connection()
        sql="select * from vw_activeaccounts where ID="+str(acc_id)
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        for index, row in df.iterrows():
            for attempt in range(2):
                try:
                    obj=SmartConnect(api_key=row['api_secret'])
                    
                    two_fa=pyotp.TOTP(row['two_fa']).now()
                    data=obj.generateSession(row['username'],str(row['password'].decode('ascii')),two_fa)
                    
                    refreshToken=data['data']['refreshToken']
                    
                    alice_blue1=obj
                    
                    dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':alice_blue1,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':dic_accounts[row['ID']]['lock'],'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}

                    mainlogger.info('Profile :'+str(alice_blue1.getProfile(refreshToken)))
                    reset_login_failed_accounts(row['ID'])
                    
                    for tries in range(2):
                        try:
                            if angel_instruments is None or len(angel_instruments)==0:
                                try:
                                    d = requests.get(angel_master_instruments_url).json()
                                    angel_instruments = pd.DataFrame.from_dict(d)
                                    angel_instruments['expiry'] = pd.to_datetime(angel_instruments['expiry']).apply(lambda x: x.date())
                                    angel_instruments = angel_instruments.astype({'strike': float})
                                    
                                    mainlogger.info('loaded angel instruments')
                                    send_notification(group_chat_id,'Loaded Angel instruments '+str(len(angel_instruments)))

                                except Exception as e:
                                    #handle_exception('Kite instruments load failed '+row['username'],str(e),row['tele_chat_id'],True) 
                                    send_notification(group_chat_id, emoji_warning+'Failed to load angel instruments')
                                    mainlogger.exception('Angel instruments load failed')
                                    
                        except Exception as e:
                            time.sleep(5)
                            pass
                        else:
                            break;
                            
                except Exception as e:
                    try:
                        dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':None,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':dic_accounts[row['ID']]['lock'],'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}
                        handle_exception('Angel Login Failed '+row['username'],str(e),row['tele_chat_id'],True) 
                        time.sleep(1)
                    except Exception as e:
                        mainlogger.exception(str(e))
                else:
                    break;
                
        coredb.close()
    
    ##LOGIN to Accounts
    def account_login(self):
        mainlogger.info('Function : AngelBroker login')
        
        global pool
        global dic_accounts
        global angel_instruments
        global primary_angel
        
        #angel_instruments={}
        
        broker_id=25 #angel broker id
        
        coredb = pool.connection()
        sql="select * from vw_activeaccounts where broker="+str(broker_id)
        
        
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        for index, row in df.iterrows():
            for attempt in range(2):
                try:
                    obj=SmartConnect(api_key=row['api_secret'])
                    two_fa=pyotp.TOTP(row['two_fa']).now()
                    data=obj.generateSession(row['username'],str(row['password'].decode('ascii')),two_fa)
                    
                    refreshToken=data['data']['refreshToken']
                    
                    alice_blue1=obj
                    
                    dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':alice_blue1,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':MyLock(),'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}

                    mainlogger.info('Profile :'+str(alice_blue1.getProfile(refreshToken)))
                    
                    reset_login_failed_accounts(row['ID'])
                    
                    for tries in range(2):
                        try:
                            if angel_instruments is None or len(angel_instruments)==0:
                                try:
                                    primary_angel=int(row['ID'])

                                    d = requests.get(angel_master_instruments_url).json()
                                    angel_instruments = pd.DataFrame.from_dict(d)
                                    angel_instruments['expiry'] = pd.to_datetime(angel_instruments['expiry']).apply(lambda x: x.date())
                                    angel_instruments = angel_instruments.astype({'strike': float})
                                    
                                    mainlogger.info('loaded angel instruments')
                                    send_notification(group_chat_id,'Loaded Angel instruments '+str(len(angel_instruments)))

                                except Exception as e:
                                    #handle_exception('Kite instruments load failed '+row['username'],str(e),row['tele_chat_id'],True) 
                                    send_notification(group_chat_id, emoji_warning+'Failed to load angel instruments')
                                    mainlogger.exception('Angel instruments load failed')
                        except:
                            time.sleep(5)
                        
                        else:
                            break
 
                except Exception as e:
                    try:
                        dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':None,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':MyLock(),'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}
                        handle_exception('Angel Login Failed '+row['username'],str(e),row['tele_chat_id'],True) 
                        increment_login_failed_accounts(row['ID'])
                        
                        time.sleep(1)
                    except Exception as e:
                        mainlogger.exception(str(e))
                else:
                    break;
                
        coredb.close()
    
    
    ##Place All Types of Order
    def custom_get_instrument_for_fno(self, symbol, exp, is_fut, strike, is_CE,segment='NFO'):
        mainlogger.info('Function : angel custom_get_instrument_for_fno symbol : ' + str(symbol) + " exp : " + str(exp) + " is_fut : " + str(is_fut) + " strike : " + str(strike) + " is_CE : " + str(is_CE))
        
        expiry=datetime.datetime.strptime(exp, '%d %b%y').date()
        instrument_type= 'CE' if is_CE==True else 'PE'
        #segment='NFO-OPT' if is_fut==False else 'NFO-FUT'
        if segment=='BFO':
            segment='BFO-OPT' if is_fut==False else 'BFO-FUT'
        else:
            segment='NFO-OPT' if is_fut==False else 'NFO-FUT'
            
        strike=float(strike)*100

        if 'OPT' in segment:
            instrument=angel_instruments.loc[((angel_instruments['expiry'] == expiry) & (angel_instruments['exch_seg'] == segment[:3]) & (angel_instruments['name'] == symbol) & (angel_instruments['instrumenttype'].str.startswith('OPT')) & (angel_instruments['symbol'].str.endswith(instrument_type)) & ( angel_instruments['strike'] == (strike)) )].iloc[0]
            #instrument=angel_instruments.loc[((angel_instruments['expiry'] == expiry) & (angel_instruments['name'] == symbol) & (angel_instruments['instrumenttype'].str.startswith('OPT')) & (angel_instruments['symbol'].str.endswith(instrument_type)) )].iloc[0]
            instrument = Instrument(exchange=instrument['exch_seg'],token=instrument['token'] , symbol=instrument['symbol'], name=instrument['name'], expiry=instrument['expiry'], lot_size=instrument['lotsize'])
            return instrument

        elif 'FUT' in segment:
            instrument=angel_instruments.loc[((angel_instruments['expiry'] == expiry) & (angel_instruments['exch_seg'] == segment[:3]) & (angel_instruments['name'] == symbol) & (angel_instruments['instrumenttype'].str.startswith('FUT')) )].iloc[0]
            instrument = Instrument(exchange=instrument['exch_seg'],token=instrument['token'] , symbol=instrument['symbol'], name=instrument['name'], expiry=instrument['expiry'], lot_size=instrument['lotsize'])
            return instrument
        
    
    def get_instrument_by_symbol(self, exchange, symbol):
        mainlogger.info('Function : angel get_instrument_by_symbol exchange : ' +  str(exchange) + " symbol : " + str(symbol))
        
        if exchange=='NSE':
            instrument=angel_instruments.loc[((angel_instruments['exch_seg'] == exchange) & (angel_instruments['name'] == symbol))].iloc[0]
            instrument = Instrument(exchange=instrument['exch_seg'],token=instrument['token'] , symbol=instrument['symbol'], name=instrument['name'], expiry=instrument['expiry'], lot_size=instrument['lotsize'])
            return instrument
    
        elif exchange in ('NFO', 'BFO'):
            instrument=angel_instruments.loc[((angel_instruments['exch_seg'] == exchange) & (angel_instruments['symbol'] == symbol))].iloc[0]
            instrument = Instrument(exchange=instrument['exch_seg'],token=instrument['token'] , symbol=instrument['symbol'], name=instrument['name'], expiry=instrument['expiry'], lot_size=instrument['lotsize'])
            return instrument

     
    def get_ltp(self, angel, exchange, symbol):
        mainlogger.info('Function : angel get_ltp + angel : ' + str(angel) + " exchange : " + str(exchange) + " symbol : " + str(symbol))
        
        ins=self.get_instrument_by_symbol(exchange,symbol)
        #symboltoken=broker.angel_broker.get_instrument_by_symbol(exchange,symbol)[1]
        price=angel.ltpData(exchange,ins.symbol,ins.token)['data']['ltp']

        return price
        
    def punch_order(self, acc_id, angel, alice_blue_master, symbol, direction, qty, triggerPrice, price, 
                order_type='MARKET',misFlag='NRML', amoFlag=False, exchange='NFO', 
                strategyName='NA'
               ):
    
        mainlogger.info('Function : angel punch_order')
        alice_blue_master=None
        try:
            tx_type='BUY' if (direction == 'buy' or  direction == 'b') else 'SELL'
            
            symboltoken=self.get_instrument_by_symbol(exchange,symbol)[1]

            if order_type=='MARKET':
                order_type='MARKET'
                price=0.0
                trigger_price = None
                variety='NORMAL'

            elif order_type=='SLM':
                order_type='STOPLOSS_MARKET'
                price=0.0
                trigger_price = triggerPrice
                variety='STOPLOSS'

            elif order_type=='SLL':
                order_type='STOPLOSS_LIMIT'
                price=price
                trigger_price = triggerPrice
                variety='STOPLOSS'

            elif order_type=='LIMIT':
                order_type='LIMIT'
                variety='NORMAL'

            ##MIS or NORMAL
            if exchange in ('NFO', 'BFO') and misFlag == 'NRML' :
                prod_type= 'CARRYFORWARD' 

            elif exchange in ('NFO', 'BFO') and misFlag == 'MIS':
                prod_type= 'INTRADAY' 

            else:
                prod_type= 'DELIVERY'

            variety= variety if amoFlag==False else 'AMO'
            ordertag=order_tag_prefix+'_'+str(random.randrange(1000,10000000))
                    
            for attempt in range(2):
                try:
                    if order_type=='LIMIT':
                        if price==0.0 or price is None:
                            price=angel.ltpData(exchange,symbol,symboltoken)['data']['ltp']

                            if price==None or price<0.5:
                                price=0.0
                                order_type='MARKET' 
                            elif tx_type=='SELL':
                                price=round(price*limit_price_factor_sell,1)
                            else:
                                price=round(price*limit_price_factor_buy,1)

                        trigger_price = None
                    
                    price=round(price,1)
                    trigger_price= trigger_price if trigger_price is None else round(trigger_price,1)
                        
                    order_params= {
                        "variety":variety,
                        "ordertype":order_type,
                        "producttype":prod_type,
                        "duration":"DAY",
                        "transactiontype":tx_type,
                        "price":price,
                        "triggerprice":trigger_price,
                        "quantity":qty,
                        "tradingsymbol":symbol,
                        "symboltoken":symboltoken,
                        "exchange":exchange,
                        "ordertag":ordertag
                        }

                    order_id=angel.placeOrder(order_params)
                    response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id,'average_price':price}}     

                except Exception as e:
                    mainlogger.exception("Order placement failed will be retried: "+str(e))
                    
                    try:
                        #check if Order Got accepted before retrying order to avoid duplication
                        order=broker.get_order_by_tag(acc_id,ordertag)

                        if  order==-2:
                            response={'status': 'rejected', 'message': 'Unable to verify if previous order api was successful. Please verify in broker terminal manually and retry if needed.', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id,'average_price':price}}     
                            return response
                        
                        elif  order!=-1 and order!=-2:
                            response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order['oms_order_id'] ,'average_price':order['average_price']}}     
                            return response  
                        
                        elif "Limit Price Protection" in str(e):
                            order_type="MARKET"

                        response={'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id,'average_price':price}}     
                        time.sleep(5)
                        self.check_login_token(acc_id)
                        angel=dic_accounts[acc_id]['token']
                        
                    except Exception as e:
                        mainlogger.exception("Punch order failed while retrying: "+str(e))
                        
                    
                else:
                    break;
                    
            return response

        except Exception as e:
            mainlogger.exception(str(e))

    ## Exit an order
    def exit_order(self, acc_id, angel, alice_blue_master, exchange, direction, symbol, order_id, qty, prod_type,order_type='MARKET', price=0.0):
        mainlogger.info('Function : angel exit_order')
        alice_blue_master=None
        try:
            tx_type='BUY' if (direction == 'buy' or  direction == 'b') else 'SELL'
            
            #prod_type= 'CARRYFORWARD' if exchange == 'NFO' else 'DELIVERY'
            variety= 'NORMAL'
            #order_type='MARKET'
            
            for attempt in range(2):
                try:
                    symboltoken=self.get_instrument_by_symbol(exchange,symbol)[1]
                    if order_type=='MARKET':
                        order_params= {
                            "orderid":order_id,
                            "variety":variety,
                            "ordertype":order_type,
                            "producttype":prod_type,
                            "duration":"DAY",
                            #"transactiontype":tx_type,
                            #"price":price,
                            #"triggerprice":trigger_price,
                            "quantity":qty,
                            "tradingsymbol":symbol,
                            "symboltoken":symboltoken,
                            "exchange":exchange
                            #"ordertag":ordertag
                            }
                    elif order_type=='LIMIT':
                        
                        if price==0.0 or price is None:
                            price=angel.ltpData(exchange,symbol,symboltoken)['data']['ltp']

                            if price==None or price<0.5:
                                price=0.0
                                order_type='MARKET' 
                            elif tx_type=='SELL':
                                price=round(price*limit_price_factor_sell,1)
                            else:
                                price=round(price*limit_price_factor_buy,1)

                        order_params= {
                            "orderid":order_id,
                            "variety":variety,
                            "ordertype":order_type,
                            "producttype":prod_type,
                            "duration":"DAY",
                            #"transactiontype":tx_type,
                            "price":price,
                            #"triggerprice":trigger_price,
                            "quantity":qty,
                            "tradingsymbol":symbol,
                            "symboltoken":symboltoken,
                            "exchange":exchange
                            #"ordertag":ordertag
                            }

                    order_id=angel.modifyOrder(order_params)['data']['orderid']
                    response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id}}     

                except Exception as e:
                    mainlogger.exception("Order exit failed: "+str(e))
                    response={'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': order_id}}  
                    time.sleep(5)
                    self.check_login_token(acc_id)
                    angel=dic_accounts[acc_id]['token']
                    
                else:
                    break;
                    
            return response
        
        except Exception as e:
            mainlogger.exception(str(e))
    
    ## Modify an order   - converted to angel
    def modify_order(self, acc_id, angel, alice_blue_master, exchange, direction, symbol, order_id, qty, prod_type, order_type, price, triggerPrice):
        mainlogger.info('Function : angel modify_order')
         
        try: 
            #prod_type= 'CARRYFORWARD' if exchange == 'NFO' else 'DELIVERY'
            
            if order_type=='MARKET':
                order_type='MARKET'
                price=0.0
                trigger_price = None
                variety='NORMAL'

            elif order_type=='SLM':
                order_type='STOPLOSS_MARKET'
                price=0.0
                trigger_price = triggerPrice
                variety='STOPLOSS'

            elif order_type=='SLL':
                order_type='STOPLOSS_LIMIT'
                price=price
                trigger_price = triggerPrice
                variety='STOPLOSS'

            elif order_type=='LIMIT':
                order_type='LIMIT'
                price=price
                trigger_price = None
                variety='NORMAL'

            for attempt in range(2):
                try:
                    symboltoken=self.get_instrument_by_symbol(exchange,symbol)[1]
                    order_params= {
                        "orderid":order_id,
                        "variety":variety,
                        "ordertype":order_type,
                        "producttype":prod_type,
                        "duration":"DAY",
                        #"transactiontype":tx_type,
                        "price":price,
                        "triggerprice":trigger_price,
                        "quantity":qty,
                        "tradingsymbol":symbol,
                        "symboltoken":symboltoken,
                        "exchange":exchange
                        #"ordertag":ordertag
                        }

                    order_id=angel.modifyOrder(order_params)['data']['orderid']
                    response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id}}     

                except Exception as e:
                    mainlogger.exception("Order modify failed: "+str(e))
                    response={'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': order_id}} 
                    time.sleep(5)
                    self.check_login_token(acc_id)
                    angel=dic_accounts[acc_id]['token']
                else:
                    break
                    
            return response
    

        except Exception as e:
            mainlogger.exception(str(e))
     
    #converted to angel only one api per second
    def check_if_position_open(self, angel, exchange, symbol, qty, direction, acc_id):
        mainlogger.info('Function : Angel check_if_position_open')
        
        dic_accounts[acc_id]['lock'].acquire(thread_lock_timeout)
        try:
            trade_book=angel.position()
            time.sleep(1) ## only one api call/second
            if dic_accounts[acc_id]['lock'].locked()==True:
                dic_accounts[acc_id]['lock'].release()
             
            if trade_book is None or len(trade_book)==0:
                mainlogger.exception("Retruned Empty Position Book")
                return True,10000
                
            for t in trade_book['data']:
                #if int(t['netqty'])!=0 and direction.upper()=='BUY' and t['exchange']==exchange and (t['tradingsymbol'])==symbol and (int(t['sellqty'])>=int(qty) or int(t['cfsellqty'])>=int(qty)) :
                if int(t['netqty'])!=0 and direction.upper()=='BUY' and t['exchange']==exchange and (t['tradingsymbol'])==symbol and (int(t['sellqty'])>0 or int(t['cfsellqty'])>0):
         
                    if abs(int(t['sellqty'])) > abs(int(t['cfsellqty'])):
                        return True, abs(int(t['sellqty']))
                    else:
                        return True, abs(int(t['cfsellqty']))                   

                #elif int(t['netqty'])!=0 and  direction.upper()=='SELL' and t['exchange']==exchange and (t['tradingsymbol'])==symbol and (int(t['buyqty'])>=int(qty) or int(t['cfbuyqty'])>=int(qty)):
                elif int(t['netqty'])!=0 and  direction.upper()=='SELL' and t['exchange']==exchange and (t['tradingsymbol'])==symbol and (int(t['buyqty'])>0 or int(t['cfbuyqty'])>0):
                   
                    if abs(int(t['buyqty'])) > abs(int(t['cfbuyqty'])):
                        return True, abs(int(t['buyqty']))
                    else:
                        return True, abs(int(t['cfbuyqty'])) 
            
            return False,0
        
        except Exception as e:
            mainlogger.exception(str(e))
            
            if dic_accounts[acc_id]['lock'].locked()==True:
                dic_accounts[acc_id]['lock'].release()
            return True,10000
    
    #Angel 1 per 1s API problem
    def get_order_book(self,acc_id):
        mainlogger.info('Function : angel broker get_order_book')
        dic_accounts[acc_id]['lock'].acquire(thread_lock_timeout)
        
        orderbook={}
        
        max_tries=4
        for attempt in range(max_tries):
            try:
                orderbook= dic_accounts[acc_id]['token'].orderBook()
                if type(orderbook['data'])==type(None):
                    orderbook['data']={}
                time.sleep(round(random.uniform(1, 3),2)) ## only one api call/second
                
            except Exception as e:
                if attempt>=max_tries-1:
                    mainlogger.exception("angel orderBook retry attempt:"+str(e))
                time.sleep(round(random.uniform(1, 3),2))
            else:
                break;
        
        if dic_accounts[acc_id]['lock'].locked()==True:
            dic_accounts[acc_id]['lock'].release()
        return orderbook
    
    
    def get_order_history_old(self,acc_id,order_id):
        mainlogger.info('Function : AngelBroker get_order_history')
        if order_id==zerodha_failed_order_hardcoded_id:
            tmp={'order_status': 'rejected', 'average_price':0, 'filled_quantity':0, 'message': 'NA', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     
            return tmp
        
        max_tries=2
        for attempt in range(max_tries):
            try:
                tmp=next((tmp for tmp in self.get_order_book(acc_id)['data'] if tmp['orderid'] == str(order_id)), None)
                if tmp is not None:
                    #tmp=dic_accounts[acc_id]['token'].order_history(order_id)[-1]
                    tmp['oms_order_id']=tmp['orderid']
                    tmp['order_status']=tmp['orderstatus']
                    tmp['price_to_fill']=tmp['price']
                    tmp['average_price']=tmp['averageprice']
                    tmp['filled_quantity']=tmp['quantity']
                    tmp['trigger_price']=tmp['triggerprice']
                    tmp['transaction_type']=tmp['transactiontype']
                    tmp['order_type']=tmp['ordertype']
                    tmp['product']=tmp['producttype']
                    tmp['rejection_reason']=tmp['text']
                    tmp['order_tag']=tmp['ordertag']
                else:
                    raise Exception("order not found/api error")

            except Exception as te:
                if attempt>=max_tries-1:
                    mainlogger.exception('Function : get_order_history '+str(te))
                    
                time.sleep(2)
                tmp={'order_status': 'not_found', 'average_price':0, 'filled_quantity':0, 'message': 'order not found/api error', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}      
            else:
                break
        return tmp
    
    def get_order_history(self,acc_id,order_id):
        mainlogger.info('Function : AngelBroker get_order_history')
        if order_id==zerodha_failed_order_hardcoded_id:
            tmp={'order_status': 'rejected', 'average_price':0, 'filled_quantity':0, 'message': 'NA', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     
            return tmp
        
        max_tries=2
        for attempt in range(max_tries):
            try:
                #tmp=next((tmp for tmp in self.get_order_book(acc_id)['data'] if tmp['orderid'] == str(order_id)), None)
                tmp=dic_accounts[acc_id]['token'].individual_order_details(order_id)['data']
                if tmp is not None:
                    #tmp=dic_accounts[acc_id]['token'].order_history(order_id)[-1]
                    tmp['oms_order_id']=tmp['orderid']+'#'+tmp['uniqueorderid']
                    tmp['order_status']=tmp['orderstatus']
                    tmp['price_to_fill']=tmp['price']
                    tmp['average_price']=tmp['averageprice']
                    tmp['filled_quantity']=tmp['quantity']
                    tmp['trigger_price']=tmp['triggerprice']
                    tmp['transaction_type']=tmp['transactiontype']
                    tmp['order_type']=tmp['ordertype']
                    tmp['product']=tmp['producttype']
                    tmp['rejection_reason']=tmp['text']
                    tmp['order_tag']=tmp['ordertag']
                else:
                    raise Exception("order not found/api error")

            except Exception as te:
                if attempt>=max_tries-1:
                    mainlogger.exception('Function : get_order_history '+str(te))
                    
                time.sleep(2)
                tmp={'order_status': 'not_found', 'average_price':0, 'filled_quantity':0, 'message': 'order not found/api error', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}      
            else:
                break
        return tmp
    
    def get_all_order_history(self,acc_id):
        mainlogger.info('Function : AngelBroker get_all_order_history')
        
        orders=[]

        for tmp in Broker.angel_broker.get_order_book(acc_id)['data']:
            try:
                tmp['oms_order_id']=tmp['orderid']+'#'+tmp['uniqueorderid']
                tmp['order_status']=tmp['orderstatus']
                tmp['exchange_time']=datetime.datetime.strptime(tmp['exchtime'], '%d-%b-%Y %H:%M:%S').timestamp()
                tmp['rejection_reason']=tmp['text']
                tmp['order_tag']=tmp['ordertag']
                tmp['price_to_fill']=tmp['price']
                tmp['average_price']=tmp['averageprice']
                tmp['filled_quantity']=tmp['quantity']
                tmp['trigger_price']=tmp['triggerprice']
                tmp['transaction_type']=tmp['transactiontype']
                tmp['order_type']=tmp['ordertype']
                tmp['product']=tmp['producttype']

                orders.append(tmp)
            except:
                pass

        return orders
    
    def tradingAccount_PNLSummary(self, acc_id): 
        mainlogger.info('Function : Angel tradingAccount_PNLSummary')
    
        
        
        positions= dic_accounts[acc_id]['token'].position()['data']

        totalRealizedPNL=0
        totalUnRealizedPNL=0

        maxrealizedLossPosition=0
        maxrealizedPoriftPosition=0

        maxUnrealizedLossPosition=0
        maxUnrealizedProfitPosition=0


        if positions!=None:
            for p in positions:

                ##print(p)
                totalRealizedPNL=totalRealizedPNL+float(p['realised'])
                totalUnRealizedPNL=totalUnRealizedPNL+float(p['unrealised'])

                if float(p['realised'])>maxrealizedPoriftPosition:
                    maxrealizedPoriftPosition=float(p['realised'])

                if float(p['realised'])<0 and abs(float(p['realised']))>maxrealizedLossPosition:
                    maxrealizedLossPosition=abs(float(p['realised']))


                if float(p['unrealised'])>maxUnrealizedProfitPosition:
                    maxUnrealizedProfitPosition=float(p['unrealised'])

                if float(p['unrealised'])<0 and abs(float(p['unrealised']))>maxUnrealizedLossPosition:
                    maxUnrealizedLossPosition=abs(float(p['unrealised']))



        pnlSummary={"totalRealizedPNL":round(totalRealizedPNL,1),"totalUnRealizedPNL":round(totalUnRealizedPNL,1),
                    "maxrealizedPoriftPosition":round(maxrealizedPoriftPosition,1),"maxrealizedLossPosition":round(maxrealizedLossPosition,1)*-1,
                    "maxUnrealizedProfitPosition":round(maxUnrealizedProfitPosition,1),"maxUnrealizedLossPosition":round(maxUnrealizedLossPosition,1)*-1
                   }

        return pnlSummary
        


# # Kite Class

# %%


class KiteBroker:

    feed_status=None
    kws=None #dic_accounts[primary_kite]['token'].kws()
    ws_subscribe_list=[]
    
    
    ##check if token is still valid
    def check_login_token(self,acc_id): 
        mainlogger.info('Function : KiteBroker check_login_token')
        
        #print('acquiring lock')
        mainlogger.info(str(dic_accounts[acc_id]))
        dic_accounts[acc_id]['lock'].acquire(thread_lock_timeout)
        
        if (datetime.datetime.now()-dic_accounts[acc_id]['token_refresh']).total_seconds()<5:
            if dic_accounts[acc_id]['lock'].locked()==True:
                dic_accounts[acc_id]['lock'].release()
            #print('release lock')
            mainlogger.info(str(dic_accounts[acc_id]))
            return 1
        
        try:
            if dic_accounts[acc_id]['token'] is None:
                self.account_single_login(acc_id)
                dic_accounts[acc_id]['token_refresh']=datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked()==True:
                    #print('release lock')
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                
            else:
                dic_accounts[acc_id]['token'].ltp('NSE:NIFTY 50')
                dic_accounts[acc_id]['token_refresh']=datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked()==True:
                    #print('release lock')
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                    
        except kiteconnect.exceptions.KiteException as te:
            if(str(te)=='Incorrect `api_key` or `access_token`.'):
                try:
                    self.account_single_login(acc_id)
                    dic_accounts[acc_id]['token_refresh']=datetime.datetime.now()
                    if dic_accounts[acc_id]['lock'].locked()==True:
                        #print('release lock')
                        dic_accounts[acc_id]['lock'].release()
                    mainlogger.info(str(dic_accounts[acc_id]))
                    
                except Exception as e:
                    mainlogger.exception(str(e))
                    if dic_accounts[acc_id]['lock'].locked()==True:
                        #print('release lock')
                        dic_accounts[acc_id]['lock'].release()
                    mainlogger.info(str(dic_accounts[acc_id]))
                #self.account_single_login(acc_id)
    
    def handle_two_fa(self,row):
        mainlogger.info('Function : KiteBroker handle_two_fa')
        if (row['totp_flag']==1):
            return pyotp.TOTP(row['two_fa']).now()
        return row['two_fa']
    
    ## Official API keys login via Selenium
    
    def kite_selenium_official_login(self, user_id,password,twofa,api_key,api_secret,totp_flag=0):
        mainlogger.info('Function : kite_selenium_official_login')
        
        for attempt in range(2):
            try:        
                root='https://kite.zerodha.com'

                routes={}
                routes.update({
                            'connect.login': '/connect/login',
                            'api.login': '/api/login',
                            'api.twofa': '/api/twofa',
                            'api.misdata': '/margins/equity'
                        })

                
                headers = {
                    'x-kite-version': '3',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36'
                }

            
                reqsession = requests.Session()

                r = reqsession.get('https://kite.zerodha.com/connect/login?api_key='+api_key)
                
                f = furl(r.url) 
                sess_id=dict(f.args)['sess_id']

                r = reqsession.post(root + routes['api.login'], data={
                    'user_id': user_id,
                    'password': password
                })

                #pin=pyotp.TOTP(twofa).now()
                pin=twofa
                
                r = reqsession.post(root + routes['api.twofa'], data={
                    'request_id': r.json()['data']['request_id'],
                    'twofa_value': pin,
                    'user_id': r.json()['data']['user_id'],
                    'twofa_type':'totp'
                })

                x=r.request.headers['Cookie']
                kf_session=re.sub(r';.*','', re.sub(r'.*kf_session=','', x)).split(' ')[0]

                public_token = r.cookies.get('public_token')
                enctoken = r.cookies.get('enctoken')
                user_id = r.cookies.get('user_id')

                headers = CaseInsensitiveDict()
                headers["Cookie"] = "public_token="+public_token+"; kf_session="+kf_session+"; user_id="+user_id+"; enctoken="+enctoken

                r = reqsession.get('https://kite.zerodha.com/connect/finish?api_key='+api_key+'&sess_id='+sess_id,headers=headers,verify=False)

                request_token=r.url.rsplit('request_token=', 1)[-1]
                request_token=request_token.split('&', 1)[0]


                kite = KiteConnect(api_key=api_key)
                data = kite.generate_session(request_token, api_secret=api_secret)
                kite.set_access_token(data["access_token"])
                kite.profile()

                return kite

            except Exception as e:
                #print(str(e))
                mainlogger.exception(str(e))
                time.sleep(3)
                pass
            else:
                break;
    
    ##Single LOGIN to Accounts
    def account_single_login(self,acc_id):
        mainlogger.info('Function : KiteBroker single login')
        
        global pool
        global dic_accounts
        global kite_instruments
        #kite_instruments={}
        
        #broker_id=26 #alice broker id
        
        coredb = pool.connection()
        sql="select * from vw_activeaccounts where ID="+str(acc_id)
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        max_tries=2
        for index, row in df.iterrows():
            for attempt in range(max_tries):
                try:
                    kite = KiteExt()
                    
                    two_fa=self.handle_two_fa(row)
                    
                    if row['api_secret'] is None or row['api_secret']=='':
                        kite.login_with_credentials(userid=row['username'], password=row['password'], pin=two_fa)
                        
                    else:
                        kite=self.kite_selenium_official_login(row['username'],row['password'],two_fa,row['app_id'],row['api_secret'],row['totp_flag'])
                        
                    alice_blue1=kite
                    
                    #access_token = AliceBlue.login_and_get_access_token(username=row['username'], password=row['password'], twoFA=row['two_fa'], api_secret=row['api_secret'],app_id=row['app_id']) #App ID
                    #alice_blue1 = AliceBlue(username=row['username'], password=row['password'], access_token=access_token, master_contracts_to_download=master_contracts)
                    dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':alice_blue1,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':dic_accounts[row['ID']]['lock'],'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}

                    mainlogger.info('Profile :'+str(alice_blue1.profile()))
                    reset_login_failed_accounts(row['ID'])
                    
                    for tries in range(2):
                        try:
                            #print(kite_instruments,len(kite_instruments))
                            if kite_instruments is None or len(kite_instruments)==0:
                                try:
                                    
                                    instruments1 = kite.instruments(exchange="NSE")
                                    instruments2 = kite.instruments(exchange="NFO")
                                    instruments3 = kite.instruments(exchange="BSE")
                                    instruments4 = kite.instruments(exchange="BFO")
                                    
                                    nse_instr = pd.DataFrame(instruments1, index=None)
                                    nfo_instr = pd.DataFrame(instruments2, index=None)
                                    bse_instr = pd.DataFrame(instruments3, index=None)
                                    bfo_instr = pd.DataFrame(instruments4, index=None)
                                    
                                    frames = [nse_instr, nfo_instr, bse_instr, bfo_instr]

                                    kite_instruments = pd.concat(frames)
                                    
                                    kite_instruments=broker.hash_row(kite_instruments, ['expiry','name','segment','instrument_type','strike'], 'option_hash')
                                    kite_instruments=broker.hash_row(kite_instruments, ['expiry','name','segment'], 'future_hash')
                                    kite_instruments=broker.hash_row(kite_instruments, ['exchange','tradingsymbol'], 'symbol_hash')

                                    #kite_instruments['hash'] = kite_instruments.apply(lambda row: hash_row(row[['expiry', 'name', 'segment', 'instrument_type', 'strike']]), axis=1)

                                    mainlogger.info('loaded kite instruments:'+str(kite_instruments.head()))
                                    send_notification(group_chat_id,'Loaded kite instruments '+str(len(kite_instruments)))

                                except Exception as e:
                                    #handle_exception('Kite instruments load failed '+row['username'],str(e),row['tele_chat_id'],True) 
                                    send_notification(group_chat_id, emoji_warning+'Failed to load kite instruments')
                                    mainlogger.exception('kite instruments load failed')
                                    
                        except Exception as e:
                            mainlogger.exception('kite instruments load failed')
                            time.sleep(5)
                            pass
                        else:
                            break;
                            
                except Exception as e:
                    try:
                        dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':None,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':dic_accounts[row['ID']]['lock'],'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}
                        if attempt>=max_tries-1:
                            handle_exception('Kite Login Failed '+row['username'],str(e),row['tele_chat_id'],True) 
                        time.sleep(1)
                    except Exception as e:
                        mainlogger.exception(str(e))
                else:
                    break;
                
        coredb.close()
    
    ##LOGIN to Accounts
    def account_login(self):
        mainlogger.info('Function : KiteBroker login')
        
        global pool
        global dic_accounts
        global kite_instruments
        global primary_kite
        global primary_kite_historical_data
        
        
        broker_id=26 #alice broker id
        
        coredb = pool.connection()
        sql="select * from vw_activeaccounts where broker="+str(broker_id)
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        max_tries=2
        for index, row in df.iterrows():
            for attempt in range(max_tries):
                try:
                    kite = KiteExt()
                    
                    two_fa=self.handle_two_fa(row)
                    
                    if row['api_secret'] is None or row['api_secret']=='':
                        kite.login_with_credentials(userid=row['username'], password=row['password'], pin=two_fa)
                        
                    else:
                        kite=self.kite_selenium_official_login(row['username'],row['password'],two_fa,row['app_id'],row['api_secret'],row['totp_flag'])
                        
                    alice_blue1=kite
                    
                    #access_token = AliceBlue.login_and_get_access_token(username=row['username'], password=row['password'], twoFA=row['two_fa'], api_secret=row['api_secret'],app_id=row['app_id']) #App ID
                    #alice_blue1 = AliceBlue(username=row['username'], password=row['password'], access_token=access_token, master_contracts_to_download=master_contracts)
                    dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':alice_blue1,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':MyLock(),'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}

                    mainlogger.info('Profile :'+str(alice_blue1.profile()))
                    reset_login_failed_accounts(row['ID'])
                    
                    if (kite_instruments is None or len(kite_instruments)==0) and int(row['ID']) in (primary_kite, primary_kite2) and (keepsl_kite_api_key==1 or row['api_secret'] is None or row['api_secret']==''):
                        mainlogger.info('loading kite instruments')
                        for tries in range(3):
                            try:
                                primary_kite=int(row['ID'])
                                #primary_kite_historical_data=int(row['ID'])
                                
                                instruments1 = kite.instruments(exchange="NSE")
                                instruments2 = kite.instruments(exchange="NFO")
                                instruments3 = kite.instruments(exchange="BSE")
                                instruments4 = kite.instruments(exchange="BFO")

                                nse_instr = pd.DataFrame(instruments1, index=None)
                                nfo_instr = pd.DataFrame(instruments2, index=None)
                                bse_instr = pd.DataFrame(instruments3, index=None)
                                bfo_instr = pd.DataFrame(instruments4, index=None)

                                frames = [nse_instr, nfo_instr, bse_instr, bfo_instr]

                                kite_instruments = pd.concat(frames)
                                    
                                kite_instruments=broker.hash_row(kite_instruments, ['expiry','name','segment','instrument_type','strike'], 'option_hash')
                                kite_instruments=broker.hash_row(kite_instruments, ['expiry','name','segment'], 'future_hash')
                                kite_instruments=broker.hash_row(kite_instruments, ['exchange','tradingsymbol'], 'symbol_hash')

                                #kite_instruments['hash'] = kite_instruments.apply(lambda row: hash_row(row[['expiry', 'name', 'segment', 'instrument_type', 'strike']]), axis=1)

                                mainlogger.info('loaded kite instruments:'+str(kite_instruments.head()))
                                send_notification(group_chat_id,'Loaded kite instruments'+str(len(kite_instruments)))

                            except:
                                mainlogger.exception('kite instruments load failed')
                                send_notification(group_chat_id,emoji_warning+'Failed to load kite instruments')
                                time.sleep(5)

                            else:
                                break

                            
                except Exception as e:
                    try:
                        dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':None,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':MyLock(),'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}
                        if attempt>=max_tries-1:
                            handle_exception('Kite Login Failed '+row['username'],str(e),row['tele_chat_id'],True) 
                        increment_login_failed_accounts(row['ID'])
                        time.sleep(1)
                    except Exception as e:
                        mainlogger.exception(str(e))
                else:
                    break;
                
        coredb.close()
    
    def custom_get_instrument_for_fno(self, symbol, exp, is_fut, strike, is_CE,segment='NFO'):
        mainlogger.info('Function : kite custom_get_instrument_for_fno')

        try:
            expiry=datetime.datetime.strptime(exp, '%d %b%y').date()
            instrument_type= 'CE' if is_CE==True else 'PE'
            
            if segment=='BFO':
                segment='BFO-OPT' if is_fut==False else 'BFO-FUT'
            else:
                segment='NFO-OPT' if is_fut==False else 'NFO-FUT'
                
            if 'OPT' in segment:
                instrument=kite_instruments.loc[((kite_instruments['expiry'] == expiry) & (kite_instruments['name'] == symbol) & (kite_instruments['segment'] == segment) & (kite_instruments['instrument_type'] == instrument_type) & ( kite_instruments['strike'] == float(strike)) )].iloc[0]
                #hash_value=hash(tuple([expiry,symbol,segment,instrument_type,float(strike)]))
                #instrument = kite_instruments[kite_instruments['option_hash'] == hash_value].iloc[0]
            
                instrument = Instrument(exchange=instrument['exchange'],token=int(instrument['instrument_token']) , symbol=instrument['tradingsymbol'], name=instrument['name'], expiry=instrument['expiry'], lot_size=instrument['lot_size'])
                return instrument

            elif 'FUT' in segment:
                instrument=kite_instruments.loc[((kite_instruments['expiry'] == expiry) & (kite_instruments['name'] == symbol) & (kite_instruments['segment'] == segment) )].iloc[0]
                #hash_value=hash(tuple([expiry,symbol,segment]))
                #instrument = kite_instruments[kite_instruments['future_hash'] == hash_value].iloc[0]
            
                instrument = Instrument(exchange=instrument['exchange'],token=int(instrument['instrument_token']) , symbol=instrument['tradingsymbol'], name=instrument['name'], expiry=instrument['expiry'], lot_size=instrument['lot_size'])
                return instrument
        
        except Exception as e:
            mainlogger.exception(str(e))
            return None
    
    def get_instrument_by_symbol(self, exchange, symbol):
        mainlogger.info('Function : kite get_instrument_by_symbol exchange : ' + str(exchange) + ', symbol : ' + str(symbol))
        
        try:
            if exchange!='NA':
                instrument=kite_instruments.loc[((kite_instruments['exchange'] == exchange) & (kite_instruments['tradingsymbol'] == symbol))].iloc[0]
            else:
                instrument=kite_instruments.loc[((kite_instruments['tradingsymbol'] == symbol))].iloc[0]
        
            #hash_value=hash(tuple([exchange, symbol]))
            #instrument = kite_instruments[kite_instruments['symbol_hash'] == hash_value].iloc[0]
            
            instrument = Instrument(
                exchange=instrument['exchange'],
                token=int(instrument['instrument_token']),
                symbol=instrument['tradingsymbol'],
                name=instrument['name'],
                expiry=instrument['expiry'],
                lot_size=instrument['lot_size'],
                base_symbol=instrument['name'],  # Assuming base_symbol matches name
                strike=instrument['strike'],
                call_put=instrument['instrument_type']
            )
            
            return instrument
        
        except Exception as e:
            mainlogger.exception(str(e))
            return None
    
    def get_instrument_by_token(self, symbolToken):
        #mainlogger.info('Function : kite get_instrument_by_token')
        
        try:
            instrument=kite_instruments.loc[((kite_instruments['instrument_token'] == symbolToken))].iloc[0]
            instrument = Instrument(exchange=instrument['exchange'],token=int(instrument['instrument_token']) , symbol=instrument['tradingsymbol'], name=instrument['name'], expiry=instrument['expiry'], lot_size=instrument['lot_size']
            , base_symbol=instrument['name'], strike=instrument['strike'], call_put=instrument['instrument_type'])
            return instrument
    
        except Exception as e:
            mainlogger.exception(str(e))
            return None
    
    def get_all_nfo_instruments_by_symbol(self, exchange='NFO', symbol='NIFTY'):
        mainlogger.info('Function : kite get_all_nfo_instruments_by_symbol')

        instrument_type= 'CE'
        segment='NFO-OPT' if exchange=='NFO' else 'BFO-OPT'
        #print(exchange,symbol,segment,instrument_type)
        try:
            instruments=(kite_instruments.loc[((kite_instruments['name'] == symbol) & (kite_instruments['segment'] == segment) & (kite_instruments['instrument_type'] == instrument_type) )])#.iloc[0]
            #print(instruments)
            return instruments
        
        except Exception as e:
            mainlogger.exception(str(e))
            return None

    ## Kite Webservice
    def on_ticks(self, ws, ticks):
        # Callback to receive ticks.
        #mainlogger.debug("Ticks: {}".format(ticks))
        self.event_handler_quote_update(ticks)

    def on_connect(self, ws, response):
        # Callback on successful connect.
        # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
        #ws.subscribe([738561, 5633])
        ##print("on_connect",KiteBroker.ws_subscribe_list)
        ws.subscribe(KiteBroker.ws_subscribe_list)
        
        mainlogger.info('All Websocket Subscriptions: '+str(KiteBroker.kws.subscribed_tokens))
        
        # Set RELIANCE to tick in `full` mode.
        #ws.set_mode(ws.MODE_FULL, [738561])

    def on_close(self, ws, code, reason):
        # On connection close stop the event loop.
        # Reconnection will not happen after executing `ws.stop()`
        #ws.stop()
        KiteBroker.kws.resubscribe()

    ## Initiate Websocket Connection
    
    def handle_event_message(self,messages):
        global instruments_by_symbol
        
        tmp_instruments_by_symbol=copy.deepcopy(instruments_by_symbol)
        
        mainlogger.info('Function: handle_event_message number of messages: '+str(len(messages)))
        for message in messages:
            ins=self.get_instrument_by_token(message['instrument_token'])
            scrip = ins[2].upper()
            
            try:
                tmp_instruments_by_symbol[scrip]['ltp']=message['last_price']
                tmp_instruments_by_symbol[scrip]['open']=-1 #message['open']
                tmp_instruments_by_symbol[scrip]['close']=-1 #message['close']

                if tmp_instruments_by_symbol[scrip]['high'] is None or tmp_instruments_by_symbol[scrip]['high']<message['last_price']:
                    tmp_instruments_by_symbol[scrip]['high']=message['last_price'] #message['high']

                if tmp_instruments_by_symbol[scrip]['low'] is None or tmp_instruments_by_symbol[scrip]['low']>message['last_price']:
                    tmp_instruments_by_symbol[scrip]['low']=message['last_price'] #message['high']
            except Exception as e:
                mainlogger.exception('event_handler_quote_update:'+str(e))
            
        instruments_by_symbol.update(tmp_instruments_by_symbol)
            
    def event_handler_quote_update(self,messages):
        KiteBroker.feed_status=datetime.datetime.now(ist)
        self.handle_event_message(messages)
        

    def feed_check(self):
        while True:
            if datetime.datetime.now(ist)-KiteBroker.feed_status > datetime.timedelta(seconds=10):
                if datetime.datetime.now(ist).time()<datetime.time(hour=15,minute=15,second=59):
                    mainlogger.info('delay {}'.format(datetime.datetime.now(ist)-KiteBroker.feed_status,datetime.datetime.now(ist)))
                    
                    KiteBroker.kws.resubscribe()
            time.sleep(30)

    def initiate_connection(self):
        mainlogger.info('Function: start_websocket')
        
        global kws
        try:
            KiteBroker.feed_status=datetime.datetime.now(ist)
            
            if keepsl_kite_api_key==0:
                dic_accounts[primary_kite]['token'].set_headers(dic_accounts[primary_kite]['token'].enctoken)
                KiteBroker.kws=dic_accounts[primary_kite]['token'].kws()
            else:
                KiteBroker.kws = KiteTicker(dic_accounts[primary_kite]['token'].api_key, dic_accounts[primary_kite]['token'].access_token)
                
            KiteBroker.kws.on_ticks = self.on_ticks
            KiteBroker.kws.on_connect = self.on_connect
            KiteBroker.kws.on_close = self.on_close
            
            KiteBroker.kws.connect(threaded=True, disable_ssl_verification=False)
          
        except Exception as e:
            mainlogger.exception(str(e))
            

    ## Incremental Subscribe to Websocket for LTP
    def subscribe_to_instrument(self, exchange, symbol):
        mainlogger.info('Function: subscribe_to_instrument')
        global global_instruments_1min
        global instruments_by_symbol
        
        ins=self.get_instrument_by_symbol(exchange, symbol) 
        
        if ins[1] in KiteBroker.ws_subscribe_list:
            return 1
    
        instruments_by_symbol[symbol]={'segment':exchange,'ltp':None,'open':None,'high':None,'low':None,'close':None}
        
        KiteBroker.kws.subscribe([ins[1]])
        
        KiteBroker.ws_subscribe_list.append(ins[1])
        q_var_global_instruments_1min.put(ins)
        #global_instruments_1min.append(ins)
        
        mainlogger.info('All Websocket Subscriptions: '+str(KiteBroker.kws.subscribed_tokens))
        
    ## Subscribe to Websocket for LTP
    def subscribe_ltp(self,instruments_list,exchange='NSE'):
        mainlogger.info('Function : subscribe_ltp')
        global global_instruments_1min
        global instruments_by_symbol
        
        for x in instruments_list:
            try:
                ins=self.get_instrument_by_symbol(exchange, x) 
                
                instruments_by_symbol[x]={'segment':'NSE','ltp':None,'open':None,'high':None,'low':None,'close':None}
                #AliceBroker.ws_subscribe_list.append(ins)
                KiteBroker.ws_subscribe_list.append(ins[1])
                q_var_global_instruments_1min.put(ins)
                #global_instruments_1min.append(ins)
            
            except Exception as e:
                mainlogger.info('Exception on Websocket Subscription: '+str([x, e]))
        
        self.initiate_connection()

    def subscribe_ltp_nfo(self,instruments_list,exchange='NFO'):
        mainlogger.info('Function : subscribe_ltp_nfo')
        global global_instruments_1min
        global instruments_by_symbol
        
        for x in instruments_list:
            try:
                ins=self.get_instrument_by_symbol(exchange, x) 
                instruments_by_symbol[x]={'segment':exchange,'ltp':None, 'open':None, 'high':None, 'low':None, 'close':None}
                
                if ins!=None:
                    KiteBroker.kws.subscribe([ins[1]])
                    KiteBroker.ws_subscribe_list.append(ins[1])
                    q_var_global_instruments_1min.put(ins)
                    #global_instruments_1min.append(ins)

            except Exception as e:
                mainlogger.info('Exception on Websocket Subscription: '+str([x, e]))
        
        mainlogger.info('All Websocket Subscriptions: '+str(KiteBroker.kws.subscribed_tokens))
    
    def get_circuit_limits(self, kite, exchange, symbol):
        mainlogger.info('Function : get_circuit_limits')
        try:
            return self.get_ltp(kite, exchange, symbol, True)
            
        except Exception as e:
                mainlogger.info('Exception get_circuit_limits: '+str(e))
                return {'higher_circuit_limit':1000000,'lower_circuit_limit':0}
    
    
    def get_weekly_expiry(self,scrip='NIFTY',exchange='NFO'):
        ##print("get_weekly_expiry")
        mainlogger.info('Function : get_weekly_expiry : ' + str(scrip) + ", " + str(exchange))

        today = datetime.datetime.now(ist).date()
        
        all_sensex_scrips = self.get_all_nfo_instruments_by_symbol(exchange,scrip)
        
        expiry=set(all_sensex_scrips.expiry)

        expiry=list(expiry)
        expiry.sort()

        today = datetime.datetime.now(ist).date()+datetime.timedelta(days=0)

        i=0
        while True:
            if(expiry[i]>=today):
                exp=str(expiry[i].day)+' '+ str(expiry[i].strftime('%b').upper())+str(expiry[i].year)[2:4]
                break

            i=i+1

        return exp

    def get_monthly_expiry(self,scrip='NIFTY',exchange='NFO'):
        ##print("get_weekly_expiry")
        mainlogger.info('Function : get_monthly_expiry')

        today = datetime.datetime.now(ist).date()+datetime.timedelta(days=0)

        all_sensex_scrips = self.get_all_nfo_instruments_by_symbol(exchange,scrip) 
        
        expiry=set(all_sensex_scrips.expiry)

        expiry=list(expiry)
        expiry.sort()

        i=0
        while True:
            if(expiry[i]>=today and expiry[i].month!=expiry[i+1].month ):
                exp=str(expiry[i].day)+' '+ str(expiry[i].strftime('%b').upper())+str(expiry[i].year)[2:4]
                break

            i=i+1

        return exp


    def is_same_week(self,date_str):
        # Convert the example date string to a datetime object
        example_date = datetime.datetime.strptime(date_str, '%d %b%y')  # Format: 24 DEC24
        
        # Get the current date
        current_date = datetime.datetime.now()
        
        # Compare the ISO week numbers and years
        if current_date.isocalendar()[:2] == example_date.isocalendar()[:2]:
            return 1  # Same week
        else:
            return 0  # Different week


    def get_monthly_expiry_plus(self,scrip='NIFTY',exchange='NFO'):
        mainlogger.info('Function : get_monthly_expiry_plus')
        
        try:
            exp_str=self.get_monthly_expiry(scrip,exchange)
            index=self.is_same_week(exp_str)
            return self.get_future_monthly_expiry(scrip,exchange)[index]
        
        except Exception as e:
                mainlogger.exception(str(e))
                return None
        '''
        if self.get_weekly_expiry(scrip,exchange)!=self.get_monthly_expiry(scrip,exchange):
            return self.get_monthly_expiry(scrip,exchange)

        else:
            try:
                exp_future_monthly=self.get_future_monthly_expiry(scrip,exchange)
                return exp_future_monthly[1]
            except Exception as e:
                mainlogger.exception(str(e))
                return None
        '''
    def get_future_weekly_expiry(self,scrip='NIFTY',exchange='NFO'):
        ##print("get_weekly_expiry")
        mainlogger.info('Function : get_future_weekly_expiry')

        today = datetime.datetime.now(ist).date()
        exp_future_weekly=[]

        all_sensex_scrips = self.get_all_nfo_instruments_by_symbol(exchange,scrip) 
        
        expiry=set(all_sensex_scrips.expiry)

        expiry=list(expiry)
        expiry.sort()

        today = datetime.datetime.now(ist).date()+datetime.timedelta(days=0)

        ##print(expiry)
        #i=0
        for e in expiry:
            if(e>=today):
                exp=str(e.day)+' '+ str(e.strftime('%b').upper())+str(e.year)[2:4]
                exp_future_weekly.append(exp)
                #break
            #i=i+1

        return exp_future_weekly

    def get_future_monthly_expiry(self,scrip='NIFTY',exchange='NFO'):
        ##print("get_weekly_expiry")
        mainlogger.info('Function : get_future_monthly_expiry')

        exp_future_monthly=[]

        today = datetime.datetime.now(ist).date()+datetime.timedelta(days=0)

       
        all_sensex_scrips = self.get_all_nfo_instruments_by_symbol(exchange,scrip) 
        expiry=set(all_sensex_scrips.expiry)

        expiry=list(expiry)
        expiry.sort()

        while True:
            i=0
            ##print(expiry[i],expiry[i].month)
            ##print(len(expiry))

            if(len(expiry)<=2):
                exp=str(expiry[0].day)+' '+ str(expiry[0].strftime('%b').upper())+str(expiry[0].year)[2:4]
                ##print("added:",expiry[-1],expiry[-1].month)
                exp_future_monthly.append(exp)
                
                exp=str(expiry[1].day)+' '+ str(expiry[1].strftime('%b').upper())+str(expiry[1].year)[2:4]
                ##print("added:",expiry[-1],expiry[-1].month)
                exp_future_monthly.append(exp)
                break

            while True:
                ##print("inn",expiry[i],expiry[i].month,expiry[i+1],expiry[i+1].month)
                if(expiry[i]==today and expiry[i].month!=expiry[i+1].month ):
                    exp=str(expiry[i].day)+' '+ str(expiry[i].strftime('%b').upper())+str(expiry[i].year)[2:4]
                    exp_future_monthly.append(exp)
                    ##print("added:",expiry[i],expiry[i].month)
                    expiry=expiry[i+1:]
                    ##print("remianing",expiry)
                    break

                elif(len(expiry)<=2):
                    #exp=str(expiry[0].day)+' '+ str(expiry[0].strftime('%b').upper())+str(expiry[0].year)[2:4]
                    #exp_future_monthly.append(exp)
                    break

                elif(expiry[i]>today and expiry[i].month!=expiry[i+1].month ):
                    exp=str(expiry[i].day)+' '+ str(expiry[i].strftime('%b').upper())+str(expiry[i].year)[2:4]
                    exp_future_monthly.append(exp)
                    ##print("added:",expiry[i],expiry[i].month)
                    expiry=expiry[i+1:]
                    ##print("remianing",expiry)
                    break

                i=i+1

        return exp_future_monthly
    
    

    def get_ltp(self, kite, exchange, symbol, circuit_limits=False):
        mainlogger.info('Function : kite get_ltp')
        
        if circuit_limits==False:
            ins=self.get_instrument_by_symbol(exchange, symbol)

            ins=exchange+':'+ins.symbol
            price=kite.ltp(ins)[ins]['last_price']

            return price
        else:
            ins=self.get_instrument_by_symbol(exchange, symbol)
            token=str(ins[1])
            
            ins=exchange+':'+ins.symbol
            q={'higher_circuit_limit':100000,'lower_circuit_limit':0}
            #quote api is 1 per sec
            try:
                for tries in range(3):
                    try:
                        q=kite.quote(ins)[token]
                        mainlogger.info('Function : kite circuit limits'+str(q))
                    except:
                        time.sleep(round(random.uniform(1, 3),2))
                        mainlogger.info('Function : kite circuit limits exceeds rate - Retry')
                        pass
                    else:
                        break;
            except:
                q=kite.quote(ins)[token]
            
            lower_circuit_limit=q['lower_circuit_limit']
            upper_circuit_limit=q['upper_circuit_limit']
    
            return {'higher_circuit_limit':upper_circuit_limit,'lower_circuit_limit':lower_circuit_limit}

    def punch_order(self, acc_id, kite, alice_blue_master, symbol,direction, qty, triggerPrice, price, 
                order_type='MARKET',misFlag='NRML', amoFlag=False, exchange='NFO', 
                strategyName='NA'
               ):
    
        mainlogger.info('Function : kite punch_order')
        alice_blue_master=None
        
        try:
            tx_type='BUY' if (direction == 'buy' or  direction == 'b') else 'SELL'
            
            prod_type='NRML' if misFlag == 'NRML' else 'MIS'
            prod_type= prod_type if exchange in ('NFO', 'BFO') else 'CNC'


            if order_type=='MARKET':
                order_type='MARKET'
                price=0.0
                trigger_price = None

            elif order_type=='SLM':
                order_type='SL-M'
                price=0.0
                trigger_price = triggerPrice

            elif order_type=='SLL':
                order_type='SL'
                price=price
                trigger_price = triggerPrice

            elif order_type=='LIMIT':
                order_type='LIMIT'
                trigger_price = None

            amoFlag='regular' if amoFlag==False else 'amo'

            ordertag=order_tag_prefix+'_'+str(random.randrange(1000,10000000))
                     
            for attempt in range(2):
                try:
                    if order_type=='LIMIT':
                        if price==0.0  or price is None:
                            ins=exchange+':'+symbol
                            price=kite.ltp(ins)[ins]['last_price']

                            if price==None or price<0.5:
                                price=0.0
                                order_type='MARKET' 
                            elif tx_type=='SELL':
                                price=round(price*limit_price_factor_sell,1)
                            elif tx_type=='BUY':
                                price=round(price*limit_price_factor_buy,1)

                    price=round(price,1)
                    trigger_price= trigger_price if trigger_price is None else round(trigger_price,1)
                    
                    ordertag=ordertag[:18]
                    order_id=kite.place_order(variety=amoFlag,exchange=exchange, tradingsymbol=symbol, transaction_type=tx_type,quantity=qty, 
                             product=prod_type, order_type=order_type, price=price, validity='DAY',
                                  disclosed_quantity=None, trigger_price=trigger_price,squareoff=None, stoploss=None,
                                  trailing_stoploss=None, tag=ordertag)
                
                    response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id}}     

                except Exception as e:
                    mainlogger.exception("Order placement failed: "+str(e))
                    
                    try:
                        response={'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     

                        #Expected exception so return the response
                        if "orders are blocked" in str(e) or "request timed out" in str(e):
                            return response
                        
                        #check if Order Got accepted before retrying order to avoid duplication
                        order=broker.get_order_by_tag(acc_id,ordertag)

                        if  order==-2:
                            response={'status': 'rejected', 'message': 'Unable to verify if previous order api was successful. Please verify in broker terminal manually and retry if needed.', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id,'average_price':price}}     
                            return response
                        
                        elif  order!=-1 and order!=-2:
                            response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order['oms_order_id'] ,'average_price':order['average_price']}}     
                            return response 
                        
                        time.sleep(5)
                        self.check_login_token(acc_id)
                        kite=dic_accounts[acc_id]['token']
                    
                    except Exception as e:
                        mainlogger.exception("Punch order failed while retrying: "+str(e))
                    
                else:
                    break;

            return response
        
        except Exception as e:
            mainlogger.exception(str(e))

    ## Exit an order
    def exit_order(self, acc_id, kite, alice_blue_master, exchange, direction, symbol, order_id, qty, prod_type,order_type='MARKET', price=0.0):
        mainlogger.info('Function : kite exit_order')
        alice_blue_master=None
        try:
            tx_type='BUY' if (direction == 'buy' or  direction == 'b') else 'SELL'
               
            for attempt in range(2):
                try:
                    if order_type=='MARKET':
                        price=None

                    elif order_type=='LIMIT':
                        if price==0.0  or price is None:
                            ins=exchange+':'+symbol
                            price=kite.ltp(ins)[ins]['last_price']

                            if price==None or price<0.5:
                                price=0.0
                                order_type='MARKET'  
                            elif tx_type=='SELL':
                                price=round(price*limit_price_factor_sell,1)
                            elif tx_type=='BUY':
                                price=round(price*limit_price_factor_buy,1)

                    mainlogger.info('Function : kite exit_order order_type'+order_type)
                    order_id=kite.modify_order(variety='regular',order_id=order_id, parent_order_id=None, price=price, order_type=order_type) 

                    response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id}}     

                except Exception as e:
                    mainlogger.exception("Order exit failed: "+str(e))
                    response={'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     
                    
                    #Expected exception so return the response
                    if "orders are blocked" in str(e) or "request timed out" in str(e):
                        return response
                    
                    #Expected exception so return the response
                    elif "Order cannot be modified as it is being processed" in str(e):
                        response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id}}
                        return response
                    
                    time.sleep(5)
                    self.check_login_token(acc_id)
                    kite=dic_accounts[acc_id]['token']
                    
                else:
                    break;

            return response
        
        except Exception as e:
            mainlogger.exception(str(e))
    
    ## Modify an order
    def modify_order(self, acc_id, kite, alice_blue_master, exchange, direction, symbol, order_id, qty, prod_type, order_type, price, triggerPrice):
        mainlogger.info('Function : kite modify_order')
        
        try:
            if order_type=='MARKET':
                order_type='MARKET'
                price=0.0
                trigger_price = None

            elif order_type=='SLM':
                order_type='SL-M'
                price=0.0
                trigger_price = triggerPrice

            elif order_type=='SLL':
                order_type='SL'
                price=price
                trigger_price = triggerPrice

            elif order_type=='LIMIT':
                order_type='LIMIT'
                price=price
                trigger_price = None

            tx_type='BUY' if (direction == 'buy' or  direction == 'b') else 'SELL'

            for attempt in range(2):
                try:                    
                    order_id=kite.modify_order(variety='regular', order_id=order_id, parent_order_id=None, quantity=int(qty), price=price, order_type=order_type, trigger_price=trigger_price) 
                    response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id}}    

                except Exception as e:
                    mainlogger.exception("Order modify failed: "+str(e))
                    response={'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     
                    
                    #Expected exception so return the response
                    if "orders are blocked" in str(e) or "request timed out" in str(e):
                        return response
                    
                    time.sleep(5)
                    self.check_login_token(acc_id)
                    kite=dic_accounts[acc_id]['token']
                    
                else:
                    break;

            return response
        
           

        except Exception as e:
            mainlogger.exception(str(e))
            
    def check_if_position_open(self, kite, exchange, symbol, qty, direction):
        mainlogger.info('Function : KiteBroker check_if_position_open')
        
        try:
            trade_book=kite.positions()
            
            if trade_book is None or len(trade_book)==0:
                mainlogger.exception("Retruned Empty Position Book")
                return True,10000
            
            for t in trade_book['net']:
               # #print(t)
                #if t['quantity']!=0 and direction.upper()=='BUY' and t['exchange']==exchange and (t['tradingsymbol'])==symbol and (int(t['sell_quantity'])>=int(qty) or int(t['total_sell_quantity'])>=int(qty)) :
                if t['quantity']!=0 and direction.upper()=='BUY' and t['exchange']==exchange and (t['tradingsymbol'])==symbol and int(t['quantity'])<0:
                    return True,abs(int(t['quantity']))
                
                #elif t['quantity']!=0 and  direction.upper()=='SELL' and t['exchange']==exchange and (t['tradingsymbol'])==symbol and (int(t['buy_quantity'])>=int(qty) or int(t['total_buy_quantity'])>=int(qty)):
                elif t['quantity']!=0 and  direction.upper()=='SELL' and t['exchange']==exchange and (t['tradingsymbol'])==symbol and int(t['quantity'])>0:
                    return True,abs(int(t['quantity']))
                    
            return False,0
        
        except Exception as e:
            mainlogger.exception(str(e))
            return True,10000
            
    def get_order_history(self, acc_id, order_id):
        mainlogger.info('Function : kite get_order_history')
        
        if order_id==zerodha_failed_order_hardcoded_id:
            tmp={'order_status': 'rejected', 'average_price':0, 'filled_quantity':0, 'message': 'NA', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     
            return tmp
        
        max_tries=2
        for attempt in range(max_tries):
            try:
                tmp=dic_accounts[acc_id]['token'].order_history(order_id)[-1]
                tmp['oms_order_id']=tmp['order_id']
                tmp['order_status']=tmp['status']
                tmp['price_to_fill']=tmp['price']
                tmp['rejection_reason']=tmp['status_message']
                try:
                    tmp['order_tag']=tmp['tag']
                except:
                    tmp['order_tag']=None

            except Exception as te:
                if attempt>=max_tries-1:
                    mainlogger.exception('Function : get_order_history '+str(te))
                    
                time.sleep(2)
                tmp={'order_status': 'not_found', 'average_price':0, 'filled_quantity':0, 'message': 'order not found/api error', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     
                
            else:
                break
                
        return tmp
        
    def get_all_order_history(self, acc_id):
        mainlogger.info('Function : kite get_all_order_history') 
        
        orders=[]

        for tmp in dic_accounts[acc_id]['token'].orders():
            try:
                tmp['oms_order_id']=tmp['order_id']
                tmp['order_status']=tmp['status']
                tmp['exchange_time']=tmp['exchange_timestamp'].timestamp()
                tmp['rejection_reason']=tmp['status_message']
                tmp['order_tag']=tmp['tag']

                orders.append(tmp)
            except:
                pass

        return orders
    
    
    def tradingAccount_PNLSummary(self, acc_id): 
        mainlogger.info('Function : Kite tradingAccount_PNLSummary')
        
        positions=dic_accounts[acc_id]['token'].positions()['net']

        totalRealizedPNL=0
        totalUnRealizedPNL=0

        maxrealizedLossPosition=0
        maxrealizedPoriftPosition=0

        maxUnrealizedLossPosition=0
        maxUnrealizedProfitPosition=0


        for p in positions:
            
            if p['quantity']!=0:
                
                totalRealizedPNL=totalRealizedPNL+float(p['realised'])
                totalUnRealizedPNL=totalUnRealizedPNL+float(p['unrealised'])

                if float(p['realised'])>maxrealizedPoriftPosition:
                    maxrealizedPoriftPosition=float(p['realised'])

                if float(p['realised'])<0 and abs(float(p['realised']))>maxrealizedLossPosition:
                    maxrealizedLossPosition=abs(float(p['realised']))


                if float(p['unrealised'])>maxUnrealizedProfitPosition:
                    maxUnrealizedProfitPosition=float(p['unrealised'])

                if float(p['unrealised'])<0 and abs(float(p['unrealised']))>maxUnrealizedLossPosition:
                    maxUnrealizedLossPosition=abs(float(p['unrealised']))



        pnlSummary={"totalRealizedPNL":round(totalRealizedPNL,1),"totalUnRealizedPNL":round(totalUnRealizedPNL,1),
                    "maxrealizedPoriftPosition":round(maxrealizedPoriftPosition,1),"maxrealizedLossPosition":round(maxrealizedLossPosition,1)*-1,
                    "maxUnrealizedProfitPosition":round(maxUnrealizedProfitPosition,1),"maxUnrealizedLossPosition":round(maxUnrealizedLossPosition,1)*-1
                   }
        
        return pnlSummary
            
            


# 
# # Alice2.0

# %%


class CryptoJsAES:
  @staticmethod
  def __pad(data):
    BLOCK_SIZE = 16
    length = BLOCK_SIZE - (len(data) % BLOCK_SIZE)
    return data + (chr(length) * length).encode()

  @staticmethod
  def __unpad(data):
    return data[:-(data[-1] if type(data[-1]) == int else ord(data[-1]))]

  def __bytes_to_key(data, salt, output=48):
    assert len(salt) == 8, len(salt)
    data += salt
    key = hashlib.md5(data).digest()
    final_key = key
    while len(final_key) < output:
      key = hashlib.md5(key + data).digest()
      final_key += key
    return final_key[:output]

  @staticmethod
  def encrypt(message, passphrase):
    salt = Random.new().read(8)
    key_iv = CryptoJsAES.__bytes_to_key(passphrase, salt, 32 + 16)
    key = key_iv[:32]
    iv = key_iv[32:]
    aes = AES.new(key, AES.MODE_CBC, iv)
    return base64.b64encode(b"Salted__" + salt + aes.encrypt(CryptoJsAES.__pad(message)))

  @staticmethod
  def decrypt(encrypted, passphrase):
    encrypted = base64.b64decode(encrypted)
    assert encrypted[0:8] == b"Salted__"
    salt = encrypted[8:16]
    key_iv = CryptoJsAES.__bytes_to_key(passphrase, salt, 32 + 16)
    key = key_iv[:32]
    iv = key_iv[32:]
    aes = AES.new(key, AES.MODE_CBC, iv)
    return CryptoJsAES.__unpad(aes.decrypt(encrypted[16:]))

class AliceNewBroker:

    #feed_status=datetime.datetime.now(ist)
    #socket_opened = False
    #ws_subscribe_list=[]
    
    global alice_instruments
    
    #alice_instruments={}
    
    download_lock=MyLock()
    
    ##check if token is still valid
    def check_login_token(self,acc_id): 
        mainlogger.info('Function : Alice2 check_login_token')
        
        mainlogger.info(str(dic_accounts[acc_id]))
        dic_accounts[acc_id]['lock'].acquire(thread_lock_timeout)
        
        if (datetime.datetime.now()-dic_accounts[acc_id]['token_refresh']).total_seconds()<5:
            if dic_accounts[acc_id]['lock'].locked()==True:
                dic_accounts[acc_id]['lock'].release()
            mainlogger.info(str(dic_accounts[acc_id]))
            return 1
        
        try:
            if dic_accounts[acc_id]['token'] is None:
                self.account_single_login(acc_id)
                dic_accounts[acc_id]['token_refresh']=datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked()==True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                
            else:
                prof=dic_accounts[acc_id]['token'].get_session_id()
                if 'not_ok' in str(prof).lower():
                    raise Exception('session invalid')
                    
                dic_accounts[acc_id]['token_refresh']=datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked()==True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                
                
        except Exception as e:
            try:
                mainlogger.exception(str(e))
                self.account_single_login(acc_id)
                dic_accounts[acc_id]['token_refresh']=datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked()==True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                
            except Exception as e:
                mainlogger.exception(str(e))
                if dic_accounts[acc_id]['lock'].locked()==True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
    
    def alice_official_login(self, userId,password,twofa,m_pin):
        mainlogger.info('Function: alice_official_weblogin_hack')
        
        if len(twofa)>15:
            twofa=pyotp.TOTP(twofa).now()
                      
        BASE_URL="https://ant.aliceblueonline.com/rest/AliceBlueAPIService"

    
        url = BASE_URL+"/customer/getEncryptionKey"

        payload = json.dumps({
          "userId": userId
        })
        headers = {
          'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        encKey = response.json()["encKey"]

        checksum = CryptoJsAES.encrypt(password.encode(), encKey.encode()).decode('UTF-8')


        url = BASE_URL+"/customer/webLogin"

        payload = json.dumps({
          "userId": userId,
          "userData": checksum
        })
        headers = {
          'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        response_data = response.json()


        url = BASE_URL+"/sso/2fa"

        payload = json.dumps({
          "answer1": twofa,
          "userId": userId,
          "sCount": str(response_data['sCount']),
          "sIndex": response_data['sIndex']
        })
        headers = {
          'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        
        url = BASE_URL+"/sso/verifyTotp"

        twofa_totp=pyotp.TOTP(m_pin).now()

        payload = json.dumps({"tOtp": twofa_totp, "userId": userId})

        headers = {
          'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        ##print(userId,"User login successfully")
        ##print("User session:",response.json()['userSessionID'])
        
        if response.json()['userSessionID'] is None:
            mainlogger.exception('login failed')

    def alice_official_login_old(self, userid,password,twoFA,m_pin):
        
        return 1 ##unused function
    
        for attempt in range(2):
            try:
                url='https://ant.aliceblueonline.com/'

                driver = webdriver.Chrome(chrome_path, options=options)
                driver.get(url)

                usr = driver.find_element(By.XPATH,"/html/body/div[1]/div/div/div/div/div/div/div[1]/div[2]/form/div/input")            
                usr.send_keys(userid)

                login = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div/div[1]/div[2]/form/button').click()
                time.sleep(1)

                ##print(driver.page_source)

                if driver.page_source.find('M-Pin')==-1:
                    #password=m_pin

                    pas = driver.find_element(By.XPATH,"/html/body/div[1]/div/div/div/div/div/div/div[1]/div[2]/form/div/div[1]/span[1]/input")
                    pas.send_keys(password)

                    login = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div/div[1]/div[2]/form/button').click()
                    time.sleep(1)

                    pin = driver.find_element(By.XPATH,"/html/body/div[1]/div/div/div/div/div/div/div[1]/div[2]/form/div/div[1]/span[1]/input")
                    pin.send_keys(twoFA)

                    login = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div/div[1]/div[2]/form/button').click()
                    time.sleep(1)

                else:

                    pas = driver.find_element(By.XPATH,"/html/body/div[1]/div/div/div/div/div/div/div[1]/div[2]/form/div/div[1]/span[1]/input")
                    pas.send_keys(m_pin)

                    login = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div/div[1]/div[2]/form/button').click()
                    time.sleep(1)



                ##print(driver.page_source)
                ##print(driver.current_url)

                #login failed 
                if driver.current_url!='https://ant.aliceblueonline.com/home':
                    print(1/0)
                else:
                    mainlogger.info('Web UI login success')
                    return 1

            except Exception as e:
                mainlogger.exception(str(e))
                time.sleep(2)
                pass
            else:
                break;

        return 0

    
    ##Single LOGIN to Accounts
    def account_single_login(self,acc_id):
        mainlogger.info('Function : alice2_single_login')
        
        global pool
        global dic_accounts
        global primary_alice2
        global alice_instruments 
        #broker_id=24 #alice broker id
        
        coredb = pool.connection()
        sql="select * from vw_activeaccounts where ID="+str(acc_id)
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        master_contracts=[''] 
        for index, row in df.iterrows():
            for attempt in range(2):
                try:
                    #access_token = AliceBlue.login_and_get_access_token(username=row['username'], password=row['password'], twoFA=row['two_fa'], api_secret=row['api_secret'],app_id=row['app_id']) #App ID
                    #alice_blue1 = AliceBlue(username=row['username'], password=row['password'], access_token=access_token, master_contracts_to_download=master_contracts)
                    
                    alice_blue1 = Aliceblue(user_id=row['username'],api_key=row['api_secret'])
                    (alice_blue1.get_session_id())
                    
                    try:
                        prof=str(alice_blue1.get_session_id())
                    except Exception as e:
                        prof='Not_Ok'
                    
                    if 'not_ok' in str(prof).lower():
                        self.alice_official_login(row['username'],str(row['password'].decode('ascii')),row['two_fa'],row['app_id'])
                        alice_blue1 = Aliceblue(user_id=row['username'],api_key=row['api_secret'])
                        (alice_blue1.get_session_id())
                        
                        prof=str(alice_blue1.get_session_id())
                        
                        if 'not_ok' in str(prof).lower():
                            raise Exception('session invalid')
                    
                    dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':alice_blue1,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':dic_accounts[row['ID']]['lock'],'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}
                    
                    mainlogger.info('Profile :'+prof)
                    reset_login_failed_accounts(row['ID'])
                    #master_contracts=['']
                    
                    for tries in range(2):
                        try:
                            if alice_instruments is None or len(alice_instruments)==0:
                                alice_blue1.get_contract_master("NFO")
                                alice_blue1.get_contract_master("NSE")
                                alice_blue1.get_contract_master("BFO")
                                alice_blue1.get_contract_master("BSE")
                                                                
                                primary_alice2=row['ID']


                                pd_nse = pd.read_csv("NSE.csv")
                                pd_nfo = pd.read_csv("NFO.csv")
                                pd_bse = pd.read_csv("BSE.csv")
                                pd_bfo = pd.read_csv("BFO.csv")
                                
                                pd_nfo.drop('Formatted Ins Name', axis=1, inplace=True)
                                pd_bfo.drop('Formatted Ins Name', axis=1, inplace=True)
                                
                                frames = [pd_nse, pd_nfo, pd_bse, pd_bfo]
                                alice_instruments = pd.concat(frames)
                                
                                #handle weird symbol names from shoonya
                                try:
                                    alice_instruments['Instrument Type'].replace('IO', 'OPTIDX', inplace=True)
                                    alice_instruments['Instrument Type'].replace('SO', 'OPTSTK', inplace=True)
                                    alice_instruments['Instrument Type'].replace('IF', 'FUTIDX', inplace=True)
                                    alice_instruments['Instrument Type'].replace('SF', 'FUTSTK', inplace=True)
                                 
                                except:
                                    pass
                                master_contracts=False
                                mainlogger.info('Loaded alice2 instruments')
                                send_notification(group_chat_id,'Loaded alice2 instruments '+str(len(alice_instruments)))
     
                        except Exception as e:
                            time.sleep(5)
                            pass
                        else:
                            break;
                    
                except Exception as e:
                    try:
                        mainlogger.exception(str(e))
                        dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':None,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':dic_accounts[row['ID']]['lock'],'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}
                        handle_exception('Alice Login Failed '+row['username'],str(e),row['tele_chat_id'],True) 
                        time.sleep(1)
                    except Exception as e:
                        mainlogger.exception(str(e))
                else:
                    break;
                
        coredb.close()
    
    def account_login_helper(self, row, download_lock):
        mainlogger.info('Function : account_login_helper')
        global pool
        global dic_accounts
        global primary_alice2
        global alice_instruments 
        
        broker_id=71
        
        for attempt in range(2):
            try:
                self.alice_official_login(row['username'],str(row['password'].decode('ascii')),row['two_fa'],row['app_id'])
                alice_blue1 = Aliceblue(user_id=row['username'],api_key=row['api_secret'])
                (alice_blue1.get_session_id())

                prof=str(alice_blue1.get_session_id())

                if 'not_ok' in str(prof).lower():
                    raise Exception('session invalid')

                dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':alice_blue1,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':MyLock(),'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}

                mainlogger.info('Profile :'+prof)
                reset_login_failed_accounts(row['ID'])

                AliceNewBroker.download_lock.acquire(30)
                
                mainlogger.info('Loading alice2 instruments')
                if alice_instruments is None or len(alice_instruments)==0:
                    try:

                        alice_blue1.get_contract_master("NFO")
                        alice_blue1.get_contract_master("NSE")
                        alice_blue1.get_contract_master("BFO")
                        alice_blue1.get_contract_master("BSE")
                        
                        primary_alice2=row['ID']


                        pd_nse = pd.read_csv("NSE.csv")
                        pd_nfo = pd.read_csv("NFO.csv")
                        pd_bse = pd.read_csv("BSE.csv")
                        pd_bfo = pd.read_csv("BFO.csv")

                
                        pd_nfo.drop('Formatted Ins Name', axis=1, inplace=True)
                        pd_bfo.drop('Formatted Ins Name', axis=1, inplace=True)

                        frames = [pd_nse, pd_nfo, pd_bse, pd_bfo]
                        alice_instruments = pd.concat(frames)

                        #handle weird symbol names from shoonya
                        try:
                            alice_instruments['Instrument Type'].replace('IO', 'OPTIDX', inplace=True)
                            alice_instruments['Instrument Type'].replace('SO', 'OPTSTK', inplace=True)
                            alice_instruments['Instrument Type'].replace('IF', 'FUTIDX', inplace=True)
                            alice_instruments['Instrument Type'].replace('SF', 'FUTSTK', inplace=True)

                        except:
                            pass
                        master_contracts=False

                        mainlogger.info('Loaded alice2 instruments')
                        send_notification(group_chat_id,'Loaded alice2 instruments '+str(len(alice_instruments)))

                    except Exception as e:  
                        pass
                    
                if AliceNewBroker.download_lock.locked()==True:
                    AliceNewBroker.download_lock.release()
                
            except Exception as e:
                try:
                    if AliceNewBroker.download_lock.locked()==True:
                        AliceNewBroker.download_lock.release()
                        
                    dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':None,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':MyLock(),'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}
                    handle_exception('Alice Login Failed '+row['username'],str(e),row['tele_chat_id'],True) 
                    increment_login_failed_accounts(row['ID'])
                    time.sleep(1)
                except Exception as e:
                    mainlogger.exception(str(e))
            else:
                break;
        
    def account_login_parallel(self):
        mainlogger.info('Function : account_login_parallel')
        
        global pool
        global dic_accounts
        global primary_alice2
        global alice_instruments 
        
        futures_list = []
        
        broker_id=71 #alice broker id
        
        coredb = pool.connection()
        sql="select * from vw_activeaccounts where broker="+str(broker_id)
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
     
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            for index,row in df.iterrows():
                try: 
                    futures = executor.submit(self.account_login_helper, row, AliceNewBroker.download_lock) #run_scheduled_one_job(coredb,row)
                    futures_list.append(futures)
                except Exception as e:
                    mainlogger.exception(str(e))
        
        coredb.close()
        
    ##LOGIN to Accounts
    def account_login(self):
        mainlogger.info('Function : alice2_account_login')
        
        global pool
        global dic_accounts
        global primary_alice2
        global alice_instruments 
        
        broker_id=71 #alice broker id
        
        coredb = pool.connection()
        sql="select * from vw_activeaccounts where broker="+str(broker_id)
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        master_contracts=True
                    
        for index, row in df.iterrows():
            for attempt in range(2):
                try:
                    self.alice_official_login(row['username'],str(row['password'].decode('ascii')),row['two_fa'],row['app_id'])
                    alice_blue1 = Aliceblue(user_id=row['username'],api_key=row['api_secret'])
                    (alice_blue1.get_session_id())
                    
                    prof=str(alice_blue1.get_session_id())
                    
                    if 'not_ok' in str(prof).lower():
                        raise Exception('session invalid')
                    
                    dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':alice_blue1,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':MyLock(),'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}
                    
                    mainlogger.info('Profile :'+prof)
                    reset_login_failed_accounts(row['ID'])
                    
                    mainlogger.info('Loading alice2 instruments')
                    if master_contracts==True:
                        try:

                            alice_blue1.get_contract_master("NFO")
                            alice_blue1.get_contract_master("NSE")
                            alice_blue1.get_contract_master("BFO")
                            alice_blue1.get_contract_master("BSE")
                            
                            primary_alice2=row['ID']


                            pd_nse = pd.read_csv("NSE.csv")
                            pd_nfo = pd.read_csv("NFO.csv")
                            pd_bse = pd.read_csv("BSE.csv")
                            pd_bfo = pd.read_csv("BFO.csv")


                            pd_nfo.drop('Formatted Ins Name', axis=1, inplace=True)
                            pd_bfo.drop('Formatted Ins Name', axis=1, inplace=True)

                            frames = [pd_nse, pd_nfo, pd_bse, pd_bfo]
                            alice_instruments = pd.concat(frames)

                            #handle weird symbol names from shoonya
                            try:
                                alice_instruments['Instrument Type'].replace('IO', 'OPTIDX', inplace=True)
                                alice_instruments['Instrument Type'].replace('SO', 'OPTSTK', inplace=True)
                                alice_instruments['Instrument Type'].replace('IF', 'FUTIDX', inplace=True)
                                alice_instruments['Instrument Type'].replace('SF', 'FUTSTK', inplace=True)

                            except:
                                pass
                            master_contracts=False
                           
                            mainlogger.info('Loaded alice2 instruments')
                            send_notification(group_chat_id,'Loaded alice2 instruments '+str(len(alice_instruments)))

                        except Exception as e:  
                            pass
                        
                except Exception as e:
                    try:
                        dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':None,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':MyLock(),'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}
                        handle_exception('Alice Login Failed '+row['username'],str(e),row['tele_chat_id'],True) 
                        increment_login_failed_accounts(row['ID'])
                        time.sleep(1)
                    except Exception as e:
                        mainlogger.exception(str(e))
                else:
                    break;
                
        coredb.close()
        
    
    ##Place All Types of Order
    def custom_get_instrument_for_fno(self, symbol, exp, is_fut, strike, is_CE,segment='NFO'):
        mainlogger.info('Function : alice2 custom_get_instrument_for_fno')
        
        expiry=datetime.datetime.strptime(exp, '%d %b%y').date().strftime('%Y-%m-%d').upper()
        instrument_type= 'CE' if is_CE==True else 'PE'
        #segment='NFO-OPT' if is_fut==False else 'NFO-FUT'
        if segment=='BFO':
            segment='BFO-OPT' if is_fut==False else 'BFO-FUT'
        else:
            segment='NFO-OPT' if is_fut==False else 'NFO-FUT'
            
        strike=float(strike) if strike!=None else None
        
        if 'OPT' in segment:
            instrument=alice_instruments.loc[((alice_instruments['Expiry Date'] == expiry) & (alice_instruments['Exch'] == segment[:3]) & (alice_instruments['Symbol'] == symbol) & (alice_instruments['Instrument Type'].str.startswith('OPT')) & (alice_instruments['Option Type'] ==instrument_type) & ( alice_instruments['Strike Price'] == (strike)) )].iloc[0]
            instrument = Instrument(exchange=instrument['Exch'],token=instrument['Token'] , symbol=instrument['Trading Symbol'], name=instrument['Trading Symbol'], expiry=instrument['Expiry Date'], lot_size=instrument['Lot Size'])
            return instrument

        elif 'FUT' in segment:
            instrument=alice_instruments.loc[((alice_instruments['Expiry Date'] == expiry) & (alice_instruments['Exch'] == segment[:3]) & (alice_instruments['Symbol'] == symbol) & (alice_instruments['Instrument Type'].str.startswith('FUT')) )].iloc[0]
            instrument = Instrument(exchange=instrument['Exch'],token=instrument['Token'] , symbol=instrument['Trading Symbol'], name=instrument['Trading Symbol'], expiry=instrument['Expiry Date'], lot_size=instrument['Lot Size'])
            return instrument
        
    
    def get_instrument_by_symbol(self, exchange, symbol):
        mainlogger.info('Function : alice2 get_instrument_by_symbol')
        
        if exchange=='NSE':
            instrument=alice_instruments.loc[((alice_instruments['Exch'] == exchange) & (alice_instruments['Symbol'] == symbol))].iloc[0]
            instrument = Instrument(exchange=instrument['Exch'],token=instrument['Token'] , symbol=instrument['Trading Symbol'], name=instrument['Trading Symbol'], expiry=instrument['Expiry Date'], lot_size=instrument['Lot Size'])
            return instrument
    
        elif exchange in ('NFO', 'BFO'):
            instrument=alice_instruments.loc[((alice_instruments['Exch'] == exchange) & (alice_instruments['Trading Symbol'] == symbol))].iloc[0]
            instrument = Instrument(exchange=instrument['Exch'],token=instrument['Token'] , symbol=instrument['Trading Symbol'], name=instrument['Trading Symbol'], expiry=instrument['Expiry Date'], lot_size=instrument['Lot Size'])
            return instrument
        
    
    def get_ltp(self, alice_blue, exchange, symbol):
        mainlogger.info('Function : alice2 get_ltp exchange :' + str(exchange) + ' symbol : ' + str(symbol))
        
        try:
            ins=self.get_instrument_by_symbol(exchange,symbol)
            price=float(alice_blue.get_scrip_info(ins)['Ltp'])

        except Exception as e:
            time.sleep(2)
            ins=self.get_instrument_by_symbol(exchange,symbol)
            price=float(alice_blue.get_scrip_info(ins)['Ltp'])

        return price
        
            
    def punch_order(self, acc_id, alice_blue, alice_blue_master, symbol,direction, qty, triggerPrice, price, 
                order_type='MARKET',misFlag='NRML', amoFlag=False, exchange='NFO', 
                strategyName='NA'
               ):
    
        mainlogger.info('Function : alice2 punch_order')
        
        tx_type=TransactionTypeAlice2.Buy if (direction == 'buy' or  direction == 'b') else TransactionTypeAlice2.Sell
        prod_type=ProductTypeAlice2.Delivery if misFlag == 'NRML' else ProductTypeAlice2.Intraday

        if order_type=='MARKET':
            order_type=OrderTypeAlice2.Market
            price=0.0
            trigger_price = None

        elif order_type=='SLM':
            order_type=OrderTypeAlice2.StopLossMarket
            price=0.0
            trigger_price = triggerPrice

        elif order_type=='SLL':
            order_type=OrderTypeAlice2.StopLossLimit
            price=float(price)
            trigger_price = float(triggerPrice)

        elif order_type=='LIMIT':
            order_type=OrderTypeAlice2.Limit

        ins=self.get_instrument_by_symbol(exchange,symbol)

        ordertag=order_tag_prefix+'_'+str(random.randrange(1000,10000000))

        for attempt in range(2):
            try:
                if order_type==OrderTypeAlice2.Limit:
                    try:
                        if price==0.0 or price is None:
                            ins=self.get_instrument_by_symbol(exchange,symbol)
                            price=float(alice_blue.get_scrip_info(ins)['Ltp'])
                            
                            if price==None or price<0.5:
                                price=0.0
                                order_type=OrderTypeAlice2.Market 
                            elif tx_type==TransactionTypeAlice2.Sell:
                                price=round(price*limit_price_factor_sell,1)
                            else:
                                price=round(price*limit_price_factor_buy,1)

                    except Exception as e:
                        time.sleep(2)
                        ins=self.get_instrument_by_symbol(exchange,symbol)
                        price=float(alice_blue.get_scrip_info(ins)['Ltp'])

                        if price==None or price<0.5:
                            price=0.0
                            order_type=OrderTypeAlice2.Market 
                        elif tx_type==TransactionTypeAlice2.Sell:
                            price=round(price*limit_price_factor_sell,1)
                        else:
                            price=round(price*limit_price_factor_buy,1)

                    trigger_price = None
                    

                mainlogger.info("alice2 place_order api triggered")
                
                price=round(price,1)
                trigger_price= trigger_price if trigger_price is None else round(trigger_price,1)
                    
                order_response=alice_blue.place_order(transaction_type = tx_type,
                instrument=ins, quantity = qty, order_type = order_type,
                product_type = prod_type, price = price, trigger_price = trigger_price, stop_loss = None,square_off = None,
                trailing_sl = None,is_amo = amoFlag, order_tag=ordertag)

                mainlogger.info("alice2 place_order api response: " + str(order_response))

                if order_response[0]['stat'].lower()!='ok':
                    raise Exception("order failed: "+str(order_response))

                order_id=order_response[0]['NOrdNo'].split('-')[0]
                response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id,'average_price':price}}     

            except Exception as e:
                mainlogger.exception(str(e))
                
                try:
                    response={'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     

                    #check if Order Got accepted before retrying order to avoid duplication
                    order=broker.get_order_by_tag(acc_id,ordertag)

                    if  order==-2:
                        response={'status': 'rejected', 'message': 'Unable to verify if previous order api was successful. Please verify in broker terminal manually and retry if needed.', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id,'average_price':price}}     
                        return response
                        
                    elif  order!=-1 and order!=-2:
                        response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order['oms_order_id'] ,'average_price':order['average_price']}}     
                        return response 

                    time.sleep(5)
                    self.check_login_token(acc_id)
                    alice_blue=dic_accounts[acc_id]['token']

                except Exception as e:
                    mainlogger.exception("Punch order failed while retrying: "+str(e))
            else:
                break;

        return response
                

    ## Exit an order
    def exit_order(self, acc_id, alice_blue, alice_blue_master, exchange, direction, symbol, order_id, qty, prod_type,order_type='MARKET', price=0.0):
        mainlogger.info('Function : alice2 exit_order')
        
        tx_type=TransactionTypeAlice2.Buy if (direction == 'buy' or direction == 'b') else TransactionTypeAlice2.Sell
            
        if order_type=='MARKET':
            order_type = OrderTypeAlice2.Market
            price=0.0

        elif order_type=='LIMIT':
            order_type = OrderTypeAlice2.Limit

        for attempt in range(2):
            try:
                if order_type==OrderTypeAlice2.Limit:
                    if price==0.0 or price is None:
                        try:
                            ins=self.get_instrument_by_symbol(exchange,symbol)
                            price=float(alice_blue.get_scrip_info(ins)['Ltp'])
                                
                        except Exception as e:
                            time.sleep(2)
                            ins=self.get_instrument_by_symbol(exchange,symbol)
                            price=float(alice_blue.get_scrip_info(ins)['Ltp'])

                        if price==None or price<0.5:
                            price=0.0
                            order_type=OrderTypeAlice2.Market 
                        elif tx_type==TransactionTypeAlice2.Sell:
                            price=round(price*limit_price_factor_sell,1)
                        else:
                            price=round(price*limit_price_factor_buy,1)

                order_response=alice_blue.modify_order(
                    transaction_type = tx_type, instrument= self.get_instrument_by_symbol(exchange,symbol),
                    order_type = order_type, product_type = prod_type,
                    order_id = order_id, price=price, trigger_price = 0.0,quantity =qty)

                if order_response['stat'].lower()!='ok':
                    raise Exception("order failed: "+str(order_response))

                response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id}}     

            except Exception as e:
                mainlogger.exception(str(e))
                response={'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': order_id}}     
                time.sleep(5)
                self.check_login_token(acc_id)
                alice_blue=dic_accounts[acc_id]['token']
            else:
                break;

        return response
    
    
    ## Modify an order
    def modify_order(self, acc_id, alice_blue, alice_blue_master, exchange, direction, symbol, order_id, qty, prod_type, order_type, price, triggerPrice):
        mainlogger.info('Function : modify_order')
    
        if order_type=='MARKET':
            order_type=OrderTypeAlice2.Market
            price=0.0
            trigger_price = None

        elif order_type=='SLM':
            order_type=OrderTypeAlice2.StopLossMarket
            price=0.0
            trigger_price = triggerPrice

        elif order_type=='SLL':
            order_type=OrderTypeAlice2.StopLossLimit
            price=price
            trigger_price = triggerPrice

        elif order_type=='LIMIT':
            order_type=OrderTypeAlice2.Limit
            price=price
            trigger_price = None

        tx_type=TransactionTypeAlice2.Buy if (direction == 'buy' or direction == 'b') else TransactionTypeAlice2.Sell


        for attempt in range(2):
            try:
                order_response=alice_blue.modify_order(
                    transaction_type = tx_type, instrument= self.get_instrument_by_symbol(exchange,symbol),
                    order_type = order_type, product_type = prod_type,
                    order_id = order_id, price=price, trigger_price = trigger_price,quantity =qty)

                if order_response['stat'].lower()!='ok':
                    raise Exception("order failed: "+str(order_response))

                response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id}}     

            except Exception as e:
                mainlogger.exception(str(e))
                response={'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': order_id}}     
                time.sleep(5)
                self.check_login_token(acc_id)
                alice_blue=dic_accounts[acc_id]['token']
            else:
                break;

        return response
        
        
    def check_if_position_open(self, alice, exchange, symbol, qty, direction):
        mainlogger.info('Function : check_if_position_open')
        
        try:
            trade_book=alice.get_netwise_positions()
            ins=self.get_instrument_by_symbol(exchange,symbol)
            
            if trade_book is None or len(trade_book)==0:
                mainlogger.exception("Retruned Empty Position Book")
                return True,10000

            for t in trade_book:
                #if t['Netqty']!=0 and direction.upper()=='BUY' and t['Exchange']==exchange and int(t['Token'])==ins[1] and int(t['Netqty'])<=int(qty)*-1 :
                if t['Netqty']!=0 and direction.upper()=='BUY' and t['Exchange']==exchange and int(t['Token'])==ins[1] and int(t['Netqty'])*-1>0:
                    return True,abs(int(t['Netqty']))

                #elif t['Netqty']!=0 and  direction.upper()=='SELL' and t['Exchange']==exchange and int(t['Token'])==ins[1] and int(t['Netqty'])>=int(qty):
                elif t['Netqty']!=0 and  direction.upper()=='SELL' and t['Exchange']==exchange and int(t['Token'])==ins[1] and int(t['Netqty'])>0:
                    return True,abs(int(t['Netqty']))
                
            return False,0
        except Exception as e:
            mainlogger.exception(str(e))
            return True,10000
            
    def get_order_history(self,acc_id,order_id):
        mainlogger.info('Function : alice2 get_order_history')
        
        if order_id==zerodha_failed_order_hardcoded_id:
            tmp={'order_status': 'rejected', 'average_price':0, 'filled_quantity':0, 'message': 'NA', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     
            return tmp

        for attempt in range(2):
            try:
                tmp=dic_accounts[acc_id]['token'].get_order_history(order_id)[0]

                #mainlogger.info('alice2 get_order_history response ' + str(tmp))
                
                tmp['oms_order_id']=tmp['nestordernumber']
                tmp['order_status']=tmp['Status'].replace('_',' ').lower().replace('canceled','cancelled')
                #tmp['order_status']='trigger pending' ## remove
                tmp['price_to_fill']=float(tmp['Prc'])
                tmp['transaction_type']=tmp['Action']
                tmp['order_type']=tmp['Ordtype']
                tmp['product']=tmp['productcode']
                tmp['exchange']=tmp['exchange']

                try:
                    tmp['order_tag']=tmp['remarks']
                except:
                    tmp['order_tag']=None

                try:
                    tmp['average_price']=float(tmp['averageprice'])
                except Exception as e:
                    tmp['average_price']=None

                try:
                    tmp['trigger_price']=float(tmp['triggerprice'])
                except Exception as e:   
                    tmp['trigger_price']=None

                try:
                    tmp['rejection_reason']=tmp['rejectionreason']
                except Exception as e:   
                    tmp['rejection_reason']=None

                try:
                    tmp['filled_quantity']=int(tmp['filledShares'])
                    tmp['quantity']=int(tmp['Qty'])

                except Exception as e:
                    try:
                        tmp['filled_quantity']=int(tmp['Qty'])
                        tmp['quantity']=int(tmp['Qty'])
                    except Exception as e:
                        tmp['filled_quantity']=None
                        tmp['quantity']=None


            except Exception as te:
                mainlogger.exception('Function : get_order_history '+str(te))
                time.sleep(2)
                tmp={'order_status': 'not_found', 'average_price':0, 'filled_quantity':0, 'message': 'order not found/api error', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     
            else:
                break
        return tmp
        
   
    def get_all_order_history(self,acc_id):
        mainlogger.info('Function : alice2 get_all_order_history')
        
        orders=[]
        
        tmp_orders=dic_accounts[acc_id]['token'].get_order_history('')
        
        if type(tmp_orders) is not list:
            mainlogger.exception('Function : get_all_order_history empty orderbook')
            return orders
        
        for tmp in tmp_orders:
            try:
                tmp['oms_order_id']=tmp['Nstordno']
                tmp['order_status']=tmp['Status'].replace('_',' ')
                tmp['price_to_fill']=float(tmp['Prc'])

                tmp['transaction_type']=tmp['Trantype']
                tmp['order_type']=tmp['Prctype']
                tmp['product']=tmp['Pcode']
                tmp['trigger_price']=None
                tmp['order_tag']=tmp['remarks']
                tmp['exchange_time']=datetime.datetime.strptime(tmp['OrderedTime'], '%d/%m/%Y %H:%M:%S').timestamp()
                tmp['rejection_reason']=tmp['RejReason']
                tmp['exchange']=tmp['Exchange']

                try:
                    tmp['average_price']=float(tmp['Avgprc'])
                except Exception as e:
                    tmp['average_price']=None

                try:
                    tmp['trigger_price']=float(tmp['Trgprc'])
                except Exception as e:   
                    tmp['trigger_price']=None

                try:
                    tmp['filled_quantity']=int(tmp['filledShares'])
                    tmp['quantity']=int(tmp['Qty'])
                except Exception as e:
                    try:
                        tmp['filled_quantity']=int(tmp['Qty'])
                        tmp['quantity']=int(tmp['Qty'])
                    except Exception as e:
                        tmp['filled_quantity']=None
                        tmp['quantity']=None

                orders.append(tmp)

            except Exception as e:
                mainlogger.exception('Function : get_all_order_history '+str(e))

        return orders  
    
    def tradingAccount_PNLSummary(self, acc_id): 
        mainlogger.info('Function : alice2 tradingAccount_PNLSummary')
        
        positions=dic_accounts[acc_id]['token'].get_netwise_positions()

        totalRealizedPNL=0
        totalUnRealizedPNL=0

        maxrealizedLossPosition=0
        maxrealizedPoriftPosition=0

        maxUnrealizedLossPosition=0
        maxUnrealizedProfitPosition=0


        if positions!=None and 'not_ok' not in str(positions).lower():

            for p in positions:

                p['realisedprofitloss']=p['realisedprofitloss'].replace(',','').replace('.*','')
                p['unrealisedprofitloss']=p['unrealisedprofitloss'].replace(',','').replace('.*','')
                
                ##print(p)
                totalRealizedPNL=totalRealizedPNL+float(p['realisedprofitloss'])
                totalUnRealizedPNL=totalUnRealizedPNL+float(p['unrealisedprofitloss'])

                if float(p['realisedprofitloss'])>maxrealizedPoriftPosition:
                    maxrealizedPoriftPosition=float(p['realisedprofitloss'])

                if float(p['realisedprofitloss'])<0 and abs(float(p['realisedprofitloss']))>maxrealizedLossPosition:
                    maxrealizedLossPosition=abs(float(p['realisedprofitloss']))


                if float(p['unrealisedprofitloss'])>maxUnrealizedProfitPosition:
                    maxUnrealizedProfitPosition=float(p['unrealisedprofitloss'])

                if float(p['unrealisedprofitloss'])<0 and abs(float(p['unrealisedprofitloss']))>maxUnrealizedLossPosition:
                    maxUnrealizedLossPosition=abs(float(p['unrealisedprofitloss']))


        pnlSummary={"totalRealizedPNL":round(totalRealizedPNL,1),"totalUnRealizedPNL":round(totalUnRealizedPNL,1),
                    "maxrealizedPoriftPosition":round(maxrealizedPoriftPosition,1),"maxrealizedLossPosition":round(maxrealizedLossPosition,1)*-1,
                    "maxUnrealizedProfitPosition":round(maxUnrealizedProfitPosition,1),"maxUnrealizedLossPosition":round(maxUnrealizedLossPosition,1)*-1
                   }

        return pnlSummary


# # Shoonya Class

# %%


class ShoonyaBroker:

    feed_status=datetime.datetime.now(ist)
    socket_opened = False
    ws_subscribe_list=[]
    
   ##check if token is still valid -done
    
    def handle_two_fa(self,row):
        mainlogger.info('Function : Shoonya handle_two_fa')
        if (len(row['two_fa'])==32):
            return pyotp.TOTP(row['two_fa']).now()
        return row['two_fa']
    
    
    def check_login_token(self,acc_id): 
        mainlogger.info('Function : ShoonyaBroker check_login_token')
        
        mainlogger.info(str(dic_accounts[acc_id]))
        dic_accounts[acc_id]['lock'].acquire(thread_lock_timeout)
        
        if (datetime.datetime.now()-dic_accounts[acc_id]['token_refresh']).total_seconds()<5:
            if dic_accounts[acc_id]['lock'].locked()==True:
                dic_accounts[acc_id]['lock'].release()
            mainlogger.info(str(dic_accounts[acc_id]))
            return 1
        
        try:
            if dic_accounts[acc_id]['token'] is None:
                self.account_single_login(acc_id)
                dic_accounts[acc_id]['token_refresh']=datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked()==True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                
            elif dic_accounts[acc_id]['token'].get_holdings()!=None:
                dic_accounts[acc_id]['token_refresh']=datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked()==True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                
            elif dic_accounts[acc_id]['token'].get_holdings() is None:
                self.account_single_login(acc_id)
                dic_accounts[acc_id]['token_refresh']=datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked()==True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))  
                    
        except Exception as e:
            mainlogger.exception(str(e))
            if dic_accounts[acc_id]['lock'].locked()==True:
                dic_accounts[acc_id]['lock'].release()
            mainlogger.info(str(dic_accounts[acc_id]))
                
    ##Single LOGIN to Accounts
    def account_single_login(self,acc_id):
        mainlogger.info('Function : ShoonyaBroker single login')
        
        global pool
        global dic_accounts
        global shoonya_instruments
        #shoonya_instruments={}
        
        #broker_id=26 #alice broker id
        
        coredb = pool.connection()
        sql="select * from vw_activeaccounts where ID="+str(acc_id)
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        for index, row in df.iterrows():
            for attempt in range(2):
                try:
                    
                    shoonya = NorenApi(host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/', eodhost='https://api.shoonya.com/chartApi/getdata/')
                    
                    two_fa=self.handle_two_fa(row)

                    r= shoonya.login(
                        userid=row['username'],
                        password=str(row['password'].decode('ascii')),
                        twoFA=two_fa,
                        vendor_code=row['username']+'_U',
                        api_secret=row['api_secret'],
                        imei=row['app_id']
                        )
                    
                    if r is None:
                        print(1/0)

                    alice_blue1=shoonya
                    
                    dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':alice_blue1,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':dic_accounts[row['ID']]['lock'],'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}

                    mainlogger.info('Profile :'+str(r))
                    reset_login_failed_accounts(row['ID'])
                    
                    for tries in range(2):
                        try:
                            if shoonya_instruments is None or len(shoonya_instruments)==0:
                                try:
                                    root = 'https://api.shoonya.com/'
                                    #root = 'https://shoonya.finvasia.com/'
                                    
                                    #masters = ['NSE_symbols.txt.zip', 'NFO_symbols.txt.zip', 'CDS_symbols.txt.zip', 'MCX_symbols.txt.zip'] 
                                    #masters = ['NSE_symbols.txt.zip', 'NFO_symbols.txt.zip'] 
                                    masters = ['NSE_symbols.txt.zip', 'NFO_symbols.txt.zip','BSE_symbols.txt.zip', 'BFO_symbols.txt.zip'] 

                                    zip_file=masters[0]
                                    
                                    url = root + zip_file
                                    r = requests.get(url, stream=True)
                                    open(zip_file, 'wb').write(r.content)
                                    file_to_extract = zip_file.split()

                                    try:
                                        with zipfile.ZipFile(zip_file) as z:
                                            z.extractall()
                                            #print("Extracted: ", zip_file)
                                            pd_nse = pd.read_csv(zip_file)
                                    except:
                                        mainlogger.exception('Error downloading shoonya instruments '+str(e))

                                    os.remove(zip_file)
                                    
                                    zip_file=masters[1]
                                
                                    url = root + zip_file
                                    r = requests.get(url, stream=True)
                                    open(zip_file, 'wb').write(r.content)
                                    file_to_extract = zip_file.split()

                                    try:
                                        with zipfile.ZipFile(zip_file) as z:
                                            z.extractall()
                                            #print("Extracted: ", zip_file)
                                            pd_nfo = pd.read_csv(zip_file)
                                    except:
                                        mainlogger.exception('Error downloading shoonya instruments '+str(e))

                                    os.remove(zip_file)
                                                                        
                                    zip_file=masters[2]
                                
                                    url = root + zip_file
                                    r = requests.get(url, stream=True)
                                    open(zip_file, 'wb').write(r.content)
                                    file_to_extract = zip_file.split()

                                    try:
                                        with zipfile.ZipFile(zip_file) as z:
                                            z.extractall()
                                            #print("Extracted: ", zip_file)
                                            pd_bse = pd.read_csv(zip_file)
                                    except:
                                        mainlogger.exception('Error downloading shoonya instruments '+str(e))

                                    os.remove(zip_file)
                                                                        
                                    zip_file=masters[3]
                                
                                    url = root + zip_file
                                    r = requests.get(url, stream=True)
                                    open(zip_file, 'wb').write(r.content)
                                    file_to_extract = zip_file.split()

                                    try:
                                        with zipfile.ZipFile(zip_file) as z:
                                            z.extractall()
                                            #print("Extracted: ", zip_file)
                                            pd_bfo = pd.read_csv(zip_file)
                                    except:
                                        mainlogger.exception('Error downloading shoonya instruments '+str(e))

                                    os.remove(zip_file)
                                                                        
                                    #print(f'remove: {zip_file}')

                                    frames = [pd_nse, pd_nfo, pd_bse, pd_bfo]
        
                                    shoonya_instruments = pd.concat(frames)
            
                                    #handle weird symbol names from shoonya
                                    try:
                                        shoonya_instruments['Symbol'].replace('SX50OPT', 'SENSEX50', inplace=True)
                                        shoonya_instruments['Symbol'].replace('BSXOPT', 'SENSEX', inplace=True)
                                        shoonya_instruments['Symbol'].replace('BKXOPT', 'BANKEX', inplace=True)
                                    except:
                                        pass
                                    
                                    mainlogger.info('loaded shoonya instruments')
                                    send_notification(group_chat_id,'Loaded shoonya instruments '+str(len(shoonya_instruments)))

                                except Exception as e:
                                    #handle_exception('Kite instruments load failed '+row['username'],str(e),row['tele_chat_id'],True) 
                                    send_notification(group_chat_id, emoji_warning+'Failed to load Shoonya instruments')
                                    mainlogger.exception('Shoonya instruments load failed')
                                    
                        except Exception as e:
                            time.sleep(5)
                            pass
                        else:
                            break;
                            
                except Exception as e:
                    try:
                        dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':None,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':dic_accounts[row['ID']]['lock'],'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}
                        handle_exception('Shoonya Login Failed '+row['username'],str(e),row['tele_chat_id'],True) 
                        time.sleep(1)
                    except Exception as e:
                        mainlogger.exception(str(e))
                else:
                    break;
                
        coredb.close()
    
    ##LOGIN to Accounts
    def account_login(self):
        mainlogger.info('Function : ShoonyaBroker login')
        
        global pool
        global dic_accounts
        global shoonya_instruments
        global primary_shoonya
        
        #shoonya_instruments={}
        
        broker_id=70 #shoonya broker id
        
        coredb = pool.connection()
        sql="select * from vw_activeaccounts where broker="+str(broker_id)
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        
        for index, row in df.iterrows():
            for attempt in range(2):
                try:
                    shoonya = NorenApi(host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/', eodhost='https://api.shoonya.com/chartApi/getdata/')
                    
                    two_fa=self.handle_two_fa(row)

                    r= shoonya.login(
                        userid=row['username'],
                        password=str(row['password'].decode('ascii')),
                        twoFA=two_fa,
                        vendor_code=row['username']+'_U',
                        api_secret=row['api_secret'],
                        imei=row['app_id']
                        )
                    
                    if r is None:
                        print(1/0)

                    alice_blue1=shoonya
                    
                    dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':alice_blue1,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':MyLock(),'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}

                    mainlogger.info('Profile :'+str(r))
                    reset_login_failed_accounts(row['ID'])
                    
                    for tries in range(2):
                        try:
                            if shoonya_instruments is None or len(shoonya_instruments)==0:
                                try:
                                    root = 'https://api.shoonya.com/'
                                    #masters = ['NSE_symbols.txt.zip', 'NFO_symbols.txt.zip', 'CDS_symbols.txt.zip', 'MCX_symbols.txt.zip'] 
                                    #root = 'https://shoonya.finvasia.com/'
                                    #masters = ['NSE_symbols.txt.zip', 'NFO_symbols.txt.zip'] 
                                    masters = ['NSE_symbols.txt.zip', 'NFO_symbols.txt.zip','BSE_symbols.txt.zip', 'BFO_symbols.txt.zip'] 

                                    zip_file=masters[0]
                                    
                                    url = root + zip_file
                                    r = requests.get(url, stream=True)
                                    open(zip_file, 'wb').write(r.content)
                                    file_to_extract = zip_file.split()

                                    try:
                                        with zipfile.ZipFile(zip_file) as z:
                                            z.extractall()
                                            #print("Extracted: ", zip_file)
                                            pd_nse = pd.read_csv(zip_file)
                                    except:
                                        mainlogger.exception('Error downloading shoonya instruments '+str(e))

                                    os.remove(zip_file)
                                    
                                    zip_file=masters[1]
                                
                                    url = root + zip_file
                                    r = requests.get(url, stream=True)
                                    open(zip_file, 'wb').write(r.content)
                                    file_to_extract = zip_file.split()

                                    try:
                                        with zipfile.ZipFile(zip_file) as z:
                                            z.extractall()
                                            #print("Extracted: ", zip_file)
                                            pd_nfo = pd.read_csv(zip_file)
                                    except:
                                        mainlogger.exception('Error downloading shoonya instruments '+str(e))

                                    os.remove(zip_file)
                                                                        
                                    zip_file=masters[2]
                                
                                    url = root + zip_file
                                    r = requests.get(url, stream=True)
                                    open(zip_file, 'wb').write(r.content)
                                    file_to_extract = zip_file.split()

                                    try:
                                        with zipfile.ZipFile(zip_file) as z:
                                            z.extractall()
                                            #print("Extracted: ", zip_file)
                                            pd_bse = pd.read_csv(zip_file)
                                    except:
                                        mainlogger.exception('Error downloading shoonya instruments '+str(e))

                                    os.remove(zip_file)
                                                                        
                                    zip_file=masters[3]
                                
                                    url = root + zip_file
                                    r = requests.get(url, stream=True)
                                    open(zip_file, 'wb').write(r.content)
                                    file_to_extract = zip_file.split()

                                    try:
                                        with zipfile.ZipFile(zip_file) as z:
                                            z.extractall()
                                            #print("Extracted: ", zip_file)
                                            pd_bfo = pd.read_csv(zip_file)
                                    except:
                                        mainlogger.exception('Error downloading shoonya instruments '+str(e))

                                    os.remove(zip_file)
                                                                        
                                    #print(f'remove: {zip_file}')

                                    frames = [pd_nse, pd_nfo, pd_bse, pd_bfo]

                                    shoonya_instruments = pd.concat(frames)
                                    
                                    #handle weird symbol names from shoonya
                                    try:
                                        shoonya_instruments['Symbol'].replace('SX50OPT', 'SENSEX50', inplace=True)
                                        shoonya_instruments['Symbol'].replace('BSXOPT', 'SENSEX', inplace=True)
                                        shoonya_instruments['Symbol'].replace('BKXOPT', 'BANKEX', inplace=True)
                                    except:
                                        pass
                                    
                                    mainlogger.info('loaded shoonya instruments')
                                    send_notification(group_chat_id,'Loaded shoonya instruments '+str(len(shoonya_instruments)))

                                except Exception as e:
                                    #handle_exception('Kite instruments load failed '+row['username'],str(e),row['tele_chat_id'],True) 
                                    send_notification(group_chat_id, emoji_warning+'Failed to load Shoonya instruments')
                                    mainlogger.exception('Shoonya instruments load failed')
                                    
                        except Exception as e:
                            time.sleep(5)
                            pass
                        else:
                            break;
 
                except Exception as e:
                    try:
                        dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':None,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':MyLock(),'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}
                        handle_exception('Shoonya Login Failed '+row['username'],str(e),row['tele_chat_id'],True) 
                        increment_login_failed_accounts(row['ID'])
                        time.sleep(1)
                    except Exception as e:
                        mainlogger.exception(str(e))
                else:
                    break;
                
        coredb.close()

    
    
    ##Place All Types of Order
    def custom_get_instrument_for_fno(self, symbol, exp, is_fut, strike, is_CE,segment='NFO'):
        mainlogger.info('Function : shoonya custom_get_instrument_for_fno')
        
        expiry=datetime.datetime.strptime(exp, '%d %b%y').date().strftime('%d-%b-%Y').upper()
        instrument_type= 'CE' if is_CE==True else 'PE'
        #segment='NFO-OPT' if is_fut==False else 'NFO-FUT'
        if segment=='BFO':
            segment='BFO-OPT' if is_fut==False else 'BFO-FUT'
        else:
            segment='NFO-OPT' if is_fut==False else 'NFO-FUT'
            
        strike=float(strike)

        if 'OPT' in segment:
            instrument=shoonya_instruments.loc[((shoonya_instruments['Expiry'] == expiry) & (shoonya_instruments['Exchange'] == segment[:3]) & (shoonya_instruments['Symbol'] == symbol) & (shoonya_instruments['Instrument'].str.startswith('OPT')) & (shoonya_instruments['OptionType'] ==instrument_type) & ( shoonya_instruments['StrikePrice'] == (strike)) )].iloc[0]
            instrument = Instrument(exchange=instrument['Exchange'],token=instrument['Token'] , symbol=instrument['TradingSymbol'], name=instrument['Symbol'], expiry=instrument['Expiry'], lot_size=instrument['LotSize'])
            return instrument

        elif 'FUT' in segment:
            instrument=shoonya_instruments.loc[((shoonya_instruments['Expiry'] == expiry) & (shoonya_instruments['Exchange'] == segment[:3]) & (shoonya_instruments['Symbol'] == symbol) & (shoonya_instruments['Instrument'].str.startswith('FUT')) )].iloc[0]
            instrument = Instrument(exchange=instrument['Exchange'],token=instrument['Token'] , symbol=instrument['TradingSymbol'], name=instrument['Symbol'], expiry=instrument['Expiry'], lot_size=instrument['LotSize'])
            return instrument
        
    
    def get_instrument_by_symbol(self, exchange, symbol):
        mainlogger.info('Function : shoonya get_instrument_by_symbol')
        
        if exchange=='NSE':
            instrument=shoonya_instruments.loc[((shoonya_instruments['Exchange'] == exchange) & (shoonya_instruments['Symbol'] == symbol))].iloc[0]
            instrument = Instrument(exchange=instrument['Exchange'],token=instrument['Token'] , symbol=instrument['TradingSymbol'], name=instrument['Symbol'], expiry=instrument['Expiry'], lot_size=instrument['LotSize'])
            return instrument
    
        elif exchange in ('NFO', 'BFO'):
            instrument=shoonya_instruments.loc[((shoonya_instruments['Exchange'] == exchange) & (shoonya_instruments['TradingSymbol'] == symbol))].iloc[0]
            instrument = Instrument(exchange=instrument['Exchange'],token=instrument['Token'] , symbol=instrument['TradingSymbol'], name=instrument['Symbol'], expiry=instrument['Expiry'], lot_size=instrument['LotSize'])
            return instrument

     
    def get_ltp(self, shoonya, exchange, symbol):
        mainlogger.info('Function : shoonya get_ltp')
        
        symbol=self.get_instrument_by_symbol(exchange, symbol).symbol
        
        price=float(shoonya.get_quotes(exchange,symbol)['lp'])
        
        return price
    
    def punch_order(self, acc_id, shoonya, alice_blue_master, symbol, direction, qty, triggerPrice, price, 
                order_type='MARKET',misFlag='NRML', amoFlag=False, exchange='NFO', 
                strategyName='NA'
               ):
    
        mainlogger.info('Function : shoonya punch_order')
        alice_blue_master=None
    
        try:
            tx_type='B' if (direction == 'buy' or  direction == 'b') else 'S'
         
            if order_type=='MARKET':
                order_type='MKT'
                price=0.0
                trigger_price = None
                #variety='NORMAL'

            elif order_type=='SLM':
                order_type='SL-MKT'
                price=0.0
                trigger_price = triggerPrice
                #variety='STOPLOSS'

            elif order_type=='SLL':
                order_type='SL-LMT'
                price=price
                trigger_price = triggerPrice
                #variety='STOPLOSS'

            elif order_type=='LIMIT':
                order_type='LMT'
                #variety='NORMAL'

            ##MIS or NORMAL
            if exchange in ('NFO', 'BFO') and misFlag == 'NRML' :
                prod_type= 'M' 

            elif exchange in ('NFO', 'BFO') and misFlag == 'MIS':
                prod_type= 'I' 

            else:
                prod_type= 'C' 

            #AMO or NOT
            is_amo= 'NO' if amoFlag==False else 'YES'
            ordertag=order_tag_prefix+'_'+str(random.randrange(1000,10000000))
                    
            for attempt in range(2):
                try:
                    if order_type=='LMT':
                        
                        if price==0.0 or price is None:
                            price=float(shoonya.get_quotes(exchange,symbol)['lp'])

                            if price==None or price<0.5:
                                price=0.0
                                order_type='MKT'  
                            elif tx_type=='SELL':
                                price=round(price*limit_price_factor_sell,1)
                            else:
                                price=round(price*limit_price_factor_buy,1)

                        trigger_price = None
                    
                    
                    price=round(price,1)
                    trigger_price= trigger_price if trigger_price is None else round(trigger_price,1)
                    
                    r=shoonya.place_order(buy_or_sell=tx_type, product_type=prod_type,
                         exchange=exchange, tradingsymbol=symbol,
                        quantity=qty, discloseqty=0,price_type=order_type, price=price,
                        trigger_price=trigger_price, amo=is_amo,
                         retention='DAY', remarks=ordertag)
                
                    if r is None:
                        raise Exception("order placement failed: Broker API returned None")

                    else:
                        order_id=r['norenordno']
                        response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id,'average_price':price}}     

                except Exception as e:
                    mainlogger.exception("Order placement failed: "+str(e))
                    
                    try:
                        response={'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     

                        #check if Order Got accepted before retrying order to avoid duplication
                        order=broker.get_order_by_tag(acc_id,ordertag)

                        if  order==-2:
                            response={'status': 'rejected', 'message': 'Unable to verify if previous order api was successful. Please verify in broker terminal manually and retry if needed.', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id,'average_price':price}}     
                            return response
                        
                        elif  order!=-1 and order!=-2:
                            response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order['oms_order_id'] ,'average_price':order['average_price']}}     
                            return response 

                        time.sleep(5)
                        self.check_login_token(acc_id)
                        shoonya=dic_accounts[acc_id]['token']

                    except Exception as e:
                        mainlogger.exception("Punch order failed while retrying: "+str(e))
                    
                else:
                    break;

            return response

        except Exception as e:
            mainlogger.exception(str(e))

    ## Exit an order
    def exit_order(self, acc_id, shoonya, alice_blue_master, exchange, direction, symbol, order_id, qty, prod_type,order_type='MARKET', price=0.0):
        mainlogger.info('Function : shoonya exit_order')
        alice_blue_master=None
        try:
            tx_type='B' if (direction == 'buy' or  direction == 'b') else 'S'
            prod_type= 'M' if exchange in ('NFO', 'BFO') else 'C'
            
            if order_type=='MARKET':
                order_type='MKT'
                
            elif order_type=='LIMIT':
                order_type='LMT'
                        
            for attempt in range(2):
                try:
                    if order_type=='MKT':
                        r=shoonya.modify_order(exchange=exchange, tradingsymbol=symbol, orderno=order_id, newprice_type=order_type, newquantity=qty)

                    elif order_type=='LMT':
                        if price==0.0 or price is None:
                            price=float(shoonya.get_quotes(exchange,symbol)['lp'])

                            if price==None or price<0.5:
                                price=0.0
                                order_type='MKT'  
                            elif tx_type=='SELL':
                                price=round(price*limit_price_factor_sell,1)
                            else:
                                price=round(price*limit_price_factor_buy,1)

                        r=shoonya.modify_order(exchange=exchange, tradingsymbol=symbol, orderno=order_id, newprice_type=order_type, newquantity=qty, newprice=price)

                    if r is None:
                        raise Exception("order exit failed: Broker API returned None")

                    order_id=r['result']

                    response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id}}  

                except Exception as e:
                    mainlogger.exception("Order exit failed: "+str(e))
                    response={'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': order_id}}     
                    time.sleep(5)
                    self.check_login_token(acc_id)
                    shoonya=dic_accounts[acc_id]['token']
                else:
                    break;

            return response
        
        except Exception as e:
            mainlogger.exception(str(e))
    
    ## Modify an order   - converted to shooonya
    def modify_order(self, acc_id, shoonya, alice_blue_master, exchange, direction, symbol, order_id, qty, prod_type, order_type, price, triggerPrice):
        mainlogger.info('Function : shoonya modify_order')

        try:
            if order_type=='MARKET':
                order_type='MKT'
                price=0.0
                trigger_price = None


            elif order_type=='SLM':
                order_type='SL-MKT'
                price=0.0
                trigger_price = triggerPrice


            elif order_type=='SLL':
                order_type='SL-LMT'
                price=price
                trigger_price = triggerPrice


            elif order_type=='LIMIT':
                order_type='LMT'
                price=price
                trigger_price = None

            prod_type= 'M' if exchange in ('NFO', 'BFO') else 'C'

            for attempt in range(2):
                try: 
                    r=shoonya.modify_order(exchange=exchange, tradingsymbol=symbol, orderno=order_id, newprice_type=order_type, newquantity=qty, newtrigger_price=trigger_price, newprice=price)
                
                    if r is None:
                        raise Exception("order exit failed: Broker API returned None")

                    order_id=r['result']

                    response={'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id}}  

                except Exception as e:
                    mainlogger.exception("Order modify failed: "+str(e))
                    response={'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': order_id}}     
                    time.sleep(5)
                    self.check_login_token(acc_id)
                    shoonya=dic_accounts[acc_id]['token']
                else:
                    break;

            return response
        
        except Exception as e:
            mainlogger.exception(str(e))
     
    #converted to shoonya
    def check_if_position_open(self, shoonya, exchange, symbol, qty, direction):
        mainlogger.info('Function : shoonya check_if_position_open')
        
        try:
            trade_book=shoonya.get_positions()
            
            if trade_book is None or len(trade_book)==0:
                mainlogger.exception("Retruned Empty Position Book")
                return True,10000
            
                
            for t in trade_book:
                #if int(t['netqty'])!=0 and direction.upper()=='BUY' and t['exch']==exchange and (t['tsym'])==symbol and int(t['netqty'])<=int(qty)*-1 :
                if int(t['netqty'])!=0 and direction.upper()=='BUY' and t['exch']==exchange and (t['tsym'])==symbol and int(t['netqty'])<0:
                    return True, abs(int(t['netqty']))

                #elif int(t['netqty'])!=0 and  direction.upper()=='SELL' and t['exch']==exchange and (t['tsym'])==symbol and int(t['netqty'])>=int(qty):
                elif int(t['netqty'])!=0 and  direction.upper()=='SELL' and t['exch']==exchange and (t['tsym'])==symbol and int(t['netqty'])>0:
                    return True, abs(int(t['netqty']))
                    
            return False,0
        
        except Exception as e:
            mainlogger.exception(str(e))
            return True,10000
            
    def get_order_history(self,acc_id,order_id):
        mainlogger.info('Function : shoonya get_order_history')
        
        if order_id==zerodha_failed_order_hardcoded_id:
            tmp={'order_status': 'rejected', 'average_price':0, 'filled_quantity':0, 'message': 'NA', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     
            return tmp

        for attempt in range(2):
            try:
                tmp=dic_accounts[acc_id]['token'].single_order_history(str(order_id))[0]
                #tmp=dic_accounts[acc_id]['token'].order_history(order_id)[-1]
                tmp['oms_order_id']=tmp['norenordno']
                tmp['order_status']=tmp['status'].replace('_',' ').lower().replace('canceled','cancelled')
                tmp['price_to_fill']=float(tmp['prc'])
                tmp['transaction_type']=tmp['trantype']
                tmp['order_type']=tmp['prctyp']
                tmp['product']=tmp['prd']
                tmp['exchange']=tmp['exch']

                try:
                    tmp['order_tag']=tmp['remarks']
                except:
                    tmp['order_tag']=None

                try:
                    tmp['average_price']=float(tmp['avgprc'])
                except Exception as e:
                    tmp['average_price']=None

                try:
                    tmp['trigger_price']=float(tmp['trgprc'])
                except Exception as e:   
                    tmp['trigger_price']=None

                try:
                    tmp['rejection_reason']=tmp['rejreason']
                except Exception as e:   
                    tmp['rejection_reason']=None

                try:
                    tmp['filled_quantity']=int(tmp['flqty'])
                    tmp['quantity']=int(tmp['flqty'])

                except Exception as e:
                    try:
                        tmp['filled_quantity']=int(tmp['qty'])
                        tmp['quantity']=int(tmp['qty'])
                    except Exception as e:
                        tmp['filled_quantity']=None
                        tmp['quantity']=None


            except Exception as te:
                mainlogger.exception('Function : get_order_history '+str(te))
                time.sleep(2)
                tmp={'order_status': 'not_found', 'average_price':0, 'filled_quantity':0, 'message': 'order not found/api error', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     
            else:
                break
            
        return tmp
        
        
   
    def get_all_order_history(self,acc_id):
        mainlogger.info('Function : shoonya get_all_order_history')
        
        orders=[]
        
        order_book=dic_accounts[acc_id]['token'].get_order_book()
        if order_book is None:
            return orders

        for tmp in order_book:
            try:
                tmp['oms_order_id']=tmp['norenordno']
                tmp['order_status']=tmp['status'].replace('_',' ').lower().replace('canceled','cancelled')
                tmp['price_to_fill']=float(tmp['prc'])

                tmp['transaction_type']=tmp['trantype']
                tmp['order_type']=tmp['prctyp']
                tmp['product']=tmp['prd']
                tmp['trigger_price']=None
                tmp['order_tag']=tmp['remarks']
                #tmp['exchange_time']=datetime.datetime.strptime(tmp['exch_tm'], '%d-%m-%Y %H:%M:%S').timestamp()
                tmp['exchange_time']=datetime.datetime.strptime(tmp['norentm'], '%H:%M:%S %d-%m-%Y').timestamp()
                tmp['exchange']=tmp['exch']

                try:
                    tmp['average_price']=float(tmp['avgprc'])
                except Exception as e:
                    tmp['average_price']=None

                try:
                    tmp['trigger_price']=float(tmp['trgprc'])
                except Exception as e:   
                    tmp['trigger_price']=None
                    
                try:
                    tmp['rejection_reason']=tmp['rejreason']
                except Exception as e:   
                    tmp['rejection_reason']=None
                
                try:
                    tmp['filled_quantity']=int(tmp['flqty'])
                    tmp['quantity']=int(tmp['flqty'])
                except Exception as e:
                    try:
                        tmp['filled_quantity']=int(tmp['qty'])
                        tmp['quantity']=int(tmp['qty'])
                    except Exception as e:
                        tmp['filled_quantity']=None
                        tmp['quantity']=None

                orders.append(tmp)

            except:
                pass

        return orders
            

    def tradingAccount_PNLSummary(self, acc_id): 
        mainlogger.info('Function : shoonya tradingAccount_PNLSummary')

        positions=dic_accounts[acc_id]['token'].get_positions()

        totalRealizedPNL=0
        totalUnRealizedPNL=0

        maxrealizedLossPosition=0
        maxrealizedPoriftPosition=0

        maxUnrealizedLossPosition=0
        maxUnrealizedProfitPosition=0

        if positions!=None and "not_ok" not in str(positions).lower():

            for p in positions:

                totalRealizedPNL=totalRealizedPNL+float(p['rpnl'])
                totalUnRealizedPNL=totalUnRealizedPNL+float(p['urmtom'])

                if float(p['rpnl'])>maxrealizedPoriftPosition:
                    maxrealizedPoriftPosition=float(p['rpnl'])

                if float(p['rpnl'])<0 and abs(float(p['rpnl']))>maxrealizedLossPosition:
                    maxrealizedLossPosition=abs(float(p['rpnl']))


                if float(p['urmtom'])>maxUnrealizedProfitPosition:
                    maxUnrealizedProfitPosition=float(p['urmtom'])

                if float(p['urmtom'])<0 and abs(float(p['urmtom']))>maxUnrealizedLossPosition:
                    maxUnrealizedLossPosition=abs(float(p['urmtom']))



        pnlSummary={"totalRealizedPNL":round(totalRealizedPNL,1),"totalUnRealizedPNL":round(totalUnRealizedPNL,1),
                    "maxrealizedPoriftPosition":round(maxrealizedPoriftPosition,1),"maxrealizedLossPosition":round(maxrealizedLossPosition,1)*-1,
                    "maxUnrealizedProfitPosition":round(maxUnrealizedProfitPosition,1),"maxUnrealizedLossPosition":round(maxUnrealizedLossPosition,1)*-1
                   }

        return pnlSummary


# # IIFL XTS Class

# %%
class IIFLBroker:

    def check_login_token(self, acc_id): 
        mainlogger.info('Function : iifl Broker check_login_token')
        
        mainlogger.info(str(dic_accounts[acc_id]))
        dic_accounts[acc_id]['lock'].acquire( thread_lock_timeout)   
        
        if (datetime.datetime.now() - dic_accounts[acc_id]['token_refresh']).total_seconds() < 5:
            if dic_accounts[acc_id]['lock'].locked() == True:
                dic_accounts[acc_id]['lock'].release()
            mainlogger.info(str(dic_accounts[acc_id]))
            return 1
        
        try:
            if dic_accounts[acc_id]['token']  is None:
                self.account_single_login(acc_id)
                dic_accounts[acc_id]['token_refresh'] = datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked() == True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                
            elif dic_accounts[acc_id]['token']['interactive'].get_profile()['type'] != "error":
                dic_accounts[acc_id]['token_refresh'] = datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked() == True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                
            elif dic_accounts[acc_id]['token']['interactive'].get_profile()['type'] == "error":
                self.account_single_login(acc_id) # try relogin if the token is invalid/expired
                dic_accounts[acc_id]['token_refresh'] = datetime.datetime.now()
                if dic_accounts[acc_id]['lock'].locked() == True:
                    dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))
                    
        except Exception as e:
            mainlogger.exception(str(e))
            if dic_accounts[acc_id]['lock'].locked() == True:
                dic_accounts[acc_id]['lock'].release()
                mainlogger.info(str(dic_accounts[acc_id]))


    def get_master_list(self):
        
        iifl_instruments = {}
        
        for attempt in range(2):
            try:
                url = "https://ttblaze.iifl.com/apimarketdata/instruments/master"
                NSECM_payload = json.dumps({
                    "exchangeSegmentList": [
                    "NSECM"
                ]})
                NSEFO_payload = json.dumps({
                    "exchangeSegmentList": [
                    "NSEFO"
                ]})
                BSECM_payload = json.dumps({
                    "exchangeSegmentList": [
                    "BSECM"
                ]})
                BSEFO_payload = json.dumps({
                    "exchangeSegmentList": [
                    "BSEFO"
                ]})
                headers = {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
                
                # Fetch NSECM data
                NSECM_result                         = requests.request("POST", url, headers = headers, data = NSECM_payload)
                NSECM_substring                      = str(NSECM_result.text)[str(NSECM_result.text).find('result') + 9 : str(NSECM_result.text).find('}')].replace('\'', '').replace('|', ',')
                NSECM_substring                      = NSECM_substring.replace(r'"', '').strip().split(r'\n')
                NSECM_data                           = [r.split(",") for r in NSECM_substring]
                NSECM_df                             = pd.DataFrame(NSECM_data, columns=['ExchangeSegment', 'ExchangeInstrumentID', 'InstrumentType', 'Name', 'Description', 'Series', 'NameWithSeries', 'InstrumentID', 'PriceBand.High', 'PriceBand.Low', 'FreezeQty', 'TickSize', 'LotSize', 'Multiplier','UnderlyingInstrumentId', 'UnderlyingIndexName', 'ContractExpiration', 'StrikePrice', 'OptionType', 'temp1', 'temp2', 'temp3'])
                NSECM_df['UnderlyingInstrumentId']   = np.nan
                NSECM_df['UnderlyingIndexName']      = np.nan
                NSECM_df['ContractExpiration']       = np.nan
                NSECM_df['StrikePrice']              = np.nan
                NSECM_df['OptionType']               = np.nan
                
                # Fetch NSEFO data
                NSEFO_result                         = requests.request("POST", url, headers = headers, data = NSEFO_payload)
                NSEFO_substring                      = str(NSEFO_result.text)[str(NSEFO_result.text).find('result') + 9 : str(NSEFO_result.text).find('}')].replace('\'', '').replace('|', ',').replace('"', '')
                NSEFO_substring                      = NSEFO_substring.strip().split(r'\n')
                NSEFO_data                           = [r.split(",") for r in NSEFO_substring]
                NSEFO_df                             = pd.DataFrame(NSEFO_data, columns = ['ExchangeSegment', 'ExchangeInstrumentID', 'InstrumentType', 'Name', 'Description', 'Series', ' NameWithSeries', 'InstrumentID', 'PriceBand.High', 'PriceBand.Low', 'FreezeQty', 'TickSize', 'LotSize', 'Multiplier', 'UnderlyingInstrumentId', 'UnderlyingIndexName', 'ContractExpiration', 'StrikePrice', 'OptionType','temp1','temp2','temp3','temp4'])
                NSEFO_df['ContractExpiration']       = pd.to_datetime(NSEFO_df['ContractExpiration'].astype(str, '%Y-%m-$dT%H:%M:%S')).dt.strftime('%Y-%m-%d')
                
                # Fetch BSECM data
                BSECM_result                         = requests.request("POST", url, headers=headers, data=BSECM_payload)
                BSECM_substring                      = str(BSECM_result.text)[str(BSECM_result.text).find('result') + 9 : str(BSECM_result.text).find('}')].replace('\'', '').replace('|', ',')
                BSECM_substring                      = BSECM_substring.replace(r'"', '').strip().split(r'\n')
                BSECM_data                           = [r.split(",") for r in BSECM_substring]
                BSECM_df                             = pd.DataFrame(BSECM_data, columns=['ExchangeSegment', 'ExchangeInstrumentID', 'InstrumentType', 'Name', 'Description', 'Series', 'NameWithSeries', 'InstrumentID', 'PriceBand.High', 'PriceBand.Low', 'FreezeQty', 'TickSize', 'LotSize', 'Multiplier','UnderlyingInstrumentId', 'UnderlyingIndexName', 'ContractExpiration', 'StrikePrice', 'OptionType', 'temp1', 'temp2', 'temp3'])
                BSECM_df['UnderlyingInstrumentId']   = np.nan
                BSECM_df['UnderlyingIndexName']      = np.nan
                BSECM_df['ContractExpiration']       = np.nan
                BSECM_df['StrikePrice']              = np.nan
                BSECM_df['OptionType']               = np.nan
                
                # Fetch BSEFO data
                BSEFO_result                         = requests.request("POST", url, headers=headers, data=BSEFO_payload)
                BSEFO_substring                      = str(BSEFO_result.text)[str(BSEFO_result.text).find('result') + 9 : str(BSEFO_result.text).find('}')].replace('\'', '').replace('|', ',').replace('"', '')
                BSEFO_substring                      = BSEFO_substring.strip().split(r'\n')
                BSEFO_data                           = [r.split(",") for r in BSEFO_substring]
                BSEFO_df                             = pd.DataFrame(BSEFO_data, columns=['ExchangeSegment', 'ExchangeInstrumentID', 'InstrumentType', 'Name', 'Description', 'Series', 'NameWithSeries', 'InstrumentID', 'PriceBand.High', 'PriceBand.Low', 'FreezeQty', 'TickSize', 'LotSize', 'Multiplier', 'UnderlyingInstrumentId', 'UnderlyingIndexName', 'ContractExpiration', 'StrikePrice', 'OptionType', 'temp1', 'temp2', 'temp3', 'temp4'])
                BSEFO_df['ContractExpiration']       = pd.to_datetime(BSEFO_df['ContractExpiration'].astype(str, '%Y-%m-$dT%H:%M:%S')).dt.strftime('%Y-%m-%d')
                
                iifl_instruments                = pd.concat([NSECM_df, NSEFO_df, BSECM_df, BSEFO_df])
                iifl_instruments['StrikePrice'] = pd.to_numeric(iifl_instruments['StrikePrice'], errors = 'coerce')
                iifl_instruments['Series']      = iifl_instruments['Series'].str.replace('IO', 'OPTIDX')
                iifl_instruments['Series']      = iifl_instruments['Series'].str.replace('IF', 'FUTIDX')
                
            except Exception as e:
                mainlogger.exception('Error downloading IIFL instruments ' + str(e))
                send_notification(group_chat_id, emoji_warning + ' Failed to load IIFL instruments')
                mainlogger.exception('IIFL instruments load failed')
                time.sleep(5)
            else:
                break
            
        return iifl_instruments


    def account_login(self):
        mainlogger.info('Function : iiflBroker login')
        
        global pool
        global dic_accounts
        global iifl_instruments
        
        broker_id = iifl_broker_code
        
        coredb = pool.connection()
        sql    = "select * from vw_activeaccounts where broker = " + str(broker_id)
        df     = pd.read_sql(sql, coredb)
        df     = df.replace({np.nan: None})
        
        for index, row in df.iterrows():
            for attempt in range(2):
                app_id     = json.loads(row['app_id'])
                api_secret = json.loads(row['api_secret'])
                try:
                    interactive_app_id     = app_id['interactive']
                    interactive_api_secret = api_secret['interactive']
                    
                    market_app_id     = app_id['market']
                    market_api_secret = api_secret['market']
                    
                    interactive_xt = XTSConnect(interactive_app_id, interactive_api_secret, "WEBAPI")
                    market_xt      = XTSConnect(market_app_id, market_api_secret, "WEBAPI")
                    
                    interactive_xt.interactive_login()
                    market_xt.marketdata_login()
                    
                    interactive_profile = interactive_xt.get_profile()
                    market_userID       = market_xt.userID
                    
                    #mainlogger.exception("iifl xxxx"+str(interactive_profile))
                    if interactive_profile == None or (isinstance(interactive_profile, str)):
                        raise Exception(str(interactive_profile))
                    elif not isinstance(interactive_profile, str) and interactive_profile['type'] == "error":
                        raise Exception(str(interactive_profile['description']))
                    
                    if market_userID  is None:
                        raise Exception("Market API login failed")
                    
                    
                    dic_accounts[row['ID']] = {'id':row['ID'], 'username':row['username'], 'token':{"interactive": interactive_xt, "market": market_xt}, 'chat_id':row['tele_chat_id'], 'broker':row['broker'], 'lock':MyLock(), 'token_refresh':datetime.datetime.now() - timedelta(minutes = 5)}
                    mainlogger.info('Interactive Profile :' + str(interactive_profile))
                    mainlogger.info('market_userID :' + str(market_userID))
                    reset_login_failed_accounts(row['ID']) #create a dummy function
                    
                    if iifl_instruments is None or len(iifl_instruments) == 0:
                        iifl_instruments = self.get_master_list()
                        mainlogger.info('loaded IIFL instruments')
                        send_notification(group_chat_id, 'Loaded IIFL instruments ' + str(len(iifl_instruments)))
                except Exception as e:
                    try:
                        dic_accounts[row['ID']] = {'id':row['ID'],'username':row['username'], 'token':None, 'chat_id':row['tele_chat_id'], 'broker':row['broker'], 'lock':MyLock(), 'token_refresh':datetime.datetime.now() - timedelta(minutes = 5)}
                        handle_exception('iifl Login Failed ' + row['username'], str(e), row['tele_chat_id'], True) 
                        increment_login_failed_accounts(row['ID'])
                        time.sleep(1)
                    except Exception as e:
                        mainlogger.exception(str(e))
                else:
                    break
        coredb.close()


    ##Single LOGIN to Accounts
    def account_single_login(self, acc_id):
        mainlogger.info('Function : iiflBroker login')
        
        global pool
        global dic_accounts
        global iifl_instruments
        
        broker_id = iifl_broker_code
        
        coredb = pool.connection()
        sql = "select * from vw_activeaccounts where broker = " + str(broker_id) + " and ID = " + str(acc_id)
        df  = pd.read_sql(sql, coredb)
        df  = df.replace({np.nan: None})
        
        for index, row in df.iterrows():
            for attempt in range(2):
                app_id = json.loads(row['app_id'])
                api_secret = json.loads(row['api_secret'])
                
                try:
                    interactive_app_id     = app_id['interactive']
                    interactive_api_secret = api_secret['interactive']
                    
                    market_app_id     = app_id['market']
                    market_api_secret = api_secret['market']
                    
                    interactive_xt = XTSConnect(interactive_app_id, interactive_api_secret, "WEBAPI")
                    market_xt      = XTSConnect(market_app_id, market_api_secret, "WEBAPI")
                    
                    interactive_xt.interactive_login()
                    market_xt.marketdata_login()
                    
                    interactive_profile = interactive_xt.get_profile()
                    market_userID       = market_xt.userID
                    
                    #mainlogger.exception("iifl xxxx"+str(interactive_profile))
                    if interactive_profile == None or (isinstance(interactive_profile, str)):
                        raise Exception(str(interactive_profile))
                    elif not isinstance(interactive_profile, str) and interactive_profile['type'] == "error":
                        raise Exception(str(interactive_profile['description']))
                        
                    if market_userID is None:
                        raise Exception("Market API login failed")
                    
                    dic_accounts[row['ID']] = {'id':row['ID'], 'username':row['username'], 'token':{"interactive": interactive_xt, "market": market_xt}, 'chat_id':row['tele_chat_id'], 'broker':row['broker'], 'lock':MyLock(), 'token_refresh':datetime.datetime.now() - timedelta(minutes = 5)}
                    mainlogger.info('Interactive Profile :' + str(interactive_profile))
                    mainlogger.info('market_userID :' + str(market_userID))
                    
                    reset_login_failed_accounts(row['ID'])
                    reset_login_failed_accounts(row['ID']) #create a dummy function
                    
                    if iifl_instruments is None or len(iifl_instruments) == 0:
                        iifl_instruments = self.get_master_list()
                        mainlogger.info('loaded IIFL instruments')
                        send_notification(group_chat_id, 'Loaded IIFL instruments ' + str(len(iifl_instruments)))
                except Exception as e:
                    try:
                        dic_accounts[row['ID']] = {'id':row['ID'], 'username':row['username'], 'token':None, 'chat_id':row['tele_chat_id'], 'broker':row['broker'], 'lock':MyLock(), 'token_refresh':datetime.datetime.now() - timedelta(minutes = 5)}
                        handle_exception('iifl Login Failed ' + row['username'], str(e), row['tele_chat_id'], True) 
                        increment_login_failed_accounts(row['ID'])
                        time.sleep(1)
                    except Exception as e:
                        mainlogger.exception("Exception while sleeping :: " + str(e))
                else:
                    break
        coredb.close()


    def get_instrument_by_symbol(self, exchange, symbol):
        mainlogger.info('Function : iifl get_instrument_by_symbol')
        if exchange   == 'NSE':
            instrument = iifl_instruments.loc[((iifl_instruments['ExchangeSegment'] == "NSECM") & (iifl_instruments['Name'] == symbol))].iloc[0]
        elif exchange == 'NFO':
            instrument = iifl_instruments.loc[((iifl_instruments['ExchangeSegment'] == "NSEFO") & (iifl_instruments['Description'] == symbol))].iloc[0]
        elif exchange == 'BSE':
            instrument = iifl_instruments.loc[((iifl_instruments['ExchangeSegment'] == "BSECM") & (iifl_instruments['Description'] == symbol))].iloc[0]
        elif exchange == 'BFO':
            instrument = iifl_instruments.loc[((iifl_instruments['ExchangeSegment'] == "BSEFO") & (iifl_instruments['Description'] == symbol))].iloc[0]
        
        instrument = Instrument(exchange = instrument['ExchangeSegment'], token = instrument['ExchangeInstrumentID'], symbol = instrument['Description'], name = instrument['Name'], expiry = instrument['ContractExpiration'], lot_size = instrument['LotSize'])
        return instrument
        
        
    def custom_get_instrument_for_fno(self, symbol, exp, is_fut, strike, is_CE, segment):
        mainlogger.info('Function : iifl custom_get_instrument_for_fno')
        strike = float(strike)
        expiry = datetime.datetime.strptime(exp, '%d %b%y').date().strftime('%Y-%m-%d')
        instrument_type = '3' if is_CE == True else '4' # CE = 3, PE = 4
        mainlogger.info("custom_get_instrument_for_fno : length of iifl_instruments: " + str(len(iifl_instruments)))
        
        if segment == 'BFO':
            segment = 'BSEFO' if segment == 'BFO' else 'BSECM'
        else:
            segment = 'NSEFO' if segment == 'NFO' else 'NSECM'
        
        if is_fut == False:
            instrument = iifl_instruments.loc[((iifl_instruments['ContractExpiration']      == expiry)
                                               & (iifl_instruments['Name']                  == symbol)
                                               & (iifl_instruments['ExchangeSegment']  == segment)
                                               & (iifl_instruments['OptionType']            == instrument_type)
                                               & (iifl_instruments['StrikePrice']           == strike)
                                               & (iifl_instruments['Series'].str.startswith('OPT'))
                                               )].iloc[0]
            instrument = Instrument(exchange = instrument['ExchangeSegment'],
                                    token    = instrument['ExchangeInstrumentID'],
                                    symbol   = instrument['Description'],
                                    name     = instrument['Name'],
                                    expiry   = instrument['ContractExpiration'],
                                    lot_size = instrument['LotSize']
                                    )
            return instrument
        elif is_fut == True:
            instrument = iifl_instruments.loc[((iifl_instruments['ContractExpiration']      == expiry) 
                                               & (iifl_instruments['Name']                  == symbol) 
                                               & (iifl_instruments['ExchangeSegment']  == segment) 
                                               & (iifl_instruments['Series'].str.startswith('FUT'))
                                               )].iloc[0]
            
            instrument = Instrument(exchange = instrument['ExchangeSegment'],
                                    token    = instrument['ExchangeInstrumentID'],
                                    symbol   = instrument['Description'],
                                    name     = instrument['Name'],
                                    expiry   = instrument['ContractExpiration'],
                                    lot_size = instrument['LotSize']
                                    )
            return instrument


    def get_ltp(self, iifl, exchange, symbol):
        mainlogger.info('Function : iifl get_ltp')
        try:
            xtsMessageCode = 1502
            publishFormat  = 'JSON'
            instrument     = self.get_instrument_by_symbol(exchange, symbol)
        
            if instrument.exchange   == "BSEFO":
                exchange = 12
            elif instrument.exchange == "NSEFO":
                exchange = 2
        
            instruments   = [{'exchangeSegment': exchange, 'exchangeInstrumentID': instrument.token}]
            response      = iifl['market'].get_quote(Instruments = instruments, xtsMessageCode = xtsMessageCode, publishFormat = publishFormat)
            json_response = json.loads(response['result']['listQuotes'][0])
            return json_response['Touchline']['LastTradedPrice']
        except Exception as e:
            mainlogger.exception(str(e))
            return -3


    def punch_order(self, acc_id, iifl, iifl_instruments, symbol, direction, qty, triggerPrice, price, 
                order_type = 'MARKET', misFlag = 'NRML', amoFlag = False, exchange = 'NFO',  strategyName = 'NA'):
        mainlogger.info('Function : iifl punch_order')
        response    = None
        try:  
            tx_type = 'BUY' if (direction == 'buy' or  direction == 'b') else 'SELL'
            if order_type == 'MARKET':
                order_type     = 'MARKET'
                price          = 0.0
                trigger_price  = 0.0

            elif order_type == 'SLM':
                order_type     = 'STOPMARKET'
                price          = 0.0
                trigger_price  = triggerPrice

            elif order_type == 'SLL':
                order_type     = 'STOPLIMIT'
                price          = price
                trigger_price  = triggerPrice

            elif order_type == 'LIMIT':
                order_type     = 'LIMIT'
                
                if price == 0.0:
                    price      = float(self.get_ltp(iifl, exchange, symbol))
                
                    if price is not None or price != -3 or price != 0:
                        if tx_type == 'SELL':
                            price  = round(price * limit_price_factor_sell, 1)
                        else:
                            price  = round(price * limit_price_factor_buy, 1)
                    else:
                        order_type = 'MARKET'
                        price      = 0.0

                else:
                    order_type = 'MARKET'
                    price      = 0.0
                
                if round(float(price),1)==0:
                    order_type = 'MARKET'
                    price      = 0.0
                    
                trigger_price = 0.0

            ##MIS or NORMAL
            if exchange in ('NFO', 'BFO') and misFlag == 'NRML':
                prod_type = 'NRML'
                
            elif exchange in ('NFO', 'BFO') and misFlag == 'MIS':
                prod_type = 'MIS'
            else:
                prod_type = 'CNC'
             
            ordertag=order_tag_prefix+'_'+str(random.randrange(1000,10000000))

            
            instrument           = self.get_instrument_by_symbol(exchange, symbol)
            exchangeInstrumentID = instrument.token
            exchangeSegment      = instrument.exchange
            username             = dic_accounts[acc_id].get('username')
            for attempt in range(2):
                try:
                    place_order_status = iifl['interactive'].place_order(
                        exchangeSegment       = exchangeSegment,
                        exchangeInstrumentID  = exchangeInstrumentID,
                        productType           = prod_type,
                        orderType             = order_type,
                        orderSide             = tx_type,
                        timeInForce           = iifl['interactive'].VALIDITY_DAY,
                        disclosedQuantity     = 0,
                        orderQuantity         = int(qty),
                        limitPrice            = round(float(price),1),
                        stopPrice             = round(float(trigger_price),1),
                        clientID              = username,
                        isAMO                 = amoFlag,
                        orderUniqueIdentifier = ordertag
                     )
                    if place_order_status['type'] == 'error':
                        raise Exception("order placement failed: Broker API returned ", place_order_status['description'])
                    else:
                        order_id = place_order_status['result']['AppOrderID']
                        response = {'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id, 'average_price': price}}     
                        #return response
                except Exception as e:
                    mainlogger.exception("Order placement failed: " + str(e))
                    try:
                        response = {'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}     

                        #check if Order Got accepted before retrying order to avoid duplication
                        order = broker.get_order_by_tag(acc_id, ordertag)

                        if  order==-2:
                            response={'status': 'rejected', 'message': 'Unable to verify if previous order api was successful. Please verify in broker terminal manually and retry if needed.', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id,'average_price':price}}     
                            return response
                        
                        elif  order!=-1 and order!=-2:
                            response = {'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order['oms_order_id'], 'average_price':order['average_price']}}     
                            return response

                        time.sleep(5)
                        self.check_login_token(acc_id)
                        iifl = dic_accounts[acc_id]['token']

                    except Exception as e:
                        mainlogger.exception("Punch order failed while retrying: " + str(e))
                else:
                    break
            return response
        except Exception as e:
            mainlogger.exception(str(e))


    def modify_order(self, acc_id, iifl, iifl_instruments, exchange, direction, symbol, order_id, qty, prod_type, order_type, price, triggerPrice,ordertag='NA'):
        mainlogger.info('Function : iifl modify_order')
        try:
            if order_type == 'MARKET':
                order_type    = 'MARKET'
                price         = 0.0
                trigger_price = 0.0
            
            elif order_type == 'SLM':
                order_type    = 'STOPMARKET'
                price         = 0.0
                trigger_price = triggerPrice
            
            elif order_type == 'SLL':
                order_type    = 'STOPLIMIT'
                price         = price
                trigger_price = triggerPrice
            
            elif order_type == 'LIMIT':
                price         = price
                trigger_price = 0.0
            
            # Commenting as this is not needed
            #order_history = self.get_order_history(acc_id, int(order_id))
            
            
            for attempt in range(2):
                try:
                    modify_order_status = iifl['interactive'].modify_order(
                        appOrderID                = int(order_id),
                        modifiedProductType       = prod_type,
                        modifiedOrderType         = order_type,
                        modifiedOrderQuantity     = int(qty),
                        modifiedDisclosedQuantity = 0,
                        modifiedLimitPrice        = float(price),
                        modifiedStopPrice         = float(trigger_price),
                        modifiedTimeInForce       = iifl['interactive'].VALIDITY_DAY,
                        orderUniqueIdentifier     = ordertag,
                        clientID                  = dic_accounts[acc_id].get('username')#,
                        #isAMO                     = False #remove later
                    )
                    if modify_order_status['type'] == 'error':
                        raise Exception("order exit failed: Broker API returned " +str(modify_order_status))
                    
                    order_id = modify_order_status['type']
                    response = {'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id}}     
                except Exception as e:
                    mainlogger.exception("Order modify failed: " + str(e))
                    response = {'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': order_id}}     
                    time.sleep(5)
                    self.check_login_token(acc_id)
                    iifl = dic_accounts[acc_id]['token']
                else:
                    break
            
            return response
        except Exception as e:
            mainlogger.exception(str(e))


    def exit_order(self, acc_id, iifl, iifl_instruments, exchange, direction, symbol, order_id, qty, prod_type, order_type='MARKET', price=0.0, ordertag='NA'):
        mainlogger.info('Function : iifl exit_order')
        try:
            exit_order_status = None
            tx_type = 'BUY' if (direction == 'buy' or  direction == 'b') else 'SELL'

            try:
                if order_type == 'MARKET':
                    order_type    = 'MARKET'
                    price         = 0.0
                    trigger_price = 0.0
                    
                elif order_type == 'LIMIT':

                    if price == 0.0:
                        price = float(self.get_ltp(iifl, exchange, symbol))
                        #price=float(self.get_ltp(iifl, exchange, symbol))
                
                    if price is not None or price != -3 or price != 0:
                        if tx_type == 'SELL':
                            price = round(price * limit_price_factor_sell, 1)
                        else:
                            price = round(price * limit_price_factor_buy, 1)
                    else:
                        order_type = 'MARKET'
                        price      = 0.0
                    
                    if round(float(price),1)==0:
                        order_type = 'MARKET'
                        price      = 0.0
                    trigger_price = 0.0
                
                # Commenting as this is not needed
                #order_history = self.get_order_history(acc_id, int(order_id))
                for attempt in range(2):
                    try:
                        exit_order_status = iifl['interactive'].modify_order(
                            appOrderID                = int(order_id),
                            modifiedProductType       = prod_type,
                            modifiedOrderType         = order_type,
                            modifiedOrderQuantity     = int(qty),
                            modifiedDisclosedQuantity = 0,
                            modifiedLimitPrice        = float(price),
                            modifiedStopPrice         = float(trigger_price),
                            modifiedTimeInForce       = iifl['interactive'].VALIDITY_DAY,
                            orderUniqueIdentifier     = ordertag,
                            clientID                  = dic_accounts[acc_id].get('username') #order_history['ClientID']#,
                            #isAMO                     = False #remove late
                        )
                        if exit_order_status['type'] == 'error':
                            raise Exception("order exit failed: Broker API returned " +str(exit_order_status))
                        response = {'status': 'success', 'message': 'Order placed successfully', 'data': {'oms_order_id': order_id}}     
                    except Exception as e:
                        mainlogger.exception("Order exit failed: " + str(e))
                        response = {'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': order_id}}     
                        time.sleep(5)
                        self.check_login_token(acc_id)
                        iifl = dic_accounts[acc_id]['token']
                    else:
                        break
                return response

            except Exception as e:
                mainlogger.exception("Order exit failed: " + str(e))
                response = {'status': 'rejected', 'message': str(e), 'data': {'oms_order_id': order_id}}     
                return response
        
        except Exception as e:
            mainlogger.exception(str(e))


    def get_order_history(self, acc_id, order_id):
        mainlogger.info('Function: IIFL get_order_history: acc_id: ' + str(acc_id) + ", order_id: " + str(order_id))
        if order_id == zerodha_failed_order_hardcoded_id:
            tmp={'order_status': 'rejected', 'average_price':0, 'filled_quantity':0, 'message': 'NA', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}
            mainlogger.info('IIFL get_order_history order_id == zerodha_failed_order_hardcoded_id: acc_id: ' + str(acc_id) + ", order_id: " + str(order_id) + ": " + str(tmp))
            return tmp
        
        for attempt in range(2):
            try:
                token = dic_accounts[acc_id]['token']['interactive']
                tmp   = token.get_order_history(int(order_id))
                mainlogger.info("RAW get_order_history:")
                mainlogger.info(str(tmp))
                latest_state_index = (len(tmp['result']) - 1)
                tmp                     = tmp['result'][latest_state_index]
                tmp['oms_order_id']     = tmp['AppOrderID']
                tmp['order_status']     = tmp['OrderStatus'].lower().replace('filled', 'complete').replace('new', 'trigger pending')
                tmp['price_to_fill']    = float(tmp['OrderPrice'])
                tmp['transaction_type'] = tmp['OrderSide']
                tmp['rejection_reason'] = tmp['CancelRejectReason']
                tmp['order_type']       = tmp['OrderType']
                tmp['product']          = tmp['ProductType']
                tmp['exchange']         = tmp['ExchangeSegment']

                try:
                    tmp['order_tag'] = tmp['OrderUniqueIdentifier']
                except:
                    tmp['order_tag'] = None

                try:
                    tmp['average_price'] = float(tmp['OrderAverageTradedPrice'])
                except Exception as e:
                    tmp['average_price'] = None

                try:
                    tmp['trigger_price'] = float(tmp['OrderStopPrice'])
                except Exception as e:
                    tmp['trigger_price'] = None

                try:
                    tmp['filled_quantity'] = int(tmp['OrderQuantity'])
                except Exception as e:
                    tmp['filled_quantity'] = None
            
                try:
                    tmp['quantity']        = int(tmp['OrderQuantity'])
                except:
                    tmp['quantity']        = None

                mainlogger.info("IIFL get_order_history acc_id : " + str(acc_id) + ", order_id: " + str(order_id) + ": formatted: " + str(tmp))
                return tmp
            except Exception as te:
                mainlogger.exception('Function : get_order_history ' + str(te))
                time.sleep(2)
                tmp = {'order_status': 'not_found', 'average_price': 0, 'filled_quantity': 0, 'message': 'order not found/api error', 'data': {'oms_order_id': zerodha_failed_order_hardcoded_id}}
                mainlogger.info("IIFL get_order_history exception: acc_id: " + str(acc_id) + ", order_id: " + str(order_id) + ": " + str(tmp))

            else:
                break
            
        return tmp


    def get_all_order_history(self,acc_id):
        mainlogger.info('Function : IIFL get_all_order_history: acc_id: ' + str(acc_id))
        
        orders=[]
        
        for tmp in dic_accounts[acc_id]['token']['interactive'].get_order_book()['result']:
            try:
                tmp['oms_order_id']     = tmp['AppOrderID']
                tmp['order_status']     = tmp['OrderStatus'].lower().replace('cancel','cancelled').replace('filled', 'complete').replace('new', 'trigger pending')
                tmp['price_to_fill']    = float(tmp['OrderPrice'])
                tmp['transaction_type'] = tmp['OrderSide']
                tmp['order_type']       = tmp['OrderType']
                tmp['product']          = tmp['ProductType']
                tmp['order_tag']        = tmp['OrderUniqueIdentifier']
                tmp['exchange_time']    = int(round(datetime.datetime.strptime(tmp['ExchangeTransactTime'], '%d-%m-%Y %H:%M:%S').timestamp()))
                tmp['rejection_reason'] = tmp['CancelRejectReason']
                tmp['exchange']         = tmp['ExchangeSegment']
                try:
                    tmp['average_price'] = float(tmp['OrderAverageTradedPrice'])
                except Exception as e:
                    tmp['average_price'] = None

                try:
                    tmp['trigger_price'] = float(tmp['OrderStopPrice'])
                except Exception as e:
                    tmp['trigger_price'] = None

                try:
                    tmp['filled_quantity'] = int(tmp['OrderQuantity'])
                    tmp['quantity']        = int(tmp['OrderQuantity'])
                except Exception as e:
                    try:
                        tmp['filled_quantity'] = int(tmp['OrderQuantity'])
                        tmp['quantity']        = int(tmp['OrderQuantity'])
                    except Exception as e:
                        tmp['filled_quantity'] = None
                        tmp['quantity']        = None

                #mainlogger.info("IIFL get_all_order_history: acc_id: " + str(acc_id) + ": " + str(tmp))
                orders.append(tmp)
            except:
                pass

        return orders


    def check_if_position_open(self, acc_id, iifl, exchange, symbol, qty, direction):
        mainlogger.info('Function : IIFL check_if_position_open')
        
        try:
            clientID   = dic_accounts[acc_id].get('username')
            trade_book = iifl['interactive'].get_position_daywise(clientID)
            
            if trade_book is None or len(trade_book)==0:
                mainlogger.exception("IIFL Retruned Empty Position Book")
                return True, 1000

            token      = self.get_instrument_by_symbol(exchange, symbol).token
            
            if exchange == "NSE":
                exchange = "NSECM" 
            else:
                exchange = "NSEFO"

            for t in trade_book['result']['positionList']:
                if direction.upper() == 'BUY' and t['ExchangeSegment'] == exchange and (t['ExchangeInstrumentId']) == token and int(t['Quantity']) < 0:
                    return True, abs(int(t['Quantity']))
                elif direction.upper() == 'SELL' and t['ExchangeSegment'] == exchange and (t['ExchangeInstrumentId']) == token and int(t['Quantity']) > 0:
                    return True, abs(int(t['Quantity']))
            return False, 0
        
        except Exception as e:
            mainlogger.exception(str(e))
            return True,10000


    def tradingAccount_PNLSummary(self, acc_id):
        mainlogger.info('Function : iifl tradingAccount_PNLSummary')
        
        clientID  = dic_accounts[acc_id].get('username')
        positions = dic_accounts[acc_id]['token']['interactive'].get_position_daywise(clientID)

        totalRealizedPNL            = 0
        totalUnRealizedPNL          = 0

        maxrealizedLossPosition     = 0
        maxrealizedPoriftPosition   = 0

        maxUnrealizedLossPosition   = 0
        maxUnrealizedProfitPosition = 0

        if positions != None and "error" not in str(positions).lower():
            for p in positions['result']['positionList']:
                if int(p['Quantity']) == 0:
                    totalRealizedPNL   = totalRealizedPNL + float(p['NetAmount'])
                    if float(p['NetAmount']) > maxrealizedPoriftPosition:
                        maxrealizedPoriftPosition = float(p['NetAmount'])
                    
                    if float(p['NetAmount']) < 0 and abs(float(p['NetAmount'])) > maxrealizedLossPosition:
                        maxrealizedLossPosition = abs(float(p['NetAmount']))
                else:
                    totalUnRealizedPNL = totalUnRealizedPNL + float(p['NetAmount'])
                    if float(p['NetAmount']) > maxUnrealizedProfitPosition:
                        maxUnrealizedProfitPosition = float(p['NetAmount'])
                    
                    if float(p['NetAmount']) < 0 and abs(float(p['NetAmount'])) > maxUnrealizedLossPosition:
                        maxUnrealizedLossPosition = abs(float(p['NetAmount']))

        pnlSummary = {
            "totalRealizedPNL":            round(totalRealizedPNL, 1),
            "totalUnRealizedPNL":          round(totalUnRealizedPNL, 1),
            "maxrealizedPoriftPosition":   round(maxrealizedPoriftPosition, 1),
            "maxrealizedLossPosition":     round(maxrealizedLossPosition, 1) * -1,
            "maxUnrealizedProfitPosition": round(maxUnrealizedProfitPosition, 1),
            "maxUnrealizedLossPosition":   round(maxUnrealizedLossPosition, 1) * -1
        }

        return pnlSummary


# # Generic Broker Class

# %%
class FlattradeBroker:
    #flattrade_instruments = {}  # instruments as pd df - NSE, NFO, BSE and BFO

    # TODO: Done - Has Commented code
    def check_login_token(self, acc_id):
        mainlogger.info("Function : FlattradeBroker check_login_token")
        mainlogger.info(str(dic_accounts[acc_id]))
        dic_accounts[acc_id]["lock"].acquire(
            thread_lock_timeout
        )

        # TODO : Check with Client
        if (
            datetime.datetime.now() - dic_accounts[acc_id]["token_refresh"]
        ).total_seconds() < 5:
            if dic_accounts[acc_id]["lock"].locked() == True:
                dic_accounts[acc_id]["lock"].release()
            mainlogger.info(str(dic_accounts[acc_id]))
            return 1
        try:
            if dic_accounts[acc_id]["token"] is None:
                self.account_single_login(acc_id)
                dic_accounts[acc_id]["token_refresh"] = datetime.datetime.now()
                if dic_accounts[acc_id]["lock"].locked() == True:
                    dic_accounts[acc_id]["lock"].release()
                mainlogger.info(str(dic_accounts[acc_id]))

            elif dic_accounts[acc_id]["token"].get_holdings() != None:
                dic_accounts[acc_id]["token_refresh"] = datetime.datetime.now()
                if dic_accounts[acc_id]["lock"].locked() == True:
                    dic_accounts[acc_id]["lock"].release()
                mainlogger.info(str(dic_accounts[acc_id]))

            elif dic_accounts[acc_id]["token"].get_holdings() is None:
                self.account_single_login(acc_id)
                dic_accounts[acc_id]["token_refresh"] = datetime.datetime.now()
                if dic_accounts[acc_id]["lock"].locked() == True:
                    dic_accounts[acc_id]["lock"].release()
                mainlogger.info(str(dic_accounts[acc_id]))

        except Exception as e:
            mainlogger.exception(str(e))
            if dic_accounts[acc_id]["lock"].locked() == True:
                dic_accounts[acc_id]["lock"].release()
            mainlogger.info(str(dic_accounts[acc_id]))

    # TODO: Done
    def get_instrument_data(self):
        pd_master = pd.DataFrame()
        mainlogger.info("Function : get_nse_nfo_master_list Flattrade start")
        try:
            root = "https://flattrade.s3.ap-south-1.amazonaws.com/scripmaster/"
            masters = [
                "NSE_Equity.csv",
                "Nfo_Equity_Derivatives.csv",
                "Nfo_Index_Derivatives.csv",
                "Currency_Derivatives.csv",
                "Commodity.csv",
                "BSE_Equity.csv",
                "Bfo_Index_Derivatives.csv",
                "Bfo_Equity_Derivatives.csv",
            ]
            for file in masters:
                url = root + file
                df = pd.read_csv(url)
                pd_master = pd.concat([pd_master, df])

            # handle weird symbol names from flattrade
            try:
                pd_master["Symbol"].replace("SX50OPT", "SENSEX50", inplace=True)
                pd_master["Symbol"].replace("BSXOPT", "SENSEX", inplace=True)
                pd_master["Symbol"].replace("BKXOPT", "BANKEX", inplace=True)
            except:
                pass

            mainlogger.info("Function : get_nse_nfo_master_list Flattrade end")
            return pd_master
        except Exception as e:
            mainlogger.exception(
                "Flattrade get_nse_nfo_master_list failed : " + str(e)
            )
            return None

    # TODO: Done
    def get_master_list(self):
        mainlogger.info("Function : get_master_list Flattrade start")
        
        global flattrade_instruments
        
        if (
            flattrade_instruments is not None
            and len(flattrade_instruments) != 0
        ):
            mainlogger.info(
                "Function : get_master_list : Flattrade Master list already fetched"
            )
            return
        retry_count = 2
        flattrade_instruments = {}
        for attempt in range(retry_count):
            try:
                # flattrade_instruments = pd.concat([self.get_nse_master_list(), self.get_nfo_master_list()])
                flattrade_instruments = self.get_instrument_data()
                if len(flattrade_instruments) != 0:
                    mainlogger.info("Flattrade instruments load successfully")
                    send_notification(
                        group_chat_id,
                        "Loaded flattrade instruments "
                        + str(len(flattrade_instruments)),
                    )
                else:
                    raise Exception("Flattrade instruments load failed")
            except Exception as e:
                if attempt == retry_count:
                    # handle_exception('Flattrade instruments failed to load ' + row['username'], str(e), row['tele_chat_id'], True)
                    send_notification(
                        group_chat_id,
                        emoji_warning
                        + "Failed to load Flattrade instruments",
                    )
                    mainlogger.exception(
                        "Flattrade instruments load failed : " + str(e)
                    )
                time.sleep(5)
            else:
                break
        mainlogger.info("Function : get_master_list Flattrade end")
        return

    # TODO: Done - Has Commented code
    ##Single LOGIN to Accounts
    def account_single_login(self, acc_id):
        mainlogger.info("Function : FlattradeBroker single login")
        cursor = None
        connection = None
        try:
            coredb = pool.connection()
            sql = "SELECT * FROM vw_activeaccounts WHERE ID=" + str(acc_id)
            df = pd.read_sql(sql, coredb)
            df = df.replace({np.nan: None})
            for _, row in df.iterrows():
                for _ in range(2):
                    try:
                        norenApi = NorenApiFT(
                            host="https://piconnect.flattrade.in/PiConnectTP/",
                            websocket="wss://piconnect.flattrade.in/PiConnectWSTp/",
                            #eodhost="https://web.flattrade.in/chartApi/getdata/",
                        )
                        usersession = asyncio.run(
                            get_session_token(
                                userid=row["username"],
                                password=str(row["password"].decode("ascii")),
                                totp=row["two_fa"],
                                apikey=row["app_id"],
                                apisecret=row["api_secret"],
                            )
                        )
                        r = norenApi.set_session(
                            userid=row["username"], password="", usertoken=usersession
                        )
                        if r is None or usersession is None or usersession=='':
                            print(1 / 0)

                        dic_accounts[row["ID"]] = {
                            "id": row["ID"],
                            "username": row["username"],
                            "token": norenApi,
                            "chat_id": row["tele_chat_id"],
                            "broker": row["broker"],
                            "lock": dic_accounts[row["ID"]]["lock"],
                            "token_refresh": datetime.datetime.now()
                            - timedelta(minutes=5),
                        }
                        mainlogger.info("Profile : " + str(norenApi.get_limits()))
                        
                        reset_login_failed_accounts(row["ID"])
                        self.get_master_list()
                    except Exception as e:
                        try:
                            dic_accounts[row["ID"]] = {
                                "id": row["ID"],
                                "username": row["username"],
                                "token": None,
                                "chat_id": row["tele_chat_id"],
                                "broker": row["broker"],
                                "lock": dic_accounts[row["ID"]]["lock"],
                                "token_refresh": datetime.datetime.now()
                                - timedelta(minutes=5),
                            }
                            # TODO: Check with client
                            handle_exception('Flattrade Login Failed ' + row['username'], str(e), row['tele_chat_id'], True)
                            time.sleep(1)
                        except Exception as e:
                            mainlogger.exception(str(e))
                    else:
                        break
        except Exception as e:
            mainlogger.exception("FlattradeBroker account_single_login : " + str(e))
        finally:
            mainlogger.info("Function : FlattradeBroker account_single_login end")
            #cursor, connection = ut.close_connection(cursor, connection)

    # TODO : Done - Has Commented code
    ##LOGIN to Accounts
    def account_login(self):
        mainlogger.info("Function : FlattradeBroker login")
        connection = None
        cursor = None
        broker_id=73
        sql = "select * from vw_activeaccounts where broker=" + str(broker_id)
        try:
            coredb = pool.connection()
            df = pd.read_sql(sql, coredb)
            df = df.replace({np.nan: None})
            for index, row in df.iterrows():
                for attempt in range(2):
                    try:
                        norenApi = NorenApiFT(
                            host="https://piconnect.flattrade.in/PiConnectTP/",
                            websocket="wss://piconnect.flattrade.in/PiConnectWSTp/",
                            #eodhost="https://web.flattrade.in/chartApi/getdata/",
                        )
                        #print(norenApi.__service_config)
                        usersession = asyncio.run(
                            get_session_token(
                                userid=row["username"],
                                password=str(row["password"].decode("ascii")),
                                totp=row["two_fa"],
                                apikey=row["app_id"],
                                apisecret=row["api_secret"],
                            )
                        )
                        r = norenApi.set_session(
                            userid=row["username"], password="", usertoken=usersession
                        )

                        if r is None:
                            print(1 / 0)

                        dic_accounts[row["ID"]] = {
                            "id": row["ID"],
                            "username": row["username"],
                            "token": norenApi,
                            "chat_id": row["tele_chat_id"],
                            "broker": row["broker"],
                            "lock": MyLock(),
                            "token_refresh": datetime.datetime.now()
                            - timedelta(minutes=5),
                        }
                        mainlogger.info("Profile : " + str(r))
                        # TODO: Check with client
                        reset_login_failed_accounts(row["ID"])
                        for tries in range(2):
                            try:
                                if (
                                    flattrade_instruments is None
                                    or len(flattrade_instruments) == 0
                                ):
                                    self.get_master_list()
                            except Exception:
                                time.sleep(5)
                                pass
                            else:
                                break
                    except Exception as e:
                        try:
                            dic_accounts[row["ID"]] = {
                                "id": row["ID"],
                                "username": row["username"],
                                "token": None,
                                "chat_id": row["tele_chat_id"],
                                "broker": row["broker"],
                                "lock": MyLock(),
                                "token_refresh": datetime.datetime.now()
                                - timedelta(minutes=5),
                            }
                            # TODO: Check with client
                            handle_exception(
                                 msg="Flattrade Login Failed " + row["username"],
                                 e=str(e),
                                 chat_id=row["tele_chat_id"],
                                 both=True,
                             )
                            increment_login_failed_accounts(row["ID"])
                            time.sleep(1)
                        except Exception as e:
                            mainlogger.exception(str(e))
                    else:
                        break
        except Exception as e:
            mainlogger.exception("FlattradeBroker account_login : " + str(e))
        finally:
            mainlogger.info("Function : FlattradeBroker account_login end")
            #coredb, connection = ut.close_connection(cursor, connection)

    # TODO: Done
    def custom_get_instrument_for_fno(
        self, symbol, exp, is_fut, strike, is_CE, segment="NFO"
    ):
        mainlogger.info("Function : flattrade custom_get_instrument_for_fno")

        expiry = (
            datetime.datetime.strptime(exp, "%d %b%y")
            .date()
            .strftime("%d-%b-%Y")
            .upper()
        )
        instrument_type = "CE" if is_CE == True else "PE"
        # segment='NFO-OPT' if is_fut==False else 'NFO-FUT'
        if segment == "BFO":
            segment = "BFO-OPT" if is_fut == False else "BFO-FUT"
        else:
            segment = "NFO-OPT" if is_fut == False else "NFO-FUT"

        strike = float(strike)
        if "OPT" in segment:
            instrument = flattrade_instruments.loc[
                (
                    (flattrade_instruments["Expiry"] == expiry)
                    & (flattrade_instruments["Exchange"] == segment[:3])
                    & (flattrade_instruments["Symbol"] == symbol)
                    & (
                        flattrade_instruments[
                            "Instrument"
                        ].str.startswith("OPT")
                    )
                    & (
                        flattrade_instruments["Optiontype"]
                        == instrument_type
                    )
                    & (flattrade_instruments["Strike"] == (strike))
                )
            ].iloc[0]
            instrument = Instrument(
                exchange=instrument["Exchange"],
                token=instrument["Token"],
                symbol=instrument["Tradingsymbol"],
                name=instrument["Symbol"],
                expiry=instrument["Expiry"],
                lot_size=instrument["Lotsize"],
            )
            return instrument
        elif "FUT" in segment:
            instrument = flattrade_instruments.loc[
                (
                    (flattrade_instruments["Expiry"] == expiry)
                    & (flattrade_instruments["Exchange"] == segment[:3])
                    & (flattrade_instruments["Symbol"] == symbol)
                    & (
                        flattrade_instruments[
                            "Instrument"
                        ].str.startswith("FUT")
                    )
                )
            ].iloc[0]
            instrument = Instrument(
                exchange=instrument["Exchange"],
                token=instrument["Token"],
                symbol=instrument["Tradingsymbol"],
                name=instrument["Symbol"],
                expiry=instrument["Expiry"],
                lot_size=instrument["Lotsize"],
            )
            return instrument

    # TODO: Done
    def get_instrument_by_symbol(self, exchange, symbol):
        mainlogger.info("Function : flattrade get_instrument_by_symbol")

        if exchange == "NSE":
            instrument = flattrade_instruments.loc[
                (
                    (flattrade_instruments["Exchange"] == exchange)
                    & (flattrade_instruments["Symbol"] == symbol)
                )
            ].iloc[0]
            instrument = Instrument(
                exchange=instrument["Exchange"],
                token=instrument["Token"],
                symbol=instrument["Tradingsymbol"],
                name=instrument["Symbol"],
                expiry=instrument["Expiry"],
                lot_size=instrument["Lotsize"],
            )
            return instrument
        elif exchange in ("NFO", "BFO"):
            instrument = flattrade_instruments.loc[
                (
                    (flattrade_instruments["Exchange"] == exchange)
                    & (flattrade_instruments["Tradingsymbol"] == symbol)
                )
            ].iloc[0]
            instrument = Instrument(
                exchange=instrument["Exchange"],
                token=instrument["Token"],
                symbol=instrument["Tradingsymbol"],
                name=instrument["Symbol"],
                expiry=instrument["Expiry"],
                lot_size=instrument["Lotsize"],
            )
            return instrument

    # TODO: Done
    def get_ltp(self, norenApi, exchange, symbol):
        mainlogger.info("Function : flattrade get_ltp")

        symbol = self.get_instrument_by_symbol(exchange, symbol).symbol
        price = float(norenApi.get_quotes(exchange, symbol)["lp"])
        return price

    # TODO: Done - Has Commented code
    def punch_order(
        self,
        acc_id,
        symbol,
        direction,
        qty,
        triggerPrice,
        price,
        order_type="MARKET",
        misFlag="NRML",
        amoFlag=False,
        exchange="NFO",
    ):
        mainlogger.info("Function : flattrade punch_order")
        norenApi = dic_accounts[acc_id]["token"]
        try:
            response = {}
            tx_type = "B" if (direction == "buy" or direction == "b") else "S"
            if order_type == "MARKET":
                order_type = "MKT"
                price = 0.0
                trigger_price = None

            elif order_type == "SLM":
                order_type = "SL-MKT"
                price = 0.0
                trigger_price = triggerPrice

            elif order_type == "SLL":
                order_type = "SL-LMT"
                price = price
                trigger_price = triggerPrice

            elif order_type == "LIMIT":
                order_type = "LMT"

            ##MIS or NORMAL
            if exchange in ("NFO", "BFO") and misFlag == "NRML":
                prod_type = "M"
            elif exchange in ("NFO", "BFO") and misFlag == "MIS":
                prod_type = "I"
            else:
                prod_type = "C"

            # TODO AMO not supported in flattrade
            # AMO or NOT
            is_amo = "NO" if amoFlag == False else "YES"
            ordertag = (
                order_tag_prefix
                + "_"
                + str(random.randrange(1000, 10000000))
            )
            #trigger_price = None
            for attempt in range(2):
                try:
                    if order_type == "LMT":
                        if price == 0.0 or price is None:
                            price = float(norenApi.get_quotes(exchange, symbol)["lp"])
                            if price==None or price<0.5:
                                price=0.0
                                order_type='MKT'
                            elif tx_type == "SELL":
                                price = round(price * limit_price_factor_sell, 1)
                            else:
                                price = round(price * limit_price_factor_buy, 1)
                        trigger_price = None

                    price = round(price, 1)
                    trigger_price = (
                        trigger_price
                        if trigger_price is None
                        else round(trigger_price, 1)
                    )
                    r = norenApi.place_order(
                        buy_or_sell=tx_type,
                        product_type=prod_type,
                        exchange=exchange,
                        tradingsymbol=symbol,
                        quantity=qty,
                        discloseqty=0,
                        price_type=order_type,
                        price=price,
                        trigger_price=trigger_price,
                        amo=is_amo,
                        retention="DAY",
                        remarks=ordertag,
                    )

                    if r is None:
                        raise Exception(
                            "order placement failed: Broker API returned None"
                        )
                    else:
                        order_id = r["norenordno"]
                        response = {
                            "status": "success",
                            "message": "Order placed successfully",
                            "data": {"oms_order_id": order_id, "average_price": price},
                        }
                except Exception as e:
                    mainlogger.exception("Order placement failed: " + str(e))
                    try:
                        response = {
                            "status": "rejected",
                            "message": str(e),
                            "data": {
                                "oms_order_id": zerodha_failed_order_hardcoded_id
                            },
                        }
                        # check if Order Got accepted before retrying order to avoid duplication
                        # TODO check with client
                        order = broker.get_order_by_tag(
                             acc_id, ordertag
                         )
                        order = -2  # TODO : Update to client
                        if order == -2:
                            response = {
                                "status": "rejected",
                                "message": "Unable to verify if previous order api was successful. Please verify in broker terminal manually and retry if needed.",
                                "data": {
                                    "oms_order_id": zerodha_failed_order_hardcoded_id,
                                    "average_price": price,
                                },
                            }
                            return response

                        elif order != -1 and order != -2:
                            response = {
                                "status": "success",
                                "message": "Order placed successfully",
                                "data": {
                                    "oms_order_id": order["oms_order_id"],
                                    "average_price": order["average_price"],
                                },
                            }
                            return response
                        time.sleep(5)
                        self.check_login_token(acc_id)
                    except Exception as e:
                        mainlogger.exception(
                            "Punch order failed while retrying: " + str(e)
                        )
                else:
                    break
            return response
        except Exception as e:
            mainlogger.exception(str(e))

    # TODO: Done
    ## Exit an order
    def exit_order(
        self,
        acc_id,
        norenApi,
        exchange,
        direction,
        symbol,
        order_id,
        qty,
        prod_type,
        order_type="MARKET",
        price=0.0,
    ):
        mainlogger.info("Function : flattrade exit_order")
        try:
            tx_type = "B" if (direction == "buy" or direction == "b") else "S"
            prod_type = "M" if exchange in ("NFO", "BFO") else "C"
            if order_type == "MARKET":
                order_type = "MKT"
            elif order_type == "LIMIT":
                order_type = "LMT"

            for attempt in range(2):
                try:
                    if order_type == "MKT":
                        r = norenApi.modify_order(
                            exchange=exchange,
                            tradingsymbol=symbol,
                            orderno=order_id,
                            newprice_type=order_type,
                            newquantity=qty,
                        )
                    elif order_type == "LMT":
                        if price == 0.0:
                            price = float(norenApi.get_quotes(exchange, symbol)["lp"])
                            if tx_type == "SELL":
                                price = round(
                                    price * limit_price_factor_sell, 1
                                )
                            else:
                                price = round(
                                    price * limit_price_factor_buy, 1
                                )
                        r = norenApi.modify_order(
                            exchange=exchange,
                            tradingsymbol=symbol,
                            orderno=order_id,
                            newprice_type=order_type,
                            newquantity=qty,
                            newprice=price,
                        )
                    if r is None:
                        raise Exception("order exit failed: Broker API returned None")
                    order_id = r["result"]
                    response = {
                        "status": "success",
                        "message": "Order placed successfully",
                        "data": {"oms_order_id": order_id},
                    }
                except Exception as e:
                    mainlogger.exception("Order exit failed: " + str(e))
                    response = {
                        "status": "rejected",
                        "message": str(e),
                        "data": {"oms_order_id": order_id},
                    }
                    time.sleep(5)
                    self.check_login_token(acc_id)
                else:
                    break
            return response
        except Exception as e:
            mainlogger.exception(str(e))

    # TODO: Done
    ## Modify an order
    def modify_order(
        self,
        acc_id,
        norenApi,
        exchange,
        direction,
        symbol,
        order_id,
        qty,
        prod_type,
        order_type,
        price,
        triggerPrice,
    ):
        mainlogger.info("Function : flattrade modify_order")
        try:
            if order_type == "MARKET":
                order_type = "MKT"
                price = 0.0
                trigger_price = None
            elif order_type == "SLM":
                order_type = "SL-MKT"
                price = 0.0
                trigger_price = triggerPrice
            elif order_type == "SLL":
                order_type = "SL-LMT"
                price = price
                trigger_price = triggerPrice
            elif order_type == "LIMIT":
                order_type = "LMT"
                price = price
                trigger_price = None

            prod_type = "M" if exchange in ("NFO", "BFO") else "C"
            trigger_price = None
            for attempt in range(2):
                try:
                    r = norenApi.modify_order(
                        exchange=exchange,
                        tradingsymbol=symbol,
                        orderno=order_id,
                        newprice_type=order_type,
                        newquantity=qty,
                        newtrigger_price=trigger_price,
                        newprice=price,
                    )
                    if r is None:
                        raise Exception("order exit failed: Broker API returned None")
                    order_id = r["result"]
                    response = {
                        "status": "success",
                        "message": "Order placed successfully",
                        "data": {"oms_order_id": order_id},
                    }
                except Exception as e:
                    mainlogger.exception("Order modify failed: " + str(e))
                    response = {
                        "status": "rejected",
                        "message": str(e),
                        "data": {"oms_order_id": order_id},
                    }
                    time.sleep(5)
                    self.check_login_token(acc_id)
                else:
                    break
            return response
        except Exception as e:
            mainlogger.exception(str(e))

    # TODO: Done
    def check_if_position_open(self, norenApi, exchange, symbol, qty, direction):
        mainlogger.info("Function : flattrade check_if_position_open")
        try:
            trade_book = norenApi.get_positions()
            if trade_book is None or len(trade_book) == 0:
                mainlogger.exception("Retruned Empty Position Book")
                return True, 10000

            for t in trade_book:
                # if int(t['netqty'])!=0 and direction.upper()=='BUY' and t['exch']==exchange and (t['tsym'])==symbol and int(t['netqty'])<=int(qty)*-1 :
                if (
                    int(t["netqty"]) != 0
                    and direction.upper() == "BUY"
                    and t["exch"] == exchange
                    and (t["tsym"]) == symbol
                    and int(t["netqty"]) < 0
                ):
                    return True, abs(int(t["netqty"]))
                # elif int(t['netqty'])!=0 and  direction.upper()=='SELL' and t['exch']==exchange and (t['tsym'])==symbol and int(t['netqty'])>=int(qty):
                elif (
                    int(t["netqty"]) != 0
                    and direction.upper() == "SELL"
                    and t["exch"] == exchange
                    and (t["tsym"]) == symbol
                    and int(t["netqty"]) > 0
                ):
                    return True, abs(int(t["netqty"]))
            return False, 0
        except Exception as e:
            mainlogger.exception(str(e))
            return True, 10000

    # TODO: Done
    def get_order_history(self, acc_id, order_id):
        mainlogger.info("Function : flattrade get_order_history")
        if order_id == zerodha_failed_order_hardcoded_id:
            tmp = {
                "order_status": "rejected",
                "average_price": 0,
                "filled_quantity": 0,
                "message": "NA",
                "data": {
                    "oms_order_id": zerodha_failed_order_hardcoded_id
                },
            }
            return tmp

        for attempt in range(2):
            try:
                tmp = dic_accounts[acc_id]["token"].single_order_history(
                    str(order_id)
                )[0]
                # tmp=dic_accounts[acc_id]['token'].order_history(order_id)[-1]
                tmp["oms_order_id"] = tmp["norenordno"]
                tmp["order_status"] = (
                    tmp["status"]
                    .replace("_", " ")
                    .lower()
                    .replace("canceled", "cancelled")
                )
                tmp["price_to_fill"] = float(tmp["prc"])
                tmp["transaction_type"] = tmp["trantype"]
                tmp["order_type"] = tmp["prctyp"]
                tmp["product"] = tmp["prd"]
                tmp["exchange"] = tmp["exch"]

                try:
                    tmp["order_tag"] = tmp["remarks"]
                except:
                    tmp["order_tag"] = None

                try:
                    tmp["average_price"] = float(tmp["avgprc"])
                except Exception:
                    tmp["average_price"] = None

                try:
                    tmp["trigger_price"] = float(tmp["trgprc"])
                except Exception:
                    tmp["trigger_price"] = None

                try:
                    tmp["rejection_reason"] = tmp["rejreason"]
                except Exception:
                    tmp["rejection_reason"] = None

                try:
                    tmp["filled_quantity"] = int(tmp["flqty"])
                    tmp["quantity"] = int(tmp["flqty"])

                except Exception:
                    try:
                        tmp["filled_quantity"] = int(tmp["qty"])
                        tmp["quantity"] = int(tmp["qty"])
                    except Exception:
                        tmp["filled_quantity"] = None
                        tmp["quantity"] = None
            except Exception as te:
                mainlogger.exception("Function : get_order_history " + str(te))
                time.sleep(2)
                tmp = {
                    "order_status": "not_found",
                    "average_price": 0,
                    "filled_quantity": 0,
                    "message": "order not found/api error",
                    "data": {
                        "oms_order_id": zerodha_failed_order_hardcoded_id
                    },
                }
            else:
                break
        return tmp

    # TODO: Done
    def get_all_order_history(self, acc_id):
        mainlogger.info("Function : flattrade get_all_order_history")
        orders = []

        order_book = dic_accounts[acc_id]["token"].get_order_book()
        if order_book is None:
            return orders

        for tmp in order_book:
            try:
                tmp["oms_order_id"] = tmp["norenordno"]
                tmp["order_status"] = (
                    tmp["status"]
                    .replace("_", " ")
                    .lower()
                    .replace("canceled", "cancelled")
                )
                tmp["price_to_fill"] = float(tmp["prc"])

                tmp["transaction_type"] = tmp["trantype"]
                tmp["order_type"] = tmp["prctyp"]
                tmp["product"] = tmp["prd"]
                tmp["trigger_price"] = None
                tmp["order_tag"] = tmp["remarks"]
                # tmp['exchange_time']=datetime.datetime.strptime(tmp['exch_tm'], '%d-%m-%Y %H:%M:%S').timestamp()
                tmp["exchange_time"] = datetime.datetime.strptime(
                    tmp["norentm"], "%H:%M:%S %d-%m-%Y"
                ).timestamp()
                tmp["exchange"] = tmp["exch"]

                try:
                    tmp["average_price"] = float(tmp["avgprc"])
                except Exception:
                    tmp["average_price"] = None

                try:
                    tmp["trigger_price"] = float(tmp["trgprc"])
                except Exception:
                    tmp["trigger_price"] = None

                try:
                    tmp["rejection_reason"] = tmp["rejreason"]
                except Exception:
                    tmp["rejection_reason"] = None

                try:
                    tmp["filled_quantity"] = int(tmp["flqty"])
                    tmp["quantity"] = int(tmp["flqty"])
                except Exception:
                    try:
                        tmp["filled_quantity"] = int(tmp["qty"])
                        tmp["quantity"] = int(tmp["qty"])
                    except Exception:
                        tmp["filled_quantity"] = None
                        tmp["quantity"] = None
                orders.append(tmp)
            except:
                pass
        return orders

    # TODO: Done
    def tradingAccount_PNLSummary(self, acc_id):
        mainlogger.info("Function : flattrade tradingAccount_PNLSummary")
        positions = dic_accounts[acc_id]["token"].get_positions()
        totalRealizedPNL = 0
        totalUnRealizedPNL = 0
        maxrealizedLossPosition = 0
        maxrealizedPoriftPosition = 0
        maxUnrealizedLossPosition = 0
        maxUnrealizedProfitPosition = 0

        if positions != None and "not_ok" not in str(positions).lower():
            for p in positions:
                totalRealizedPNL = totalRealizedPNL + float(p["rpnl"])
                totalUnRealizedPNL = totalUnRealizedPNL + float(p["urmtom"])

                if float(p["rpnl"]) > maxrealizedPoriftPosition:
                    maxrealizedPoriftPosition = float(p["rpnl"])

                if (
                    float(p["rpnl"]) < 0
                    and abs(float(p["rpnl"])) > maxrealizedLossPosition
                ):
                    maxrealizedLossPosition = abs(float(p["rpnl"]))

                if float(p["urmtom"]) > maxUnrealizedProfitPosition:
                    maxUnrealizedProfitPosition = float(p["urmtom"])

                if (
                    float(p["urmtom"]) < 0
                    and abs(float(p["urmtom"])) > maxUnrealizedLossPosition
                ):
                    maxUnrealizedLossPosition = abs(float(p["urmtom"]))

        pnlSummary = {
            "totalRealizedPNL": round(totalRealizedPNL, 1),
            "totalUnRealizedPNL": round(totalUnRealizedPNL, 1),
            "maxrealizedPoriftPosition": round(maxrealizedPoriftPosition, 1),
            "maxrealizedLossPosition": round(maxrealizedLossPosition, 1) * -1,
            "maxUnrealizedProfitPosition": round(maxUnrealizedProfitPosition, 1),
            "maxUnrealizedLossPosition": round(maxUnrealizedLossPosition, 1) * -1,
        }

        return pnlSummary

#broker level change

class Broker:
    
    kite_broker=KiteBroker()
    angel_broker=AngelBroker()
    shoony_broker=ShoonyaBroker()
    alice_new_broker=AliceNewBroker()
    iifl_broker=IIFLBroker()
    flat_broker=FlattradeBroker()
    
    redis_ltp=RedisLtpFetcher()
    
    def dynamic_hash_row(self, row, columns):
        # Create a tuple with the values of the selected columns
        row_data = tuple(row[col] for col in columns)
        # Generate the hash for the tuple
        return hash(row_data)

    def hash_row(self, instruments, columns_to_hash, hash_column_name):
        instruments[hash_column_name] = instruments.apply(self.dynamic_hash_row, axis=1, columns=columns_to_hash)
        return instruments

    def check_login_token(self,acc_id, row=None):
        mainlogger.info('Function : Broker check_login_token')
        
        try:
            broker=dic_accounts[acc_id]['broker']
            try:
                row=dict(row)
                if row['account_update_time'] <= dic_accounts[acc_id]['token_refresh']: 
                    mainlogger.info('Skipped check_login_token')
                    return 1
            except:
                pass
            
        except Exception as e:
            #print('exception', str(e)) #remove
            coredb=pool.connection()
            sql="select * from vw_activeaccounts where ID="+str(acc_id)
            df = pd.read_sql(sql, coredb)
            df = df.replace({np.nan: None})
            
            broker=None
            
            for index, row in  df.iterrows():
                dic_accounts[row['ID']]={'id':row['ID'],'username':row['username'],'token':None,'chat_id':row['tele_chat_id'],'broker':row['broker'],'lock':MyLock(),'token_refresh':datetime.datetime.now()-timedelta(minutes=5)}
                broker=row['broker']
                break
                
            coredb.close()
        
        if broker==zerodha_broker_code:
            Broker.kite_broker.check_login_token(acc_id)

        elif broker==angel_broker_code:
            Broker.angel_broker.check_login_token(acc_id)

        elif broker==finvasia_broker_code:
            Broker.shoony_broker.check_login_token(acc_id)

        elif broker==alice_broker_code:
            Broker.alice_new_broker.check_login_token(acc_id)
        
        elif broker==iifl_broker_code:
            Broker.iifl_broker.check_login_token(acc_id)
            
        elif broker==flat_broker_code:
            Broker.flat_broker.check_login_token(acc_id)
            
        return 1
    
    def account_single_login(self,acc_id):
        mainlogger.info('Function : Broker account_single_login')
        
        try:
            broker=dic_accounts[acc_id]['broker']
        except:
            coredb=pool.connection()
            sql="select broker from vw_activeaccounts where ID="+str(acc_id)
            df = pd.read_sql(sql, coredb)
            df = df.replace({np.nan: None})
            broker=df['broker'][0]
            coredb.close()
        
        if broker==zerodha_broker_code:
            Broker.kite_broker.account_single_login(acc_id)
            
        elif broker==angel_broker_code:
            Broker.angel_broker.account_single_login(acc_id)
            
        elif broker==finvasia_broker_code:
            Broker.shoony_broker.account_single_login(acc_id)
            
        elif broker==alice_broker_code:
            Broker.alice_new_broker.account_single_login(acc_id)
            
        elif broker==iifl_broker_code:
            Broker.iifl_broker.account_single_login(acc_id)

        elif broker==flat_broker_code:
            Broker.flat_broker.account_single_login(acc_id)
        return 1
    
    
    def account_login(self,all_brokers=True):
        mainlogger.info('Function : Broker login')
        
        if all_brokers==True:
            Broker.kite_broker.account_login()
            #Broker.alice_broker.account_login()
            Broker.angel_broker.account_login()
            Broker.shoony_broker.account_login()
            #Broker.alice_new_broker.account_login()
            Broker.alice_new_broker.account_login_parallel()
            Broker.iifl_broker.account_login()
            Broker.flat_broker.account_login()
        else:
            Broker.alice_broker.account_login()
    
    def get_weekly_expiry(self, primary_broker,scrip='NIFTY',exchange='NFO'):
        mainlogger.info('Function : Broker get_order_history')

        if primary_broker==zerodha_broker_code:
            ##print("Broker.kite_broker.get_weekly_expiry()")
            return Broker.kite_broker.get_weekly_expiry(scrip,exchange)


    def get_monthly_expiry(self, primary_broker,scrip='NIFTY',exchange='NFO'):
        mainlogger.info('Function : Broker get_order_history')

        if primary_broker==zerodha_broker_code:
            return Broker.kite_broker.get_monthly_expiry(scrip,exchange)

    
    def get_monthly_expiry_plus(self, primary_broker,scrip='NIFTY',exchange='NFO'):
        mainlogger.info('Function : Broker get_order_history')

        if primary_broker==zerodha_broker_code:
            return Broker.kite_broker.get_monthly_expiry_plus(scrip,exchange)


    def get_future_weekly_expiry(self, primary_broker,scrip='NIFTY',exchange='NFO'):
        mainlogger.info('Function : Broker get_future_weekly_expiry')

        if primary_broker==zerodha_broker_code:
            return Broker.kite_broker.get_future_weekly_expiry(scrip,exchange)


    def get_future_monthly_expiry(self, primary_broker,scrip='NIFTY',exchange='NFO'):
        mainlogger.info('Function : Broker get_future_monthly_expiry')

        if primary_broker==zerodha_broker_code:
            return Broker.kite_broker.get_future_monthly_expiry(scrip,exchange)

    
    def is_expiry_day(self, exp_weekly):
        ##print("is_expiry_day")
        mainlogger.info('Function : is_expiry_day')
        dt=global_dt_today
        exp_date=datetime.datetime.strptime(exp_weekly, '%d %b%y').date()

        if exp_date==dt:
            return 'EXP_DAY'

        return 'NOT_EXP_DAY'

    def is_monthly_expiry_day(self, exp_monthly):
        ##print("is_expiry_day")
        mainlogger.info('Function : is_monthly_expiry_day')
        dt=global_dt_today
        exp_date=datetime.datetime.strptime(exp_monthly, '%d %b%y').date()

        if exp_date==dt:
            return 'MONTHLY_EXP_DAY'

        return 'NOT_MONTHLY_EXP_DAY'

    def subscribe_ltp(self, ins_list,exchange='NSE'):
        mainlogger.info('Function : Broker subscribe_ltp')
    
        if primary_broker==zerodha_broker_code:
            Broker.kite_broker.subscribe_ltp(ins_list,exchange)
            return 1
        
    
    def subscribe_ltp_nfo(self, ins_list, exchange='NFO'):
        mainlogger.info('Function : Broker subscribe_ltp_nfo')
        
        if primary_broker==zerodha_broker_code:
            Broker.kite_broker.subscribe_ltp_nfo(ins_list,exchange)
            return 1
        
    def subscribe_to_instrument(self, exchange, symbol):
        mainlogger.info('Function : Broker subscribe_to_instrument')
        try:
            qltp.put({'exchange':exchange,'symbol':symbol})
        except Exception as e:
            mainlogger.exception(str(e))
        
        
    def subscribe_to_instrument_final(self, exchange, symbol):
        mainlogger.info('Function : Broker subscribe_to_instrument_final')
        
        if primary_broker==zerodha_broker_code:
            Broker.kite_broker.subscribe_to_instrument(exchange,symbol)
            return 1
    
    def get_circuit_limits(self, acc_id,exchange, symbol):
        mainlogger.info('Function : Broker get_circuit_limits')
        
        if dic_accounts[acc_id]['broker']==zerodha_broker_code:
            return Broker.kite_broker.get_circuit_limits(dic_accounts[acc_id]['token'],exchange, symbol)
        else:
            return Broker.kite_broker.get_circuit_limits(dic_accounts[primary_kite]['token'],exchange, symbol)
        
        return -1
        
    def redis_get_ltp(self,exchange, symbol):
        #check redis first
        ins=self.get_instrument_by_symbol(primary_scrip_generic, exchange, symbol)
        ltp=Broker.redis_ltp.get_ltp(ins[1])        
        return ltp
        
    def get_ltp(self, acc_id, exchange, symbol):
        mainlogger.info('Function : Broker get_ltp')
        
        if dic_accounts[acc_id]['broker']==zerodha_broker_code:
            return Broker.kite_broker.get_ltp(dic_accounts[acc_id]['token'], exchange, symbol)
               
        elif dic_accounts[acc_id]['broker']==angel_broker_code:
            return Broker.angel_broker.get_ltp(dic_accounts[acc_id]['token'], exchange, symbol)
        
        elif dic_accounts[acc_id]['broker']==finvasia_broker_code:
            return Broker.shoony_broker.get_ltp(dic_accounts[acc_id]['token'], exchange, symbol)
        
        elif dic_accounts[acc_id]['broker']==alice_broker_code:
            return Broker.alice_new_broker.get_ltp(dic_accounts[acc_id]['token'], exchange, symbol)
        
        elif dic_accounts[acc_id]['broker']==iifl_broker_code:
            return Broker.iifl_broker.get_ltp(dic_accounts[acc_id]['token'], exchange, symbol)
        
        elif dic_accounts[acc_id]['broker']==flat_broker_code:
            return Broker.flat_broker.get_ltp(dic_accounts[acc_id]['token'], exchange, symbol)
        
        return -1
    
   
    
    def punch_order(self, acc_id, symbol,direction, qty, triggerPrice, price, 
                order_type='MARKET',misFlag='NRML', amoFlag=False, exchange='NFO', 
                strategyName='NA'
               ):
        mainlogger.info('Function : Broker punch_order')
    
        if dic_accounts[acc_id]['broker']==zerodha_broker_code:
            return Broker.kite_broker.punch_order(acc_id, dic_accounts[acc_id]['token'], None, symbol,direction, qty, triggerPrice, price, 
                order_type,misFlag, amoFlag, exchange, 
                strategyName
               )
        
        
        elif dic_accounts[acc_id]['broker']==angel_broker_code:
            return Broker.angel_broker.punch_order(acc_id, dic_accounts[acc_id]['token'],None, symbol,direction, qty, triggerPrice, price, 
                order_type,misFlag, amoFlag, exchange, 
                strategyName
               )
        
        elif dic_accounts[acc_id]['broker']==finvasia_broker_code:
            return Broker.shoony_broker.punch_order(acc_id, dic_accounts[acc_id]['token'],None, symbol,direction, qty, triggerPrice, price, 
                order_type,misFlag, amoFlag, exchange, 
                strategyName
               )
        
        elif dic_accounts[acc_id]['broker']==alice_broker_code:
            return Broker.alice_new_broker.punch_order(acc_id, dic_accounts[acc_id]['token'],None, symbol,direction, qty, triggerPrice, price, 
                order_type,misFlag, amoFlag, exchange, 
                strategyName
               )
        
        elif dic_accounts[acc_id]['broker']==iifl_broker_code:
            return Broker.iifl_broker.punch_order(acc_id, dic_accounts[acc_id]['token'],None, symbol,direction, qty, triggerPrice, price, 
                order_type,misFlag, amoFlag, exchange, 
                strategyName
               )
  
        elif dic_accounts[acc_id]['broker']==flat_broker_code:
            return Broker.flat_broker.punch_order(acc_id, symbol,direction, qty, triggerPrice, price, 
                order_type,misFlag, amoFlag, exchange
               )
        return -1
    
    def exit_order(self, acc_id, exchange, direction, symbol, order_id, qty, prod_type, order_type='MARKET', price=0.0, ordertag='NA'):
        mainlogger.info('Function : Broker exit_order')
        if dic_accounts[acc_id]['broker']==zerodha_broker_code:
            return Broker.kite_broker.exit_order(acc_id, dic_accounts[acc_id]['token'], None, exchange, direction, symbol, order_id, qty, prod_type,order_type, price)
        
        elif dic_accounts[acc_id]['broker']==angel_broker_code:
            order_id=order_id.split('#')[0]
            return Broker.angel_broker.exit_order(acc_id, dic_accounts[acc_id]['token'], None, exchange, direction, symbol, order_id, qty, prod_type,order_type, price)
        
        elif dic_accounts[acc_id]['broker']==finvasia_broker_code:
            return Broker.shoony_broker.exit_order(acc_id, dic_accounts[acc_id]['token'], None, exchange, direction, symbol, order_id, qty, prod_type,order_type, price)
        
        elif dic_accounts[acc_id]['broker']==alice_broker_code:
            return Broker.alice_new_broker.exit_order(acc_id, dic_accounts[acc_id]['token'], None, exchange, direction, symbol, order_id, qty, prod_type,order_type,price)
        
        elif dic_accounts[acc_id]['broker']==iifl_broker_code:
            return Broker.iifl_broker.exit_order(acc_id, dic_accounts[acc_id]['token'], None, exchange, direction, symbol, order_id, qty, prod_type,order_type,price,ordertag)

        elif dic_accounts[acc_id]['broker']==flat_broker_code:
            return Broker.flat_broker.exit_order(acc_id, dic_accounts[acc_id]['token'],  exchange, direction, symbol, order_id, qty, prod_type,order_type,price)
    
        return -1
    
    def modify_order(self, acc_id, exchange, direction, symbol, order_id, qty, prod_type, order_type, price, trigger_price, ordertag='NA'):
        mainlogger.info('Function : Broker modify_order')
        
        if dic_accounts[acc_id]['broker']==zerodha_broker_code:
            return Broker.kite_broker.modify_order(acc_id, dic_accounts[acc_id]['token'], None, exchange, direction, symbol, order_id, qty, prod_type, order_type, price, trigger_price)
        
        
        elif dic_accounts[acc_id]['broker']==angel_broker_code:
            order_id=order_id.split('#')[0]
            return Broker.angel_broker.modify_order(acc_id, dic_accounts[acc_id]['token'], None, exchange, direction, symbol, order_id, qty, prod_type, order_type, price, trigger_price)
        
        elif dic_accounts[acc_id]['broker']==finvasia_broker_code:
            return Broker.shoony_broker.modify_order(acc_id, dic_accounts[acc_id]['token'], None, exchange, direction, symbol, order_id, qty, prod_type, order_type, price, trigger_price)
        
        elif dic_accounts[acc_id]['broker']==alice_broker_code:
            return Broker.alice_new_broker.modify_order(acc_id, dic_accounts[acc_id]['token'], None, exchange, direction, symbol, order_id, qty, prod_type, order_type, price, trigger_price)
        
        elif dic_accounts[acc_id]['broker']==iifl_broker_code:
            return Broker.iifl_broker.modify_order(acc_id, dic_accounts[acc_id]['token'], None, exchange, direction, symbol, order_id, qty, prod_type, order_type, price, trigger_price,ordertag)
        
        elif dic_accounts[acc_id]['broker']==flat_broker_code:
            return Broker.flat_broker.modify_order(acc_id, dic_accounts[acc_id]['token'], exchange, direction, symbol, order_id, qty, prod_type, order_type, price, trigger_price)
        
        return -1
    
    def check_if_position_open(self, acc_id, exchange, symbol, qty, direction):
        mainlogger.info('Function : Broker check_if_position_open')
        
        if dic_accounts[acc_id]['broker']==zerodha_broker_code:
            return Broker.kite_broker.check_if_position_open(dic_accounts[acc_id]['token'], exchange, symbol, qty, direction)
    
        elif dic_accounts[acc_id]['broker']==angel_broker_code:
            return Broker.angel_broker.check_if_position_open(dic_accounts[acc_id]['token'], exchange, symbol, qty, direction, acc_id)
    
        elif dic_accounts[acc_id]['broker']==finvasia_broker_code:
            return Broker.shoony_broker.check_if_position_open(dic_accounts[acc_id]['token'], exchange, symbol, qty, direction)
        
        elif dic_accounts[acc_id]['broker']==alice_broker_code:
            return Broker.alice_new_broker.check_if_position_open(dic_accounts[acc_id]['token'], exchange, symbol, qty, direction)
        
        elif dic_accounts[acc_id]['broker']==iifl_broker_code:
            return Broker.iifl_broker.check_if_position_open(acc_id,dic_accounts[acc_id]['token'], exchange, symbol, qty, direction)

        elif dic_accounts[acc_id]['broker']==flat_broker_code:
            return Broker.flat_broker.check_if_position_open(dic_accounts[acc_id]['token'], exchange, symbol, qty, direction)
     
        return -1
    
    
    def custom_get_instrument_for_fno(self, acc_id, symbol, exp, fut, strike, ce, segment='NFO'):
        mainlogger.info('Function : Broker custom_get_instrument_for_fno')
        
        try:
            if dic_accounts[acc_id]['broker']==zerodha_broker_code:
                return Broker.kite_broker.custom_get_instrument_for_fno(symbol, exp, fut, strike, ce, segment)

            
            elif dic_accounts[acc_id]['broker']==angel_broker_code:
                return Broker.angel_broker.custom_get_instrument_for_fno(symbol, exp, fut, strike, ce, segment)

            elif dic_accounts[acc_id]['broker']==finvasia_broker_code:
                return Broker.shoony_broker.custom_get_instrument_for_fno(symbol, exp, fut, strike, ce, segment)

            elif dic_accounts[acc_id]['broker']==alice_broker_code:
                return Broker.alice_new_broker.custom_get_instrument_for_fno(symbol, exp, fut, strike, ce, segment)
            
            elif dic_accounts[acc_id]['broker']==iifl_broker_code:
                return Broker.iifl_broker.custom_get_instrument_for_fno(symbol, exp, fut, strike, ce, segment)
            
            elif dic_accounts[acc_id]['broker']==flat_broker_code:
                return Broker.flat_broker.custom_get_instrument_for_fno(symbol, exp, fut, strike, ce, segment)
            
            
        except Exception as e:
            mainlogger.exception('Function : Broker custom_get_instrument_for_fno not found '+str([acc_id,symbol, exp, fut, strike, ce,segment]))
            return "FAILED: custom_get_instrument_for_fno_final instrument not found"

        
    def get_instrument_by_symbol(self, acc_id, exchange, symbol):
        mainlogger.info('Function : Broker get_instrument_by_symbol')
        
        try:
            
            if dic_accounts[acc_id]['broker']==zerodha_broker_code:
                return Broker.kite_broker.get_instrument_by_symbol(exchange, symbol)

            elif dic_accounts[acc_id]['broker']==angel_broker_code:
                return Broker.angel_broker.get_instrument_by_symbol(exchange, symbol)

            elif dic_accounts[acc_id]['broker']==finvasia_broker_code:
                return Broker.shoony_broker.get_instrument_by_symbol(exchange, symbol)

            elif dic_accounts[acc_id]['broker']==alice_broker_code:
                return Broker.alice_new_broker.get_instrument_by_symbol(exchange, symbol)
            
            elif dic_accounts[acc_id]['broker']==iifl_broker_code:
                return Broker.iifl_broker.get_instrument_by_symbol(exchange, symbol)

            elif dic_accounts[acc_id]['broker']==flat_broker_code:
                return Broker.flat_broker.get_instrument_by_symbol(exchange, symbol)
        
        except Exception as e:
            mainlogger.exception('Function : Broker get_instrument_by_symbol not found '+str([acc_id,exchange,symbol]))
            return "FAILED: get_instrument_by_symbol instrument not found"
    
        
    def get_order_history(self, acc_id, order_id):
        mainlogger.info('Function : Broker get_order_history')
        
        if dic_accounts[acc_id]['broker']==zerodha_broker_code:
            return Broker.kite_broker.get_order_history(acc_id, order_id)
            
        elif dic_accounts[acc_id]['broker']==angel_broker_code:
            order_id=order_id.split('#')[1]
            return Broker.angel_broker.get_order_history(acc_id, order_id)
            
        ##shoonya
        elif dic_accounts[acc_id]['broker']==finvasia_broker_code:
            return Broker.shoony_broker.get_order_history(acc_id, order_id)
        
        elif dic_accounts[acc_id]['broker']==alice_broker_code:
            return Broker.alice_new_broker.get_order_history(acc_id, order_id)
        
        elif dic_accounts[acc_id]['broker']==iifl_broker_code:
            return Broker.iifl_broker.get_order_history(acc_id, order_id)

        elif dic_accounts[acc_id]['broker']==flat_broker_code:
            return Broker.flat_broker.get_order_history(acc_id, order_id)
            
        return -1
    
    def get_all_order_history(self, acc_id):
        mainlogger.info('Function : Broker get_all_order_history')
        
        try:
            if dic_accounts[acc_id]['broker']==zerodha_broker_code:
                return Broker.kite_broker.get_all_order_history(acc_id)

            elif dic_accounts[acc_id]['broker']==angel_broker_code:
                return Broker.angel_broker.get_all_order_history(acc_id)

            elif dic_accounts[acc_id]['broker']==finvasia_broker_code:
                return Broker.shoony_broker.get_all_order_history(acc_id)

            elif dic_accounts[acc_id]['broker']==alice_broker_code:
                return Broker.alice_new_broker.get_all_order_history(acc_id)

            elif dic_accounts[acc_id]['broker']==iifl_broker_code:
                return Broker.iifl_broker.get_all_order_history(acc_id)

            elif dic_accounts[acc_id]['broker']==flat_broker_code:
                return Broker.flat_broker.get_all_order_history(acc_id)
            
        except Exception as e:
            mainlogger.exception(str(e))
            return None
    
    def tradingAccount_PNLSummary(self, acc_id): 
        mainlogger.info('Function : Broker tradingAccount_PNLSummary')
        
        if dic_accounts[acc_id]['token'] is None:
            return -1
        
        if dic_accounts[acc_id]['broker']==zerodha_broker_code:
            return Broker.kite_broker.tradingAccount_PNLSummary(acc_id)
            
        elif dic_accounts[acc_id]['broker']==angel_broker_code:
            return Broker.angel_broker.tradingAccount_PNLSummary(acc_id)
            
        elif dic_accounts[acc_id]['broker']==finvasia_broker_code:
            return Broker.shoony_broker.tradingAccount_PNLSummary(acc_id)
        
        elif dic_accounts[acc_id]['broker']==alice_broker_code:
            return Broker.alice_new_broker.tradingAccount_PNLSummary(acc_id)
        
        elif dic_accounts[acc_id]['broker']==iifl_broker_code:
            return Broker.iifl_broker.tradingAccount_PNLSummary(acc_id)

        elif dic_accounts[acc_id]['broker']==flat_broker_code:
            return Broker.flat_broker.tradingAccount_PNLSummary(acc_id)
        
            
        return -1
    
    def get_order_by_tag(self, acc_id, tag):
        mainlogger.info('Function : Broker get_order_by_tag')
        try:
            orderbook=self.get_all_order_history(acc_id)
            
            if orderbook is None:
                return -1
            
            for o in orderbook:
                if o['order_tag']==tag:
                    return o

            return -1
        except Exception as e:
            mainlogger.exception(str(e))
            return -2
                
# # Enable / Disable Login Failed Accounts

# %%


def disable_login_failed_accounts():
    #Disable Accounts with Failed Login for the last 3 days - 2 retries * 3 so value is 6
    mainlogger.info('Function:disable_login_failed_accounts') 

    try:
        coredb=pool.connection()
        cursor=coredb.cursor() 
        
        sel_disable_acccounts="select username, tele_chat_id from vw_activeaccounts va  where id in (select id from account where failed_attempts>6)"
        disable_acccounts_upd_sql = "update account set active_flag=0 where failed_attempts>6 and active_flag in (1,0)"
        
        df = pd.read_sql(sel_disable_acccounts, coredb)
        df = df.replace({np.nan: None})

        for index,row in df.iterrows():
            handle_exception('Account Disabled Because of Login Failures. Please verify credentials and enable it back. Account:'+row['username'],'NA',row['tele_chat_id'],True) 

        cursor.execute(disable_acccounts_upd_sql) 
        coredb.commit()
        cursor.close()
    
    except Exception as e:
        mainlogger.exception(str(e))
        
def reset_login_failed_accounts(acc_id):
    #reset failed attempts to zero
    mainlogger.info('Function:reset_login_failed_accounts')

    try:
        coredb=pool.connection()
        cursor=coredb.cursor() 
        
        reset_acccounts_upd_sql = "update account set failed_attempts=0, last_updated=last_updated, last_algo_login =current_timestamp() where id=%(acc_id)s"
        dic={'acc_id':acc_id}
        
        cursor.execute(reset_acccounts_upd_sql,dic) 
        coredb.commit()
        cursor.close()
    
    except Exception as e:
        mainlogger.exception(str(e))
        
def increment_login_failed_accounts(acc_id):
    #reset failed attempts to zero
    mainlogger.info('Function:increment_login_failed_accounts')

    try:
        coredb=pool.connection()
        cursor=coredb.cursor() 
        
        increment_acccounts_upd_sql = "update account set failed_attempts=failed_attempts+1, last_updated=last_updated where id=%(acc_id)s"
        
        dic={'acc_id':acc_id}
        
        cursor.execute(increment_acccounts_upd_sql,dic) 
        coredb.commit()
        cursor.close()
    
    except Exception as e:
        mainlogger.exception(str(e))


# # Login

# %%
def test_redlock():
    import pickle
    import redis

    ##remove all this 
    broker=Broker()
    broker.account_login(True)

    r = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
    obj = broker
    pickled_object = pickle.dumps(obj)
    r.set('global_var_dic_accounts', pickled_object)
    unpacked_object = pickle.loads(r.get('global_var_dic_accounts'))
    x_after = unpacked_object

    ##convert to managed dict 
    #dic_accounts=Manager().dict(unpacked_object)
    
    ##kite_instruments
    pickled_object = pickle.dumps(kite_instruments)
    r.set('global_var_kite_instruments', pickled_object)
    unpacked_object = pickle.loads(r.get('global_var_kite_instruments'))
    kite_after = unpacked_object

    ##convert to managed dict 
    #dic_accounts=Manager().dict(unpacked_object)


# %%
#broker=Broker()
#broker.check_login_token(primary_kite)
#ins=broker.get_instrument_by_symbol(primary_kite,'NFO','MIDCPNIFTY24N0412300PE')
#ins[1]==15765762.0

def polling_strategy_maxoi_rollover():
    mainlogger.info('Function:polling_strategy_maxoi_rollover')
    
    try:
        coredb = pool.connection()

        sql=("select distinct scrip_generic, ol.limit_price from ordertracker ot, orderleg ol "
             "where ol.strike_type=705 and ol.id=ot.OrderLegs_ID "
             "and ot.order_type not in ('Open') and ot.scheduler_name like '202%%'")
        #print(sql)
        df = pd.read_sql(sql, coredb)
        df['base_symbol'] = None
        df['call_put'] = None
        df['expiry'] = None
        df['strike'] = None
        df['exchange'] = None
        
        #print("df",df)
        
        #create unqiye symbol, call, expiry (Nifty, CE, Nov10) to get MaxOI
        ins_list = []
        for index,row in df.iterrows():
            
            ins=broker.get_instrument_by_symbol(primary_kite,'NA',row['scrip_generic'])
            ins = ins._asdict()
            df.loc[index, 'base_symbol'] = ins['base_symbol']
            df.loc[index, 'call_put'] = ins['call_put']
            df.loc[index, 'expiry'] = ins['expiry']
            df.loc[index, 'strike'] = ins['strike']
            df.loc[index, 'exchange'] = ins['exchange']
             
            ins_list.append((ins['exchange'], ins['base_symbol'], ins['call_put'], ins['expiry'], row['limit_price']))
        
        #Get MaxOi for this
        ins_list = list(set(ins_list))
        
        
        for i, (exchange, base_symbol, call_put, expiry, limit_price) in enumerate(ins_list):
            #print(f"Symbol: {base_symbol}, Call/Put: {call_put}, Expiry: {expiry}")
            
            attempts = 3  # First attempt + 1 retry
            ce_pick, pe_pick = None, None
            for attempt in range(attempts):
                ce_pick_1, pe_pick_1 = datafeed().get_cepe_picks(0,
                                                          limit_price,
                                                          base_symbol.lower(),
                                                          expiry.strftime('%d %b %Y'),
                                                          exchange,
                                                          0,
                                                          'MAXOPENINT'
                                                         )
                time.sleep(1)  # Delay for 1 second
                ce_pick_2, pe_pick_2 = datafeed().get_cepe_picks(0,
                                                          limit_price,
                                                          base_symbol.lower(),
                                                          expiry.strftime('%d %b %Y'),
                                                          exchange,
                                                          0,
                                                          'MAXOPENINT'
                                                         )
                
                if ce_pick_1.equals(ce_pick_2) and pe_pick_1.equals(pe_pick_2):
                    # If both match, continue with these values
                    ce_pick, pe_pick = ce_pick_1, pe_pick_1
                    break
                elif attempt == attempts - 1:
                    # Final attempt to get consistent values if the first two didn't match
                    ce_pick, pe_pick = datafeed().get_cepe_picks(0,
                                                          limit_price,
                                                          base_symbol.lower(),
                                                          expiry.strftime('%d %b %Y'),
                                                          exchange,
                                                          0,
                                                          'MAXOPENINT'
                                                         )
                    
            mainlogger.info('Strikes Received:' + str([ce_pick,pe_pick]))
            
            if call_put=='CE':
                strike=ce_pick['stkPrc']
            else:
                strike=pe_pick['stkPrc']
                
            ins_list[i]=(exchange, base_symbol, call_put, expiry, limit_price,strike)
        
        # Convert the updated list to a DataFrame
        df_ins_list = pd.DataFrame(ins_list, columns=['exchange', 'base_symbol', 'call_put', 'expiry', 'limit_price', 'strike'])
        
        #print("df_ins_list",df_ins_list)
        # Merge df and df_ins_list on the specified columns
        merged_df = pd.merge(df, df_ins_list, how='left', 
                             on=['base_symbol', 'exchange', 'call_put', 'expiry', 'limit_price'], 
                             suffixes=('_df', '_ins_list'))

        # Add a new column 'strike_changed' and set it to 1 if the strike values are different
        merged_df['strike_changed'] = (merged_df['strike_df'] != merged_df['strike_ins_list']).astype(int)
        
        # Show the resulting DataFrame
        #print("merged_df",merged_df)
        
        # Filter rows where strike_changed is 1
        rows_to_update = merged_df[merged_df['strike_changed'] == 1]
        
        #print("rows_to_update",rows_to_update)
        # Create a list to hold the SQL update statements
        update_statements = []
        
        cursor=coredb.cursor()
        # Iterate over each row and create the update SQL
        for index, row in rows_to_update.iterrows():
            # Extract values for SQL query
            scrip_generic = row['scrip_generic']
            limit_price = row['limit_price']
            #strike = row['strike_ins_list']  # Updated strike value

            # Create the UPDATE SQL statement
            sql = f"""
            UPDATE ordertracker ot
            SET scheduler_name = 'NOWSE',
            comments=CONCAT_WS('|',comments,'sl is hit')
            WHERE ot.scrip_generic = '{scrip_generic}'
              AND EXISTS (
                  SELECT 1
                  FROM orderleg ol
                  WHERE ol.strike_type = 705
                    AND ol.id = ot.OrderLegs_ID
                    AND ot.order_type NOT IN ('Open')
                    AND ot.scheduler_name LIKE '202%%'
                    AND ol.limit_price = {limit_price}
              );
            """
            #print(sql)
            cursor.execute(sql)
            coredb.commit()
            #update_statements.append(sql)


    except Exception as e:
        #print("error",str(e), str(traceback.print_exc()))
        mainlogger.exception(str(e))
        
def check_polling_strategies():
    mainlogger.info('Function:check_polling_strategies')
    
    polling_strategy_maxoi_rollover()



# %%
if __name__ == "__main__":

    broker=Broker()
    ##all account logins
    if (is_algo_hours() and env=='prod') or prod_login_all==1:
        mainlogger.info("Production market time: Login ALL accounts")
        broker.account_login(True)
        disable_login_failed_accounts()
    ##only test account logins

    #broker level change
    else:
        mainlogger.info("Testing or after market: Login only primary accounts")
        broker.check_login_token(primary_alice2)
        broker.check_login_token(primary_kite)
        broker.check_login_token(primary_kite_historical_data)
        broker.check_login_token(primary_kite2)
        broker.check_login_token(primary_angel)
        broker.check_login_token(primary_shoonya)
        broker.check_login_token(primary_iifl)  
        broker.check_login_token(primary_flat)  
        
   
    if primary_broker==zerodha_broker_code:
            primary_scrip_generic=primary_kite

def cleanup_timescaledb():
    mainlogger.info('Function : cleanup_timescaledb')
    #timescale_connection = "postgres://postgres:password@172.31.42.85:5432/postgres?connect_timeout=5"
    try:
        conn = psycopg2.connect(timescale_connection)
        pipeline = TimescalePipeline(conn)
        pipeline.timescaledb_cleanup_script()
        send_notification(group_chat_id, 'Init:cleanup_timescaledb done')
    except Exception as e:
        send_notification(group_chat_id,'TimescaleDB Cleanup Failed'+str(e))
        mainlogger.exception(str(e))

if __name__ == '__main__':
    if start_options_stream=="1" and env=='prod' :
        mainlogger.info("Starting OptionsStreamStart")
        if stream_timescale:
            cleanup_timescaledb()

        #indice_symbols_tokens=[256265,260105,257801]
        indice_symbols_tokens=[]
            
        for s in indice_symbols:
            exchange=s.split(":")[0]
            symbol=s.split(":")[1]
            ins=broker.get_instrument_by_symbol(primary_scrip_generic, exchange, symbol)
            indice_symbols_tokens.append(ins[1])
        
        options_stream_starter=OptionsStreamStart(symbols,indice_symbols,indice_symbols_tokens)
        
        #access_token=obj.kite_validation( "OO9269","#Catd0gsheep","FLPYWNL5KASBOT7WBMGGDKJNWWKJDRR4","1","h5k3umyqr6cch9gd","1qe5r7e14fj8ho3v6xgu08qcgmf7pq02")
        #api_key='h5k3umyqr6cch9gd'
        api_key=dic_accounts[primary_kite]['token'].api_key
        access_token=dic_accounts[primary_kite]['token'].access_token
        options_stream_starter.start_options_streaming(api_key,access_token)
        mainlogger.info("Started OptionsStreamStart")
    
# ### Function - Get  Expiry 

# %%

# %%

if __name__ == "__main__":
    
    exp_weekly_def=broker.get_weekly_expiry(primary_broker)
    exp_weekly_fin=broker.get_weekly_expiry(primary_broker,'FINNIFTY')
    exp_weekly_midcp=broker.get_weekly_expiry(primary_broker,'MIDCPNIFTY')
    exp_weekly_bnf=broker.get_weekly_expiry(primary_broker,'BANKNIFTY')
    exp_weekly_sensex=broker.get_weekly_expiry(primary_broker,'SENSEX','BFO')
    

    exp_monthly_def=broker.get_monthly_expiry(primary_broker)
    exp_monthly_fin=broker.get_monthly_expiry(primary_broker,'FINNIFTY')
    exp_monthly_midcp=broker.get_monthly_expiry(primary_broker,'MIDCPNIFTY')
    exp_monthly_bnf=broker.get_monthly_expiry(primary_broker,'BANKNIFTY')
    exp_monthly_sensex=broker.get_monthly_expiry(primary_broker,'SENSEX','BFO')
        


    exp_monthly_plus_def=broker.get_monthly_expiry_plus(primary_broker)
    exp_monthly_plus_fin=broker.get_monthly_expiry_plus(primary_broker,'FINNIFTY')
    exp_monthly_plus_midcp=broker.get_monthly_expiry_plus(primary_broker,'MIDCPNIFTY')
    exp_monthly_plus_bnf=broker.get_monthly_expiry_plus(primary_broker,'BANKNIFTY')
    exp_monthly_plus_sensex=broker.get_monthly_expiry_plus(primary_broker,'SENSEX','BFO')
        


    try:
        exp_future_weekly_def=broker.get_future_weekly_expiry(primary_broker)
        exp_future_weekly_fin=broker.get_future_weekly_expiry(primary_broker,'FINNIFTY')
        exp_future_weekly_midcp=broker.get_future_weekly_expiry(primary_broker,'MIDCPNIFTY')
        exp_future_weekly_bnf=broker.get_future_weekly_expiry(primary_broker,'BANKNIFTY')
        exp_future_weekly_sensex=broker.get_future_weekly_expiry(primary_broker,'SENSEX','BFO')
    except Exception as e:
        mainlogger.exception(str(e))

    try:
        exp_future_monthly_def=broker.get_future_monthly_expiry(primary_broker)
        exp_future_monthly_fin=broker.get_future_monthly_expiry(primary_broker,'FINNIFTY')
        exp_future_monthly_midcp=broker.get_future_monthly_expiry(primary_broker,'MIDCPNIFTY')
        exp_future_monthly_bnf=broker.get_future_monthly_expiry(primary_broker,'BANKNIFTY')
        exp_future_monthly_sensex=broker.get_future_monthly_expiry(primary_broker,'SENSEX','BFO')
                
    except Exception as e:
        mainlogger.exception(str(e))

    EXP_DAY_DEF=broker.is_expiry_day(exp_weekly_def)
    EXP_DAY_FIN=broker.is_expiry_day(exp_weekly_fin)
    EXP_DAY_MIDCP=broker.is_expiry_day(exp_weekly_midcp) 
    EXP_DAY_BNF=broker.is_expiry_day(exp_weekly_bnf)
    EXP_DAY_SENSEX=broker.is_expiry_day(exp_weekly_sensex)
        


    MONTHLY_EXP_DAY_DEF=broker.is_monthly_expiry_day(exp_monthly_def)
    MONTHLY_EXP_DAY_FIN=broker.is_monthly_expiry_day(exp_monthly_fin)
    MONTHLY_EXP_DAY_MIDCP=broker.is_monthly_expiry_day(exp_monthly_midcp)
    MONTHLY_EXP_DAY_BNF=broker.is_monthly_expiry_day(exp_monthly_bnf)
    MONTHLY_EXP_DAY_SENSEX=broker.is_monthly_expiry_day(exp_monthly_sensex)
        


# # Keywords Lookup
# %%
def keyword_lookup(keys):
    
    if keys is None or keys=='' or keys=='NA':
        return None
    
    l=[]
    for d in str(keys).split(","):
        try:
            l.append(dic_keywords[int(float(d))]['name'])
        except Exception as e:
            mainlogger.exception(str(e))
            l.append('keyword missing')
    
    if len(l)==1:
        return l[0]
    return l

#keyword_lookup('0')


# # refresh_advanced_instruments

# %%


def refresh_advanced_instruments(coredb):
    
    global global_instruments
    global global_candle_durations
    global sdf
    global sdf_1min
    
    
    #sql="select distinct i.symbol, i.exchange from vw_activestrategies_all va, instrument i where open_time='Advanced' and i.ID = va.base_instrument_id"
    sql=( "select distinct symbol, exchange, is_volume_indicator, nfo_symbol from (  "
 "select t.*, i.symbol as nfo_symbol  from (  "
 "select distinct i.symbol, i.exchange, indicator_name, candle_duration, expr, params, le.is_volume_indicator, "
 "va.Instrument_ID, va.instrument_type, range_start, range_end, case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as scrip_generic "
 "from vw_activestrategies_all va,orderlegsextension le, instrument i,  ordertracker ot  "
 "where va.ID=le.OrderLeg_ID  and i.ID =va.base_instrument_id and va.ID =ot.OrderLegs_ID  "
 "and  (cast(ot.created_at as date)= current_date()) and (ot.scheduler_name='Advanced') and ot.order_type='Open'  "
 "union  "
 "select distinct i.symbol, i.exchange, indicator_name, candle_duration, expr, params, le.is_volume_indicator, "
 "va.Instrument_ID, va.instrument_type, range_start, range_end, case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as scrip_generic "
 "from vw_activestrategies_all va,orderlegsextension le, instrument i,  ordertracker ot  "
 "where va.ID=le.OrderLeg_ID  and i.ID =va.base_instrument_id and va.ID =ot.OrderLegs_ID  "
 "and  (cast(ot.created_at as date) >= current_date() - interval 30 day) and (ot.scheduler_name<>'Complete')  "
 "and ot.order_type in ('Squareoff', 'Close') "
 "union  "
 "select distinct case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as symbol, case when va.instrument_type=800 and i.exchange='NSE' then 'NFO' else 'BFO' end as exchange, indicator_name, candle_duration, expr, params, le.is_volume_indicator, "
 "va.Instrument_ID, va.instrument_type, range_start, range_end, case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as scrip_generic "
 "from vw_activestrategies_all va,orderlegsextension le, instrument i,  ordertracker ot  "
 "where va.ID=le.OrderLeg_ID  and i.ID =va.base_instrument_id and va.ID =ot.OrderLegs_ID  "
 "and  (cast(ot.created_at as date)= current_date()) and (ot.scheduler_name='Advanced') and ot.order_type='Open'  "
 "union  "
 "select distinct case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as symbol, case when va.instrument_type=800 and i.exchange='NSE' then 'NFO' else 'BFO' end as exchange, indicator_name, candle_duration, expr, params, le.is_volume_indicator, "
 "va.Instrument_ID, va.instrument_type, range_start, range_end, case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as scrip_generic "
 "from vw_activestrategies_all va,orderlegsextension le, instrument i,  ordertracker ot  "
 "where va.ID=le.OrderLeg_ID  and i.ID =va.base_instrument_id and va.ID =ot.OrderLegs_ID  "
 "and  (cast(ot.created_at as date) >= current_date() - interval 30 day) and (ot.scheduler_name<>'Complete')  "
 "and ot.order_type in ('Squareoff', 'Close') "
 ")as t, instrument i  where t.Instrument_ID=i.ID "
 ")t where symbol<>'NA' ")
    
    df = pd.read_sql(sql, coredb)
    df = df.replace({np.nan: None})
    
    global_instruments=[]
    global_candle_durations=[]
    
    for index,row in df.iterrows():
        
        if row['is_volume_indicator']==1 and row['symbol'] in nse_indices:
     
            if "FIN" in row['symbol']:
                exp=exp_monthly_fin
            else:
                exp=exp_monthly_def

            ins=broker.custom_get_instrument_for_fno(primary_scrip_generic, row['nfo_symbol'], exp, True, 0, False)

        else:
            ins=broker.get_instrument_by_symbol(primary_scrip_generic,row['exchange'],row['symbol'])
        
        #append if INSTRUMENT is not None
        if ins is not None:
            global_instruments.append(ins)
    
    sql=( "select distinct candle_duration from (  "
 "select t.*, i.symbol as nfo_symbol  from (  "
 "select distinct i.symbol, i.exchange, indicator_name, candle_duration, expr, params, le.is_volume_indicator, "
 "va.Instrument_ID, va.instrument_type, range_start, range_end, case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as scrip_generic "
 "from vw_activestrategies_all va,orderlegsextension le, instrument i,  ordertracker ot  "
 "where va.ID=le.OrderLeg_ID  and i.ID =va.base_instrument_id and va.ID =ot.OrderLegs_ID  "
 "and  (cast(ot.created_at as date)= current_date()) and (ot.scheduler_name='Advanced') and ot.order_type='Open'  "
 "union  "
 "select distinct i.symbol, i.exchange, indicator_name, candle_duration, expr, params, le.is_volume_indicator, "
 "va.Instrument_ID, va.instrument_type, range_start, range_end, case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as scrip_generic "
 "from vw_activestrategies_all va,orderlegsextension le, instrument i,  ordertracker ot  "
 "where va.ID=le.OrderLeg_ID  and i.ID =va.base_instrument_id and va.ID =ot.OrderLegs_ID  "
 "and  (cast(ot.created_at as date) >= current_date() - interval 30 day) and (ot.scheduler_name<>'Complete')  "
 "and ot.order_type in ('Squareoff', 'Close') "
 "union  "
 "select distinct case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as symbol, case when va.instrument_type=800 and i.exchange='NSE' then 'NFO' else 'BFO' end as exchange, indicator_name, candle_duration, expr, params, le.is_volume_indicator, "
 "va.Instrument_ID, va.instrument_type, range_start, range_end, case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as scrip_generic "
 "from vw_activestrategies_all va,orderlegsextension le, instrument i,  ordertracker ot  "
 "where va.ID=le.OrderLeg_ID  and i.ID =va.base_instrument_id and va.ID =ot.OrderLegs_ID  "
 "and  (cast(ot.created_at as date)= current_date()) and (ot.scheduler_name='Advanced') and ot.order_type='Open'  "
 "union  "
 "select distinct case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as symbol, case when va.instrument_type=800 and i.exchange='NSE' then 'NFO' else 'BFO' end as exchange, indicator_name, candle_duration, expr, params, le.is_volume_indicator, "
 "va.Instrument_ID, va.instrument_type, range_start, range_end, case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as scrip_generic "
 "from vw_activestrategies_all va,orderlegsextension le, instrument i,  ordertracker ot  "
 "where va.ID=le.OrderLeg_ID  and i.ID =va.base_instrument_id and va.ID =ot.OrderLegs_ID  "
 "and  (cast(ot.created_at as date) >= current_date() - interval 30 day) and (ot.scheduler_name<>'Complete')  "
 "and ot.order_type in ('Squareoff', 'Close') "
 ")as t, instrument i  where t.Instrument_ID=i.ID "
 ")t where symbol<>'NA' ")
    df = pd.read_sql(sql, coredb)
    df = df.replace({np.nan: None})
    
    for index,row in df.iterrows():
        candle_duration=keyword_lookup(row['candle_duration'])
        if candle_duration.isnumeric():
            candle_duration=int(candle_duration)
        global_candle_durations.append(candle_duration)
    
    #Removed as it doesn't work in multi-processing scenario
    #broker.check_login_token(primary_kite_historical_data)  
    
    try:
        sdf=candle_obj.get_ohlc_data(dic_accounts[primary_kite_historical_data]['token'], global_instruments, global_candle_durations, global_indicators, global_number_of_candles, sdf, ignore_incomplete_candle=True)
    except Exception as e:
        mainlogger.exception(str(e) + str(global_instruments))
    
    try:
        sdf_1min=candle_obj.get_ohlc_data(dic_accounts[primary_kite_historical_data]['token'], global_instruments_1min, ['minute'], [], 3, sdf_1min, ignore_incomplete_candle=False) 
    except Exception as e:
        mainlogger.exception(str(e) + str(global_instruments))
        #mainlogger.exception(str(e) + str(global_instruments_1min))
        
    return 1

#refresh_advanced_instruments(pool.connection())


# # Refresh New Base Symbols

# %%


def refresh_new_base_symbols():
    mainlogger.info("Function:refresh_new_base_symbols")
    
    coredb = pool.connection()
    
    sql="select * from vw_trackedsymbols"
    df = pd.read_sql(sql, coredb)
    df = df.replace({np.nan: None})
    
    for index, row in df.iterrows():
        try:
            x=instruments_by_symbol[row['base_symbol']]
            ##print('found',x)
        except:
            ##print('new',row['base_symbol'])
            if row['segment']=='NFO':
                broker.subscribe_to_instrument('NSE',row['base_symbol'])
            else:
                broker.subscribe_to_instrument('BSE',row['base_symbol'])
            
    coredb.close()
    
#refresh_new_base_symbols()


# # Initialize Variables 

# %%
if __name__ == "__main__":

    try:
        mainlogger.info("Initialize: Started")
        send_notification(group_chat_id,'Initialize: Started')


        coredb = pool.connection()

        sql="select * from vw_trackedsymbols where segment='NFO'"
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})

        #instruments_by_id=df.set_index("ID", inplace = False).to_dict(orient="index")
        instruments_by_symbol=df.set_index("base_symbol", inplace = False).to_dict(orient="index")
        broker.subscribe_ltp(df['base_symbol'].to_list(),'NSE')
        
        time.sleep(10)
        
        sql="select * from vw_trackedsymbols where segment='BFO'"
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        if len(df)>0:
            #instruments_by_id=df.set_index("ID", inplace = False).to_dict(orient="index")
            df.set_index("base_symbol", inplace = False).to_dict(orient="index")
            broker.subscribe_ltp_nfo(df['base_symbol'].to_list(),'BSE')
        
        mainlogger.info("Initialize:subscribe_ltp")

        #sql="select distinct symbol  from scripltp s where cast(last_updated as date)>= cast(current_timestamp() as date) - interval 7 day and exchange='NFO'"
        #sql=("select distinct scrip_generic  as symbol from ordertracker where scrip is not null "
        #     "and cast(last_updated as date)>= cast(current_timestamp() as date) - interval 30 day "
        #     "and scheduler_name not in (select orderstatus from orderstatus where is_final_status=1) ")
        sql= ("select distinct o.scrip_generic as symbol  from ordertracker o, orderleg ol, instrument i "
              "where o.OrderLegs_ID =ol.id and ol.Instrument_ID=i.ID and i.exchange='NFO' and scrip is not null "
              "and cast(o.last_updated as date)>= cast(current_timestamp() as date) - interval 30 day "
              "and scheduler_name not in (select orderstatus from orderstatus where is_final_status=1) "
             )
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        if len(df)>0:
            broker.subscribe_ltp_nfo(df['symbol'].to_list())
        
        #BFO
        sql= ("select distinct o.scrip_generic as symbol from ordertracker o, orderleg ol, instrument i "
              "where o.OrderLegs_ID =ol.id and ol.Instrument_ID=i.ID and i.exchange='BFO' and scrip is not null "
              "and cast(o.last_updated as date)>= cast(current_timestamp() as date) - interval 30 day "
              "and scheduler_name not in (select orderstatus from orderstatus where is_final_status=1) "
             )
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        if len(df)>0:
            broker.subscribe_ltp_nfo(df['symbol'].to_list(), 'BFO')
        
        mainlogger.info("Initialize:subscribe_ltp_nfo")

        ##Keywords
        sql="select id,name from keyword"
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})

        dic_keywords=df.set_index("id", inplace = False).to_dict(orient="index")

        mainlogger.info("Initialize:keywords initialized")

        ##Advanced Strategies Candle, TA
        candle_obj = get_historical_data()
        indicator_obj = get_indicator_signal()
        '''
        if primary_broker==zerodha_broker_code:
            primary_scrip_generic=primary_kite
        '''
        #refresh_advanced_instruments(coredb)

        del df
        coredb.close()

        #broker level change
        if len(kite_instruments)==0 or len(alice_instruments)==0 or len(shoonya_instruments)==0 or len(angel_instruments)==0 or len(iifl_instruments)==0 or len(flattrade_instruments)==0:
            mainlogger.info("Initialize: Failed "+str(["kite",len(kite_instruments), "alice",len(alice_instruments), "shoonya",len(shoonya_instruments), "angel",len(angel_instruments), "flat",len(flattrade_instruments), "iifl", len(iifl_instruments)]))
            
            #send_notification(group_chat_id,'Initialize: Failed. Not all instruments loaded')
            handle_exception('Initialize: Failed. Not all instruments loaded ','NA','NA',False)
        else:
            mainlogger.info("Initialize: Succesful")
            send_notification(group_chat_id,'Initialize: Succesful')


    except Exception as e:
        print(str(e))
        mainlogger.exception(str(e))


# %%
if __name__ == "__main__":

    mainlogger.info("Primary Members: " + str(
        ['primary_kite',primary_kite,'primary_kite_historical_data',primary_kite_historical_data,'primary_alice2',primary_alice2,
                                         'primary_angel',primary_angel,'primary_scrip_generic',primary_scrip_generic,'primary_shoonya',primary_shoonya] 
                                       ))


# # Open and Close Trade

# %%
def construct_order_message(strat, account, symbol, qty, direction, order_type="Entry", price=None, error=None):
    message=""
    try:
        message+="Account: "+str(account)+"\n"
        message+="Strategy: "+str(strat)+"\n"
        message+="Message Sent: "+str(datetime.datetime.now().strftime("%H:%M:%S"))+"\n"
        message+="Order Type: "+str(order_type)+"\n"
        
        if symbol is not None:
            message+="Symbol: "+str(symbol)+"\n"
        if direction is not None:
            message+="Direction: "+str(direction).upper()+"\n"
        if qty is not None:
            message+="Qty: "+str(qty)+"\n"
        if price is not None:
            message+="Filled Price: "+str(round(price,2))+"\n"
        if error is not None:
            message+="Error: "+str(error)+"\n"
    except Exception as e:
        mainlogger.exception(str(e))  
    
    return message

# %%
    
def validate_response(otid,response,orderstatus=False):
    mainlogger.info('Function: validate_response') 
    
    try:
        if orderstatus==False and response['status'].lower() == 'rejected':
            queue_exception(response['message'], otid)
            
        elif orderstatus==True and response['order_status'].lower() == 'rejected':
            queue_exception(response['rejection_reason'], otid)
            
        elif orderstatus==True and response['order_status'].lower() == 'not_found':
            queue_exception(response['message'], otid)
     
    except Exception as e:
        mainlogger.exception(str(e))    
        

def get_contract_expiry(row):
    mainlogger.info('Function: get_contract_expiry') 
    
    #HANDLE FINNIFTY EXPIRY
    if row['symbol']=='BANKNIFTY':
        exp_weekly=exp_weekly_bnf
        exp_monthly=exp_monthly_bnf
        exp_monthly_plus=exp_monthly_plus_bnf
        exp_future_weekly=exp_future_weekly_bnf
        exp_future_monthly=exp_future_monthly_bnf
        EXP_DAY=EXP_DAY_BNF
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_BNF
        
    elif row['symbol']=='FINNIFTY':
        exp_weekly=exp_weekly_fin
        exp_monthly=exp_monthly_fin
        exp_monthly_plus=exp_monthly_plus_fin
        exp_future_weekly=exp_future_weekly_fin
        exp_future_monthly=exp_future_monthly_fin
        EXP_DAY=EXP_DAY_FIN
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_FIN

    elif row['symbol']=='MIDCPNIFTY':
        exp_weekly=exp_weekly_midcp
        exp_monthly=exp_monthly_midcp
        exp_monthly_plus=exp_monthly_plus_midcp
        exp_future_weekly=exp_future_weekly_midcp
        exp_future_monthly=exp_future_monthly_midcp
        EXP_DAY=EXP_DAY_MIDCP
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_MIDCP
        
    elif row['symbol']=='SENSEX':
        exp_weekly=exp_weekly_sensex
        exp_monthly=exp_monthly_sensex
        exp_monthly_plus=exp_monthly_plus_sensex
        exp_future_weekly=exp_future_weekly_sensex
        exp_future_monthly=exp_future_monthly_sensex
        EXP_DAY=EXP_DAY_SENSEX
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_SENSEX
        
    else:
        exp_weekly=exp_weekly_def
        exp_monthly=exp_monthly_def
        exp_monthly_plus=exp_monthly_plus_def
        exp_future_weekly=exp_future_weekly_def
        exp_future_monthly=exp_future_monthly_def
        EXP_DAY=EXP_DAY_DEF
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_DEF
        

    if keyword_lookup(row['expiry_type'])=='weekly':
        exp= exp_weekly
        
    elif keyword_lookup(row['expiry_type'])=='monthly':
        exp= exp_monthly
        
    elif keyword_lookup(row['expiry_type'])=='monthly_plus':
        exp= exp_monthly_plus

    elif keyword_lookup(row['expiry_type'])=='future_weekly':
        exp= exp_future_weekly[row['future_expiry_index']]

    elif keyword_lookup(row['expiry_type'])=='future_monthly':
        exp= exp_future_monthly[row['future_expiry_index']]

    return exp

def options_interest(row, ce, exp, closer=0,oi_type='PREMIUM'):
    mainlogger.info('Function:options_interest  ' + str(row['order_tracker_id'])) 
    
    fail_upd_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s "
           " where ID=%(ordertracker_id)s" 
          )
    
    ce_pick,pe_pick=datafeed().get_cepe_picks(row['low_limit_price'],
                                                          row['limit_price'],
                                                          row['symbol'].lower(),
                                                          datetime.datetime.strptime(exp, '%d %b%y').date().strftime('%d %b %Y'),
                                                          row['exchange'],
                                                          closer,
                                                          oi_type
                                                         )
    mainlogger.info('Hedge Strikes Received:' + str([ce_pick,pe_pick]))

    if ce==True:
        if str(type(ce_pick))!="<class 'pandas.core.series.Series'>":
            ## Update Order Status in DB no ma ching strike for premium
            dic={
                 'ordertracker_id':row['order_tracker_id'],
                 'scheduler_name': 'Rejected'
                }

            queue_db_updates(row['order_tracker_id'], fail_upd_sql, dic)

            msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], None, None,keyword_lookup(row['direction']),"Entry", None, 'Order Not Placed: No suitable strike found')
            send_notification(
                 dic_accounts[row['Account_ID']]['chat_id'], emoji_warning+"\n"+msg)
            #send_notification(
            #    dic_accounts[row['Account_ID']]['chat_id'],
            #    emoji_warning+'Order Not Placed: No suitable strike found for '+dic_accounts[row['Account_ID']]['username']+" - "+row['name'])
            return -1

        strike=ce_pick['stkPrc']

    else:
        if str(type(pe_pick))!="<class 'pandas.core.series.Series'>":
            ## Update Order Status in DB
            dic={
                 'ordertracker_id':row['order_tracker_id'],
                 'scheduler_name': 'Rejected'
                }
            queue_db_updates(row['order_tracker_id'],fail_upd_sql,dic)

            msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], None, None,keyword_lookup(row['direction']),"Entry", None, 'Order Not Placed: No suitable strike found')

            send_notification(
                dic_accounts[row['Account_ID']]['chat_id'],
                emoji_warning+'\n'+msg)
            return -1

        strike=pe_pick['stkPrc']
        
    return strike
        
def straddle_premium_strike(row, ins_ce, ins_pe, gen_ins_ce, gen_ins_pe, ce, exp, strike_type):
    mainlogger.info('Function:straddle_premium  ' + str(row['order_tracker_id'])) 
    
    ltp_ce=broker.redis_get_ltp(row['exchange'],gen_ins_ce[2])
    if ltp_ce==-1:
        ltp_ce=float(broker.get_ltp(row['Account_ID'],row['exchange'],ins_ce[2]))    
    
    ltp_pe=broker.redis_get_ltp(row['exchange'],gen_ins_pe[2])
    if ltp_pe==-1:
        ltp_pe=float(broker.get_ltp(row['Account_ID'],row['exchange'],ins_pe[2]))
    
    total_premium=ltp_ce+ltp_pe
    preimum=round(total_premium*(row['strike_diff']/100),2)
    
    mainlogger.info('Function:straddle_premium  total_premium' + str(total_premium)) 
    #update row with premium details 
    row['low_limit_price']=0.1
    row['limit_price']=preimum
    
    if strike_type=='STRADDLE_PREMIUM_CLOSE':
        closer=1
    elif strike_type=='STRADDLE_PREMIUM_GREATER':
        closer=2
        row['low_limit_price']=preimum
        row['limit_price']=100000
    else:
        closer=0
        
    strike =options_interest(row, ce, exp, closer)
    
    return strike
    
def open_trade(row,coredb,tmp_instruments_by_symbol,base_symbol_ltp=None):
    #mainlogger.info('alice len'+str(len(kite_instruments)))
    
    mainlogger.info('Function:open_trade  ' + str(row['order_tracker_id'])) 
    
    upd_sql = ("UPDATE ordertracker set Account_ID=%(Account_ID)s, order_id= %(order_id)s, order_response=%(order_response)s, scheduler_name=%(scheduler_name)s,scrip=%(scrip)s,scrip_generic=%(scrip_generic)s,direction=%(direction)s, trigger_price=%(trigger_price)s, limit_price=%(limit_price)s, filled_price=%(filled_price)s, filled_qty=%(filled_qty)s, ot_order_type=%(ot_order_type)s, last_updated=%(last_updated)s "
           " where ID=%(ordertracker_id)s" 
          )
    
    upd_wnt_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s, wnt_reference_price=%(wnt_reference_price)s, wnt_strategy_end_time=%(wnt_strategy_end_time)s, scrip_generic=%(scrip_generic)s, scrip=%(scrip)s, direction=%(direction)s"
           " where ID=%(ordertracker_id)s" 
          )
    
    upd_instr_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s, sched_order_time=%(sched_order_time)s, scrip_generic=%(scrip_generic)s, scrip=%(scrip)s"
           " where ID=%(ordertracker_id)s" 
          )
    
    fail_upd_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s "
           " where ID=%(ordertracker_id)s" 
          )
    
    re_fail_upd_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s, reentry_max=0, tp_reentry_max=0 "
           " where ID=%(ordertracker_id)s" 
          )
    
    ins_sql = ("INSERT INTO ordertracker (Account_ID,AccountGroup_ID,StrategyGroup_ID,Strategy_ID,OrderLegs_ID,order_group_id,order_type,sched_order_time,scheduler_name,order_id,order_response,scrip,scrip_generic,direction,trigger_price,limit_price,parent_order_id, parent_filled_price, parent_filled_qty,filled_qty, sl_scheduler_name, amo_date, ot_order_type, reentry_max, tp_reentry_max,wnt_strategy_end_time) "
           " VALUES (%(Account_ID)s,%(AccountGroup_ID)s, %(StrategyGroup_ID)s, %(Strategy_ID)s, %(ID)s, %(order_group_id)s, %(order_type)s, %(sched_order_time)s, %(scheduler_name)s, %(order_id)s, %(order_response)s, %(scrip)s, %(scrip_generic)s, %(direction)s, %(trigger_price)s, %(limit_price)s, %(parent_order_id)s, %(parent_filled_price)s, %(parent_filled_qty)s, %(filled_qty)s, %(sl_scheduler_name)s, %(amo_date)s, %(ot_order_type)s, %(ot_reentry_max)s, %(ot_tp_reentry_max)s,%(wnt_strategy_end_time)s)")
    
    upd_orderleg_exec_sql=("SELECT update_orderleg_exec(%(OrderLegs_ID)s)")
    
    ##RE Reset Scenario update LP and TP as SL may have changed 
    upd_tp_lp = ("UPDATE ordertracker set limit_price=%(limit_price)s, trigger_price=%(trigger_price)s  "
           "where Account_ID=%(Account_ID)s and order_id=%(order_id)s" 
          )
    ##print(row)
    
    convert_to_market=True
    
    #Auth Failed and Disabled casses
    if row['Account_ID'] is None or dic_accounts[row['Account_ID']]['token'] is None:
        mainlogger.info('Skipped Order for this account : '+ str(row['Account_ID']))
        try:
            fail_upd_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s "
           " where ID=%(ordertracker_id)s" 
          )
            dic={
                 'ordertracker_id':row['order_tracker_id'],
                 'scheduler_name': 'Error'
                }

            queue_db_updates(row['order_tracker_id'], fail_upd_sql, dic)

            queue_exception('Login token error: Re-verify credentials and Retry Failed Legs', row['order_tracker_id'])
        except Exception as e:
            mainlogger.exception(str(e))    
         
        return
    
    #Otherwise continue
    if (row['exchange'] == 'NFO' or row['exchange'] == 'BFO' or row['exchange'] == 'NSE'):
        
        strike_type=keyword_lookup(row['strike_type'])
        
        if (row['exchange'] == 'NFO' or row['exchange'] == 'BFO'):
            
            ltp=base_symbol_ltp
            
            exp=get_contract_expiry(row)
                
            fut= False if keyword_lookup(row['segment'])=='option' else True
            ce= True if keyword_lookup(row['option_type'])=='CE' else False
            
            ##ATM
            strike=row['round_figure']*round(ltp/row['round_figure']) #nearest 0 logic
            
            if strike_type=='ATM':
                pass
            
            elif('STRADDLE_PREMIUM' in strike_type and row['strike_diff']!=None):
                ins_ce=broker.custom_get_instrument_for_fno(row['Account_ID'],row['symbol'], exp, fut, strike, True, row['exchange'])
                ins_pe=broker.custom_get_instrument_for_fno(row['Account_ID'],row['symbol'], exp, fut, strike, False, row['exchange'])
                
                gen_ins_ce=broker.custom_get_instrument_for_fno(primary_scrip_generic,row['symbol'], exp, fut, strike, True, row['exchange'])
                gen_ins_pe=broker.custom_get_instrument_for_fno(primary_scrip_generic,row['symbol'], exp, fut, strike, False, row['exchange'])
                
                #override with new strike
                strike=straddle_premium_strike(row, ins_ce, ins_pe, gen_ins_ce, gen_ins_pe, ce, exp,strike_type)
                
                if strike == -1:
                    return 1
                
                    
            elif(strike_type=='ATM_PERC' and row['strike_diff']!=None):
                strike=strike+(strike*(row['strike_diff']/100))
                strike=row['round_figure']*round(strike/row['round_figure']) #nearest 0 logic            
                    
            elif(strike_type=='ITM' and row['strike_diff']!=None):
                if ce==True:
                    strike=strike-row['strike_diff']
                else:
                    strike=strike+row['strike_diff']

            elif(strike_type=='OTM' and row['strike_diff']!=None):
                if ce==True:
                    strike=strike+row['strike_diff']
                else:
                    strike=strike-row['strike_diff']
            
            elif strike_type=="OI":
                strike=options_interest(row, ce, exp)
                if strike == -1:
                    return 1
            
            elif strike_type=="OI_CLOSER":
                strike=options_interest(row, ce, exp,1)
                if strike == -1:
                    return 1
                
            elif strike_type=="OI_GREATER":
                strike=options_interest(row, ce, exp,2)
                if strike == -1:
                    return 1
                
            elif strike_type=="MAXOPENINTROLL":
                strike=options_interest(row, ce, exp,0,oi_type='MAXOPENINT')
                if strike == -1:
                    return 1
                
            elif(strike_type=='OI_DELTA'):
                #ce_pick,pe_pick=datafeed().get_cepe_picks(1,row['limit_price'],row['symbol'].lower(),datetime.datetime.strptime(exp, '%d %b%y').date().strftime('%d %b %Y'))
                ltp=base_symbol_ltp
                    
                mainlogger.info('Greek Inputs: ' + str([0.001, row['limit_price'], row['symbol'].lower(), datetime.datetime.strptime(exp, '%d %b%y').date().strftime('%d %b %Y'), ltp, 'delta', row['exchange']]))
                ce_pick,pe_pick=datafeed().get_greek_picks(0.001,
                                                           row['limit_price'],
                                                           row['symbol'].lower(),
                                                           datetime.datetime.strptime(exp, '%d %b%y').date().strftime('%d %b %Y'),
                                                           ltp,
                                                           'delta', row['exchange'])

                mainlogger.info('Greek Strikes Received:' + str([ce_pick,pe_pick]))
                
                if ce==True:
                    if str(type(ce_pick))!="<class 'pandas.core.series.Series'>":
                        ## Update Order Status in DB no ma ching strike for premium
                        dic={
                             'ordertracker_id':row['order_tracker_id'],
                             'scheduler_name': 'Rejected'
                            }
                        queue_db_updates(row['order_tracker_id'],fail_upd_sql,dic)
                        
                        msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], None, None,keyword_lookup(row['direction']),"Entry", None, 'Order Not Placed: No suitable strike found')
                        
                        send_notification(dic_accounts[row['Account_ID']]['chat_id'],emoji_warning+"\n"+msg)
                        return 1
                    
                    strike=ce_pick['stkPrc']
                    
                else:
                    if str(type(pe_pick))!="<class 'pandas.core.series.Series'>":
                        ## Update Order Status in DB
                        dic={
                             'ordertracker_id':row['order_tracker_id'],
                             'scheduler_name': 'Rejected'
                            }
                        queue_db_updates(row['order_tracker_id'],fail_upd_sql,dic)
                        
                        msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], None, None,keyword_lookup(row['direction']),"Entry", None, 'Order Not Placed: No suitable strike found')
                        
                        send_notification(dic_accounts[row['Account_ID']]['chat_id'],emoji_warning+"\n"+msg)
                        return 1
                    
                    strike=pe_pick['stkPrc']
            
            # based on strike derive symbol
            ins=broker.custom_get_instrument_for_fno(row['Account_ID'],row['symbol'], exp, fut, strike, ce, row['exchange'])
            alice_ins=broker.custom_get_instrument_for_fno(primary_scrip_generic,row['symbol'], exp, fut, strike, ce,row['exchange'])
            lotSize=int(ins[5])
            
            #add it to watchlist of symbols
            broker.subscribe_to_instrument(row['exchange'], alice_ins[2])
            
        elif (row['exchange'] == 'NSE'):
            lotSize=1
            #ins=dic_accounts[primary_alice]['token'].get_instrument_by_symbol(row['exchange'],row['symbol'])
            ins=broker.get_instrument_by_symbol(row['Account_ID'],row['exchange'],row['symbol'])
            alice_ins=broker.get_instrument_by_symbol(primary_scrip_generic,row['exchange'],row['symbol'])            
           
            #add it to watchlist of symbols
            broker.subscribe_to_instrument(row['exchange'],alice_ins[2])
            
        symbol=ins[2]
    
        qty=int(lotSize*row['qty'])
        
        ##RE-ENTRY order - original strike
        if row['scheduler_name']=='NOWRE' and int(row['reentry_condition']) in (1,2): 
            direction=row['ot_direction']
            symbol=row['scrip']
            alice_ins=alice_ins._replace(symbol = row['scrip_generic'])
   
            
        ##RE-Execute order - current strike
        elif row['scheduler_name']=='NOWRE' and int(row['reentry_condition'])==3:
            direction=row['ot_direction']
            #symbol=row['scrip']
            #alice_ins=alice_ins._replace(symbol = row['scrip_generic'])
            
        ##WNT with Underlying Instrument 
        elif row['scheduler_name']=='NOWWNT' and row['instrument_type']==802:
            direction=row['ot_direction']
            #symbol=row['scrip']
            #alice_ins=alice_ins._replace(symbol = row['scrip_generic'])
            
         ##WNT with Instrument 
        elif row['scheduler_name']=='NOWWNT':
            direction=row['ot_direction']
            symbol=row['scrip']
            alice_ins=alice_ins._replace(symbol = row['scrip_generic'])
            
        ##Advanced Strats on Instrument like RBO    
        elif row['scheduler_name']=='NOW' and row['instrument_type']==800:
            direction=keyword_lookup(row['direction'])
            symbol=row['scrip']
            alice_ins=alice_ins._replace(symbol = row['scrip_generic'])
            
        ##NORMAL TRADES
        else:
            direction=keyword_lookup(row['direction'])
            

        triggerPrice=0.0
        price=0.0
        misFlag= 'NRML' if row['intraday']==0 else 'MIS'
        strategyName=row['order_group_id']
        amoFlag=False
        order_type=row['leg_order_type']
        
        if '202' in row['scheduler_name']:
            while datetime.datetime.now() < datetime.datetime.strptime(row['scheduler_name'], '%Y%m%d%H%M'):
                time.sleep(0.10)
                
        if row['instrument_type']==800 and ('202' in row['scheduler_name']):
            
            dic={'ordertracker_id':row['order_tracker_id'],
                 'scheduler_name': 'Advanced',
                 'sched_order_time': datetime.datetime.combine(datetime.datetime.now(ist).date(),  datetime.time(15, 30)),
                 'scrip_generic': alice_ins[2],
                 'scrip': symbol
                }
            
            queue_db_updates(row['order_tracker_id'],upd_instr_sql,dic)
            
            mainlogger.info('Instrument: Captured')
            return 1
       
        ##WNT ORDER Stop At this block - WNT works with RE and waits for WNT conditions to match    
        elif row['wnt_condition']!='0' and ((row['scheduler_name']=='NOWRE' and (int(row['reentry_condition']) in (2,3) or int(row['tp_reentry_condition']) in (2,3))) or row['scheduler_name'] not in ('NOWRE', 'NOWWNT', 'NOWSE', 'EXITNOWUI')):
            
            #store uderlying index details
            if row['instrument_type']==802:
                wnt_reference_price=base_symbol_ltp
                symbol=row['base_symbol']
                alice_ins=alice_ins._replace(symbol = row['base_symbol'])
            
            #instrument details from OT
                
            else:
                #WNT in combination with Advanced strategies     
                try:
                    wnt_reference_price=broker.redis_get_ltp(row['exchange'],alice_ins[2])
                    if wnt_reference_price==-1:
                        wnt_reference_price=float(broker.get_ltp(row['Account_ID'],row['exchange'],symbol))
                except:
                    time.sleep(3)
                    wnt_reference_price=float(broker.get_ltp(row['Account_ID'],row['exchange'],symbol))

            if row['close_day']!=None and row['close_time']!=None:
                wnt_strategy_end_time=datetime.datetime.strptime(get_close_datetime(row), '%m-%d-%y %H:%M')
            else:
                dt=datetime.datetime.now(ist)
                wnt_strategy_end_time=(dt+datetime.timedelta(hours=10)).replace(second=0, microsecond=0) ##to make sure it is considered for WNT and RE-Entry intrady only
            
            dic={'ordertracker_id':row['order_tracker_id'],
                 'scheduler_name': 'WNT',
                 'scrip_generic': alice_ins[2],
                 'scrip': symbol,
                 'direction':direction,
                 'wnt_reference_price': wnt_reference_price,
                 'wnt_strategy_end_time':wnt_strategy_end_time
                }
            
            queue_db_updates(row['order_tracker_id'],upd_wnt_sql,dic)
            
            mainlogger.info('WNT Order: Captured')
            return 1
        
        
        ## remaining orders continue their execution
        
        ##use ltp for limit orders
        try:
            price=instruments_by_symbol[alice_ins[2]]['ltp']
            '''
            #additonal check to prevent limit exceeding in glitches 
            if  strike_type=='OI' and price>row['limit_price'] and row['scheduler_name'].startswith('202'):
                #price is way above limit price not convert to market
                if price>row['limit_price']*1.20:
                    convert_to_market=False
                    
                price=row['limit_price']
                order_type='LIMIT'
            '''    
        except:
            price=0.0
        
                
        main_order_response=broker.punch_order(row['Account_ID'], symbol,direction, qty, triggerPrice, price, 
                order_type, misFlag, amoFlag, row['exchange'], strategyName)
        
        mainlogger.info('Open Order Response:' + str(main_order_response))
        validate_response(row['order_tracker_id'],main_order_response)
        
        ##SL Logic
        if main_order_response['data']['oms_order_id']==zerodha_failed_order_hardcoded_id:
            order_status={'order_status':'rejected', 'average_price':0}
        else:
            order_status=broker.get_order_history(row['Account_ID'],main_order_response['data']['oms_order_id'])
            time.sleep(1)
        ##order_status['order_status']='complete' #only for testing

        reject_reason='Order Rejected by Broker for Strategy '

        i=0
        while order_status['order_status'].lower() != 'rejected' and order_status['order_status'].lower() !='complete':
            order_status=broker.get_order_history(row['Account_ID'],main_order_response['data']['oms_order_id'])
            time.sleep(1)
            
            #wait for 10 seconds for MARKET or LIMIT order to fill
            i=i+1
            if i==10:
                mainlogger.info('Open Order Wait Time Exceeded')
                
                #change from LIMIT to MARKET order after waiting time exceeded
                if row['exchange'] in ('NFO','BFO') and row['leg_order_type'] == 'LIMIT' and convert_to_market == True:
                    try:
                        r=broker.exit_order(row['Account_ID'], row['exchange'], direction, symbol, main_order_response['data']['oms_order_id'], qty, order_status['product'],'MARKET',None,order_status['order_tag'])
                        order_status=broker.get_order_history(row['Account_ID'],main_order_response['data']['oms_order_id'])
                        validate_response(row['order_tracker_id'],r)
                        
                    except Exception as e:
                        mainlogger.exception(str(e))
                                          
                    #wait for additional 5 seconds for Limit to Market converted order to fill
                    i=0
                    while order_status['order_status'].lower() != 'rejected' and order_status['order_status'].lower() !='complete':
                        order_status=broker.get_order_history(row['Account_ID'],main_order_response['data']['oms_order_id'])
                        time.sleep(1)
                        
                        i=i+1
                        if i==5:
                            
                            #wait for additional 2 minutes in case of delayed orders
                            i=0
                            while order_status['order_status'].lower() != 'rejected' and order_status['order_status'].lower() !='complete':
                                order_status=broker.get_order_history(row['Account_ID'],main_order_response['data']['oms_order_id'])
                                time.sleep(10)
                                i=i+1
                                if i==6:
                                    mainlogger.info('Market Order Wait Time Exceeded.Placing best case Algo SL. ' + str(order_status))
                                    #order_status['order_status'] = 'rejected' #exit loop logic to avoid infinite waiting
                                    #order_status['average_price']=0
                                    #reject_reason='Order Tooke more than 2 minutes to Fill. Manage this leg manually. Check Your Broker Terminal.'
                                    #convert to Algo SL
                                    order_status['order_status'] = 'complete' #exit loop logic to avoid infinite waiting
                                    #order_status['average_price']=instruments_by_symbol[alice_ins[2]]['ltp']
                                    ltp=broker.redis_get_ltp(row['exchange'],alice_ins[2])
                                    if ltp==-1:
                                        ltp=float(broker.get_ltp(row['Account_ID'],row['exchange'],symbol))
                                    order_status['average_price']=ltp
                                    reject_reason='Order Tooke more than 1 minutes to Fill. Placing best case Algo SL.'
                            break
                    break
                    
                else:
                    #wait for additional 2 minutes in case of delayed orders
                    i=0
                    while order_status['order_status'].lower() != 'rejected' and order_status['order_status'].lower() !='complete':
                        order_status=broker.get_order_history(row['Account_ID'],main_order_response['data']['oms_order_id'])
                        time.sleep(10)
                        i=i+1
                        if i==6:
                            mainlogger.info('Market Order Wait Time Exceeded.Placing best case Algo SL. ' + str(order_status))
                            #order_status['order_status'] = 'rejected' #exit loop logic to avoid infinite waiting
                            #order_status['average_price']=0
                            #reject_reason='Order Tooke more than 2 minutes to Fill. Manage this leg manually. Check Your Broker Terminal.'
                            #convert to Algo SL
                            order_status['order_status'] = 'complete' #exit loop logic to avoid infinite waiting
                            #order_status['average_price']=instruments_by_symbol[alice_ins[2]]['ltp']
                            ltp=broker.redis_get_ltp(row['exchange'],alice_ins[2])
                            if ltp==-1:
                                ltp=float(broker.get_ltp(row['Account_ID'],row['exchange'],symbol))
                            order_status['average_price']=ltp

                            reject_reason='Order Tooke more than 1 minutes to Fill. Placing best case Algo SL.'
                    break
        
        #order didn't fail with exception so broker will have reject reason which we can log
        if main_order_response['data']['oms_order_id']!=zerodha_failed_order_hardcoded_id:
            validate_response(row['order_tracker_id'],order_status,True)
        
        db_status='Unknown'
        if order_status['order_status'].lower()=='complete':
            db_status='Complete'
        elif order_status['order_status'].lower()=='rejected':
            db_status='Rejected'
            order_status['average_price']=0
        
        
        ## Update Order Status in DB
        dic={ 'Account_ID':row['Account_ID'],
             'order_id':main_order_response['data']['oms_order_id'],
             'order_response':order_status['order_status'].lower(),
             'ordertracker_id':row['order_tracker_id'],
             'filled_price':order_status['average_price'],
             'scheduler_name': db_status,
             'scrip':symbol,
             'scrip_generic': alice_ins[2],
             'direction':direction,
             'trigger_price':triggerPrice,
             'limit_price':price,
             'filled_qty':qty,
             'ot_order_type':order_type,
             'last_updated':datetime.datetime.now()
            }
        queue_db_updates(row['order_tracker_id'],upd_sql,dic)
        #cursor.execute(upd_sql,dic)
        
        #controls once a month kind of legs
        if(row['scheduler_type']!=None):
            dic={'OrderLegs_ID':row['ID']}
            queue_db_updates(row['order_tracker_id'],upd_orderleg_exec_sql,dic)
            #cursor.execute(upd_orderleg_exec_sql,dic)
            
        
        #Case-1: Rejected Order
        if(order_status['order_status'].lower()=='rejected'):
        
            msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], symbol, qty,keyword_lookup(row['direction']),"Entry", None, reject_reason)
        
            send_notification(dic_accounts[row['Account_ID']]['chat_id'],emoji_warning+"\n"+msg)
        
        #Case-2: Order with SL (with or without exit time)
        elif(row['stop_loss_type']!=None):
            
            sl=row['stop_loss']
            sl_type=keyword_lookup(row['stop_loss_type'])

            if '_PERC' in sl_type:
                if direction=='sell':
                    sl=(100+sl)/100
                else:
                    sl=(100-sl)/100 
                sl_order_price=round(sl*order_status['average_price'],1)

            elif '_PTS' in sl_type:
                if direction=='buy' or direction=='b':
                    sl=sl*-1
                sl_order_price=round(sl+order_status['average_price'],1)

            
            ##this IF is unused code as SLM isn't allowed anymore by brokers
            if sl_type=='SLM_PERC' or sl_type=='SLM_PTS':
                triggerPrice=sl_order_price
                price=0.0
                
                #flip direction
                direction= 'buy' if direction=='sell' else 'sell'
                
                sl_order_response=broker.punch_order(row['Account_ID'], symbol, direction, qty, triggerPrice, price, 
                    'SLM', misFlag, amoFlag, row['exchange'], strategyName)
                
                mainlogger.info('Open SL Order Response:' + str(sl_order_response))
                validate_response(row['order_tracker_id'],sl_order_response)
                
                sl_order_status=broker.get_order_history(row['Account_ID'],sl_order_response['data']['oms_order_id'])
                if(sl_order_status['order_status'].lower()=='rejected'):
                
                    msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], symbol, qty,keyword_lookup(row['direction']),"SL", None, 'SL Rejected so converted to Algo SL. No manual intervention needed.')

                    send_notification(dic_accounts[row['Account_ID']]['chat_id'],emoji_warning+"\n"+msg)
                    #send_notification(dic_accounts[row['Account_ID']]['chat_id'],emoji_warning+'Stop Loss Rejected by Broker for Strategy '+str([row['name'],symbol]))
        

            elif sl_type=='SLL_PERC' or  sl_type=='SLL_PTS':
                if direction=='sell':
                    slip=(100+row['slippage'])/100
                else:
                    slip=(100-row['slippage'])/100

                triggerPrice=sl_order_price
                price=round(slip*sl_order_price,1)
                
                ##Handle Negative SL scenraios - for example SL points of 1000 rs when sold price is 500 --> -500 SL
                if triggerPrice<=0 or price<=0:
                    triggerPrice=1
                    price=1
                
                #flip direction
                direction= 'buy' if direction=='sell' else 'sell'
                
                sl_order_response=broker.punch_order(row['Account_ID'], symbol, direction, qty, triggerPrice, price, 
                    'SLL', misFlag, amoFlag, row['exchange'], strategyName)
                
                mainlogger.info('Open SL Order Response:' + str(sl_order_response))
                validate_response(row['order_tracker_id'],sl_order_response)
                
                ##"Algo Monitored SLs" orderID:-12345, place LIMIT orders when SL is seen by algo
                
                sl_order_status=broker.get_order_history(row['Account_ID'],sl_order_response['data']['oms_order_id'])
                if(sl_order_status['order_status'].lower()=='rejected'):
                    msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], symbol, qty,keyword_lookup(row['direction']),"SL", None, 'SL Rejected so converted to Algo SL. No manual intervention needed.')

                    send_notification(dic_accounts[row['Account_ID']]['chat_id'],emoji_warning+"\n"+msg)
        
            elif sl_type=='ALGO_SLL_PERC' or sl_type=='ALGO_SLL_PTS':
                asl_slip=1
                if direction=='sell':
                    slip=(100+row['slippage'])/100
                    asl_slip=0.99
                else:
                    slip=(100-row['slippage'])/100
                    asl_slip=1.01

                triggerPrice=round(sl_order_price*asl_slip,1) #asl_slip to handle ASL -ve slippage
                price=round(slip*sl_order_price,1)
                
                ##Handle Negative SL scenraios - for example SL points of 1000 rs when sold price is 500 --> -500 SL
                if triggerPrice<=0 or price<=0:
                    triggerPrice=1
                    price=1
                
                #flip direction
                direction= 'buy' if direction=='sell' else 'sell'
                
                mainlogger.info('Open SL Order Response:Algo SL Placed')
                
            else:
                direction= 'buy' if direction=='sell' else 'sell'
             
            dic=row.to_dict()

            if row['close_time']!=None:
                
                #IF is for WNT close time should be based on when WNT was captured 
                if row['wnt_strategy_end_time'] is not None and not pd.isnull(row['wnt_strategy_end_time']):
                    mainlogger.info('use wnt_strategy_end_time as close time '+str(row['wnt_strategy_end_time']))
                    close_time=row['wnt_strategy_end_time']
                else:
                    close_time=datetime.datetime.strptime(get_close_datetime(row), '%m-%d-%y %H:%M')
                    
                dic['scheduler_name']=close_time.strftime('%Y%m%d%H%M')
                dic['sched_order_time']=close_time
                dic['wnt_strategy_end_time']=close_time  #for all cases introduce strategy end time in wnt_strategy_end_time
            else:
                dic['scheduler_name']='Manual'
                dt=datetime.datetime.now(ist)
                dic['sched_order_time']=(dt+datetime.timedelta(hours=10)).replace(second=0, microsecond=0) ##to make sure it is considered for WNT and RE-Entry intrady only
                #dic['sched_order_time']=None
                    
            dic['scrip']=symbol
            dic['scrip_generic']= alice_ins[2]
            dic['direction']=direction
            dic['parent_order_id']=main_order_response['data']['oms_order_id']
            dic['parent_filled_price']=order_status['average_price']
            dic['parent_filled_qty']=qty
            dic['filled_qty']=qty
            dic['ot_order_type']=order_type
            dic['ot_reentry_max']=row['ot_reentry_max']
            dic['ot_tp_reentry_max']=row['ot_tp_reentry_max']
            
            
            if row['schedule_sl_time']!=None:
                dt=datetime.datetime.now(ist)
                delta=global_delta_next_trading_day
                sl_scheduler_time=(dt+ datetime.timedelta(days=delta)).strftime("%m-%d-%y")+' '+row['schedule_sl_time']
                sl_scheduler_time=datetime.datetime.strptime(sl_scheduler_time, '%m-%d-%y %H:%M')
                dic['sl_scheduler_name']=sl_scheduler_time.strftime('%Y%m%d%H%M')
                
            else:
                dic['sl_scheduler_name']=None
            
            if sl_type!='Advanced':
                dic['trigger_price']=triggerPrice
                dic['limit_price']=price
                
                if sl_type=='ALGO_SLL_PERC' or  sl_type=='ALGO_SLL_PTS': 
                    ##squareoff for algo monitored SLs
                    dic['order_type']='Squareoff'
                    dic['amo_date']=amo_dummy_date
                    
                    dic['order_id']=None
                    dic['order_response']=None
                else:
                    if(sl_order_status['order_status'].lower()=='rejected'):
                        ##convert to ALGO SL for SL rejected scenarios
                        dic['order_type']='Squareoff'
                        dic['amo_date']=amo_dummy_date

                        dic['order_id']=None
                        dic['order_response']=None
                    else:
                        ##Close for Broker SLs
                        dic['order_type']='Close'
                        dic['amo_date']=None

                        dic['order_id']=sl_order_response['data']['oms_order_id']
                        dic['order_response']=sl_order_status['order_status']

                
            elif sl_type=='Advanced':
                dic['order_id']=None
                dic['order_response']=None
                dic['trigger_price']=None
                dic['limit_price']=None
                
                dic['amo_date']=amo_dummy_date
                dic['order_type']='Squareoff'
            
            dic['AccountGroup_ID']=row['AccountGroup_ID']
            dic['StrategyGroup_ID']=row['StrategyGroup_ID']
            
            ##print(ins_sql,dic)
            
            queue_db_updates(row['order_tracker_id'],ins_sql,dic)
            #cursor.execute(ins_sql,dic)
        
        #Case-3: Order without any SL but with  exit time
        elif(row['close_time']!=None):
            
            #IF is for WNT close time should be based on when WNT was captured 
            if row['wnt_strategy_end_time'] is not None and not pd.isnull(row['wnt_strategy_end_time']):
                mainlogger.info('use wnt_strategy_end_time as close time '+str(row['wnt_strategy_end_time']))
                close_time=row['wnt_strategy_end_time']
            else:
                close_time=datetime.datetime.strptime(get_close_datetime(row), '%m-%d-%y %H:%M')
            #close_time=datetime.datetime.strptime(get_close_datetime(row), '%m-%d-%y %H:%M')
            
            direction= 'buy' if direction=='sell' else 'sell'
            
            dic=row.to_dict()
            
            dic['scheduler_name']=close_time.strftime('%Y%m%d%H%M')
            dic['sched_order_time']=close_time
            dic['wnt_strategy_end_time']=close_time  #for all cases introduce strategy end time in wnt_strategy_end_time
            dic['order_type']='Squareoff'
            dic['ot_order_type']=order_type
            
            dic['scrip']=symbol
            dic['scrip_generic']= alice_ins[2]
            dic['direction']=direction
            dic['parent_order_id']=main_order_response['data']['oms_order_id']
            dic['parent_filled_price']=order_status['average_price']
            dic['parent_filled_qty']=qty
            dic['filled_qty']=qty
            dic['amo_date']=amo_dummy_date #square off order no need to place AMO
            
            dic['order_id']=None
            dic['order_response']=None
            dic['trigger_price']=None
            dic['limit_price']=None
            dic['sl_scheduler_name']=None
            
            dic['AccountGroup_ID']=row['AccountGroup_ID']
            dic['StrategyGroup_ID']=row['StrategyGroup_ID']
            
            ##print(dic)
            queue_db_updates(row['order_tracker_id'],ins_sql,dic)
            #cursor.execute(ins_sql,dic)
        
        if(order_status['order_status']!='rejected'):
            msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], symbol, qty,keyword_lookup(row['direction']),"Entry", order_status['average_price'], None)
            send_notification(dic_accounts[row['Account_ID']]['chat_id'],msg)
            #send_notification(dic_accounts[row['Account_ID']]['chat_id'],'Order Placed for Strategy '+str([row['name'], symbol, 'Price: '+str(order_status['average_price']), 'Direction: '+str(keyword_lookup(row['direction'])), 'Qty: '+str(qty)]))

    else:
        mainlogger.info('Open Trade Failed: Only NFO and EQ Supported')
    
    
    return 1

def close_trade(row,coredb):
    mainlogger.info('Function : close_trade  ' + str(row['order_tracker_id']))
    
    upd_sql = ("UPDATE ordertracker set  order_response=%(order_response)s, scheduler_name=%(scheduler_name)s, filled_price=%(filled_price)s, filled_qty=%(filled_qty)s , last_updated=%(last_updated)s"
           "where ID=%(ordertracker_id)s" 
          )
    upd_hit_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s, filled_price=%(filled_price)s, filled_qty=%(filled_qty)s "
           "where ID=%(ordertracker_id)s" 
          )
    
    #Auth Failed and Disabled casses
    if row['Account_ID'] is None or dic_accounts[row['Account_ID']]['token'] is None:
        mainlogger.info('Skipped Order for this account : '+ str(row['Account_ID']))
        try:
            fail_upd_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s "
           " where ID=%(ordertracker_id)s" 
          )
            dic={
                 'ordertracker_id':row['order_tracker_id'],
                 'scheduler_name': 'Error'
                }

            queue_db_updates(row['order_tracker_id'], fail_upd_sql, dic)

            queue_exception('Login token error: Re-verify credentials and Retry Failed Legs', row['order_tracker_id'])
        except Exception as e:
            mainlogger.exception(str(e))   
    
        return
    
    
    sl_order=broker.get_order_history(row['Account_ID'],row['order_id'])

    
    if(sl_order['order_status'].lower()=='trigger pending' or sl_order['order_status'].lower()=='open'):
            direction=sl_order['transaction_type'].lower()
            symbol=row['scrip']
            order_id=sl_order['oms_order_id']
            qty=int(sl_order['quantity'])
            prod_type=sl_order['product']
            exchange=sl_order['exchange']
            order_type=row['ot_order_type']
            ordertag=sl_order['order_tag']
            
            try:
                price=instruments_by_symbol[row['scrip_generic']]['ltp']
            except:
                price=0.0
                
            if '202' in row['scheduler_name']:
                while datetime.datetime.now() < datetime.datetime.strptime(row['scheduler_name'], '%Y%m%d%H%M'):
                    time.sleep(0.10)
                    
            close_response=broker.exit_order(row['Account_ID'], exchange, direction, symbol, order_id, qty, prod_type,order_type,price,ordertag)
            
            ##SL Logic
            order_status=broker.get_order_history(row['Account_ID'],row['order_id'])
            
            i=0
            while order_status['order_status'].lower() != 'rejected' and order_status['order_status'].lower() !='complete':
                time.sleep(1)
                order_status=broker.get_order_history(row['Account_ID'],row['order_id'])
                i=i+1
                if i==10:
                    mainlogger.info('Close Order Wait Time Exceeded')
                    #change from LIMIT to MARKET order after waiting time exceeded
                    if row['exchange'] in ('NFO', 'BFO') and order_type == 'LIMIT':
                        try:
                            r=broker.exit_order(row['Account_ID'], exchange, direction, symbol, order_id, qty, prod_type,'MARKET',None, ordertag)
                            order_status=broker.get_order_history(row['Account_ID'],row['order_id'])
                            validate_response(row['order_tracker_id'],r)
                            
                        except Exception as e:
                            mainlogger.exception(str(e))
                        
                        i=0
                        while order_status['order_status'].lower() != 'rejected' and order_status['order_status'].lower() !='complete':
                            order_status=broker.get_order_history(row['Account_ID'],order_id)
                            time.sleep(1)
                            i=i+1
                            if i==5:
                                #wait 2 more minutes for order to fill
                                i=0
                                while order_status['order_status'].lower() != 'rejected' and order_status['order_status'].lower() !='complete':
                                    order_status=broker.get_order_history(row['Account_ID'],order_id)
                                    time.sleep(10)
                                    i=i+1
                                    if i==6:
                                        mainlogger.info('Changed Market Order Wait Time Exceeded and marked Rejected: ' + str(order_status))
                                        order_status['order_status'] = 'rejected' #exit loop logic to avoid infinite waiting
                                        order_status['average_price']=0
                                        reject_reason='Order Tooke more than 1 minutes to Fill. Manage this leg manually. Check Your Broker Terminal.'
                                break
                        break

                    else:
                        #wait 2 more minutes for order to fill
                        i=0
                        while order_status['order_status'].lower() != 'rejected' and order_status['order_status'].lower() !='complete':
                            order_status=broker.get_order_history(row['Account_ID'],order_id)
                            time.sleep(10)
                            i=i+1
                            if i==6:
                                mainlogger.info('Changed Market Order Wait Time Exceeded and marked Rejected: ' + str(order_status))
                                order_status['order_status'] = 'rejected' #exit loop logic to avoid infinite waiting
                                order_status['average_price']=0
                                reject_reason='Order Tooke more than 1 minutes to Fill. Manage this leg manually. Check Your Broker Terminal.'
                        break
            
            validate_response(row['order_tracker_id'],order_status,True)
            
            scheduler_name='Rejected' if order_status['order_status']=='rejected' else 'Complete'
            
            dic={'order_response':close_response['status'],
             'ordertracker_id':row['order_tracker_id'],
             'scheduler_name': scheduler_name,
             'filled_price':order_status['average_price'],
             'filled_qty':qty,
             'last_updated':datetime.datetime.now()
            }
            queue_db_updates(row['order_tracker_id'],upd_sql,dic)

            mainlogger.info('Close Order Placed Response:' + str(close_response))
            validate_response(row['order_tracker_id'],close_response)
        
            if close_response['status'].lower()=='success':
                msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], symbol, qty,direction,"Exit", None, None)
                
                send_notification(dic_accounts[row['Account_ID']]['chat_id'],msg)
            
            else:  
                msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], symbol, qty,direction,"Exit", None, 'Order Close Failed')
                
                send_notification(dic_accounts[row['Account_ID']]['chat_id'],emoji_warning+"\n"+msg)
                
    
    elif(sl_order['order_status'].lower()=='complete'):
        dic={
            'ordertracker_id':row['order_tracker_id'],
             'scheduler_name': 'Complete',
             'filled_price':sl_order['average_price'],
             'filled_qty':int(sl_order['filled_quantity'])
            }
        queue_db_updates(row['order_tracker_id'],upd_hit_sql,dic)
        #cursor.execute(upd_hit_sql,dic)
        
        mainlogger.info('Close Order Not Placed as SL is HIT')
    
    elif(sl_order['order_status'].lower()=='not_found'):
        '''
        dic={
            'ordertracker_id':row['order_tracker_id'],
             'scheduler_name': 'Missed',
             'filled_price':0,
             'filled_qty':0
            }
        '''
        mainlogger.info('OrderID Not Found so trying Squareoff')
        msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], row['scrip'], row['qty'],row['ot_direction'],"Exit", None, "SL OrderID Not Found. Position will be squared off.")
        send_notification(dic_accounts[row['Account_ID']]['chat_id'],'\n'+msg)
        
        squareoff_trade(row,coredb)
        
        '''
        validate_response(row['order_tracker_id'],sl_order,True)
        queue_db_updates(row['order_tracker_id'],upd_hit_sql,dic)
        
        msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], row['scrip'], row['qty'],row['ot_direction'],"Exit", None, "OrderID Not Found so Close Failed")

        send_notification(dic_accounts[row['Account_ID']]['chat_id'],emoji_warning+'\n'+msg)
        '''
        
    else:
        mainlogger.exception('OrderID Unknown Status so trying Squareoff')
        msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], row['scrip'], row['qty'],row['ot_direction'],"Exit", None, "SL OrderID Unknown Status. Position will be squared off.")
        send_notification(dic_accounts[row['Account_ID']]['chat_id'],'\n'+msg)
        
        squareoff_trade(row,coredb)
        
    return 1


def modify_trade(row,coredb):
    mainlogger.info('Function : modify_trade ' + str(row['order_tracker_id']))
    
    
    upd_sql = ("UPDATE ordertracker set  order_response=%(order_response)s, scheduler_name=%(scheduler_name)s, order_type=%(order_type)s, trigger_price=%(trigger_price)s, limit_price=%(limit_price)s "
           "where ID=%(ordertracker_id)s" 
          )
    upd_hit_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s, order_type=%(order_type)s, filled_price=%(filled_price)s, filled_qty=%(filled_qty)s  "
           "where ID=%(ordertracker_id)s" 
          )
    
    upd_processing = ("UPDATE ordertracker set scheduler_name='Processing' where ID=%(ordertracker_id)s")
    
    #Auth Failed and Disabled casses
    if row['Account_ID'] is None or dic_accounts[row['Account_ID']]['token'] is None:  #Auth Failed and Disabled casses
        mainlogger.info('Skipped Order for this account : '+ str(row['Account_ID']))
        try:
            fail_upd_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s "
           " where ID=%(ordertracker_id)s" 
          )
            dic={
                 'ordertracker_id':row['order_tracker_id'],
                 'scheduler_name': 'Error'
                }

            queue_db_updates(row['order_tracker_id'], fail_upd_sql, dic)

            queue_exception('Login token error: Re-verify credentials and Retry Failed Legs', row['order_tracker_id'])
        except Exception as e:
            mainlogger.exception(str(e)) 
            
        return -1
    
        
    #ALGO SL cases
    if row['order_id'] is None or row['order_id']=='':
        if row['ot_direction']=='buy' or row['ot_direction']=='b':
            slip=(100+row['slippage'])/100
        else:
            slip=(100-row['slippage'])/100

        trigger_price= round(float(row['parent_filled_price']),1)
        price=round(slip*trigger_price,1)
        
        symbol=row['scrip']
        
        dic={'order_response':'Algo SL modified',
         'ordertracker_id':row['order_tracker_id'],
         'scheduler_name': row['sched_order_time'].strftime('%Y%m%d%H%M'),
         'order_type': 'Squareoff',
         'trigger_price':trigger_price,
         'limit_price':price
        }
        queue_db_updates(row['order_tracker_id'],upd_sql,dic)
        
        mainlogger.info('Modify Order Placed Response: ALGO SL')
        send_notification(dic_accounts[row['Account_ID']]['chat_id'],'Order Modified '+str([row['name'],symbol]))

    #Broker SL cases    
    else:
        sl_order=broker.get_order_history(row['Account_ID'],row['order_id'])
        
        if(sl_order['order_status'].lower()=='trigger pending' or sl_order['order_status'].lower()=='open'):
            direction=sl_order['transaction_type'].lower()
        
            if direction=='buy' or direction=='b':
                slip=(100+row['slippage'])/100
            else:
                slip=(100-row['slippage'])/100

            trigger_price= round(float(row['parent_filled_price']),1)
            price=round(slip*trigger_price,1)

            ##SQUAREOFF if price is beyond SL: IF SL BUY TP < LTP or IF SL SELL  TP > LTP 
            try:
                ins_ltp=instruments_by_symbol[row['scrip_generic']]['ltp']

                if (direction == 'buy' or  direction == 'b'):
                    if (trigger_price<=ins_ltp):

                        dic={'ordertracker_id':row['order_tracker_id']}
                        queue_db_updates(row['order_tracker_id'], upd_processing, dic)

                        row['scheduler_name']='NOW'

                        mainlogger.info('MSLC Not Placed as SL is HIT')
                        close_trade(row, coredb)
                        return 1
                else:
                    if (trigger_price>=ins_ltp):
                        dic={'ordertracker_id':row['order_tracker_id']}
                        queue_db_updates(row['order_tracker_id'], upd_processing, dic)

                        row['scheduler_name']='NOW'

                        mainlogger.info('MSLC Not Placed as SL is HIT')
                        close_trade(row, coredb)
                        return 1

            except Exception as e:
                mainlogger.exception(str(e)) 
            
            symbol=row['scrip']
            order_id=sl_order['oms_order_id']
            qty=int(sl_order['quantity'])
            prod_type=sl_order['product']
            exchange=sl_order['exchange']
            ordertag=sl_order['order_tag']
            order_type='SLL'

            #close_response=modify_order(side,symbol,ids,q)
            modify_response=broker.modify_order(row['Account_ID'], exchange, direction, symbol, order_id, qty, prod_type, order_type, price, trigger_price, ordertag)

            dic={'order_response':modify_response['status'],
             'ordertracker_id':row['order_tracker_id'],
             'scheduler_name': row['sched_order_time'].strftime('%Y%m%d%H%M'),
             'order_type': 'Close',
             'trigger_price':trigger_price,
             'limit_price':price
            }
            queue_db_updates(row['order_tracker_id'],upd_sql,dic)
            #cursor.execute(upd_sql,dic)

            mainlogger.info('Modify Order Placed Response:' + str(modify_response))
            validate_response(row['order_tracker_id'],modify_response)


            if modify_response['status'].lower()=='success':
                send_notification(dic_accounts[row['Account_ID']]['chat_id'],'Order Modified '+str([row['name'],symbol]))

            else:  
                send_notification(dic_accounts[row['Account_ID']]['chat_id'],emoji_warning+'Order Modification Failed '+str([row['name'],symbol]))
        else:
            dic={
                'ordertracker_id':row['order_tracker_id'],
                 'order_type': 'Close',
                 'scheduler_name': 'Complete',
                 'filled_price':sl_order['average_price'],
                 'filled_qty':int(sl_order['filled_quantity'])
                }
            queue_db_updates(row['order_tracker_id'], upd_hit_sql,dic)
            #cursor.execute(upd_hit_sql,dic)

            mainlogger.info('Modify Order Not Placed as SL is HIT')

    
    return 1

def squareoff_trade(row,coredb,reason=''):
    mainlogger.info('Function : squareoff_trade ' + str(row['order_tracker_id']))
    
    upd_sql = ("UPDATE ordertracker set order_id= %(order_id)s, order_response=%(order_response)s, scheduler_name=%(scheduler_name)s,scrip=%(scrip)s,direction=%(direction)s, trigger_price=%(trigger_price)s, limit_price=%(limit_price)s, filled_price=%(filled_price)s, filled_qty=%(filled_qty)s , last_updated=%(last_updated)s"
           " where ID=%(ordertracker_id)s" 
          )
    
    upd_sql_sl_hit = ("UPDATE ordertracker set order_id= %(order_id)s, order_response=%(order_response)s, scheduler_name=%(scheduler_name)s,scrip=%(scrip)s,direction=%(direction)s, trigger_price=%(trigger_price)s, limit_price=%(limit_price)s, filled_price=%(filled_price)s, filled_qty=%(filled_qty)s, comments=CONCAT_WS('|',comments,'sl is hit') "
           " where ID=%(ordertracker_id)s" 
          )
  
    #Auth Failed and Disabled casses
    if row['Account_ID'] is None  or dic_accounts[row['Account_ID']]['token'] is None:
        mainlogger.info('Skipped Order for this account : '+ str(row['Account_ID']))
        try:
            fail_upd_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s "
           " where ID=%(ordertracker_id)s" 
          )
            dic={
                 'ordertracker_id':row['order_tracker_id'],
                 'scheduler_name': 'Error'
                }

            queue_db_updates(row['order_tracker_id'], fail_upd_sql, dic)

            queue_exception('Login token error: Re-verify credentials and Retry Failed Legs', row['order_tracker_id'])
        except Exception as e:
            mainlogger.exception(str(e)) 
        return
    
    symbol=row['scrip']
    qty=int(row['parent_filled_qty'])
    direction=row['ot_direction']
    triggerPrice=0.0
    price=0.0
    misFlag= 'NRML' if row['intraday']==0 else 'MIS'
    strategyName=row['order_group_id']
    amoFlag=False
    order_type=row['ot_order_type']
      
    ##use ltp for limit orders
    try:
        price=instruments_by_symbol[row['scrip_generic']]['ltp']
    except:
        price=0.0
    
    ##For Algo SL squareoff use LIMIT price
    if row['scheduler_name']=='NOWSE' and row['comments'] and 'sl is hit' in row['comments']:
        price=row['ot_limit_price']
        order_type='LIMIT'
    
    if '202' in row['scheduler_name']:
        while datetime.datetime.now() < datetime.datetime.strptime(row['scheduler_name'], '%Y%m%d%H%M'):
            time.sleep(0.10)
                    
    main_order_response=broker.punch_order(row['Account_ID'], symbol,direction, qty, triggerPrice, price, 
                order_type, misFlag, amoFlag, row['exchange'], strategyName)

    mainlogger.info('Squareoff Order Response:' + str(main_order_response))
    validate_response(row['order_tracker_id'],main_order_response)

    ##SL Logic
    if main_order_response['data']['oms_order_id']==zerodha_failed_order_hardcoded_id:
        order_status={'order_status':'rejected', 'average_price':0}
    else:
        order_status=broker.get_order_history(row['Account_ID'],main_order_response['data']['oms_order_id'])
        time.sleep(1)
    i=0
    while order_status['order_status'].lower() != 'rejected' and order_status['order_status'].lower() !='complete':
        order_status=broker.get_order_history(row['Account_ID'],main_order_response['data']['oms_order_id'])
        time.sleep(1)
        i=i+1
        if i==10:
            mainlogger.info('Squareoff Order Wait Time Exceeded')
                
            #change from LIMIT to MARKET order after waiting time exceeded 10 seconds
            if row['exchange'] in ('NFO', 'BFO') and order_type == 'LIMIT':
                try:
                    r=broker.exit_order(row['Account_ID'], row['exchange'], direction, symbol, main_order_response['data']['oms_order_id'], qty, order_status['product'],'MARKET',None,order_status['order_tag'])
                    order_status=broker.get_order_history(row['Account_ID'],main_order_response['data']['oms_order_id'])
                    validate_response(row['order_tracker_id'],r)
                
                except Exception as e:
                    mainlogger.exception(str(e))
                
                i=0
                while order_status['order_status'].lower() != 'rejected' and order_status['order_status'].lower() !='complete':
                    order_status=broker.get_order_history(row['Account_ID'],main_order_response['data']['oms_order_id'])
                    time.sleep(1)
                    i=i+1
                    if i==5:
                        
                        #Wait for 2 more minutes for order to fill
                        i=0
                        while order_status['order_status'].lower() != 'rejected' and order_status['order_status'].lower() !='complete':
                            order_status=broker.get_order_history(row['Account_ID'],main_order_response['data']['oms_order_id'])
                            time.sleep(10)
                            i=i+1
                            if i==6:
                                mainlogger.info('Changed Market Order Wait Time Exceeded and marked Rejected: ' + str(order_status))
                                order_status['order_status'] = 'rejected' #exit loop logic to avoid infinite waiting
                                order_status['average_price']=0
                                reject_reason='Order Tooke more than 1 minutes to Fill. Manage this leg manually. Check Your Broker Terminal.'
                        break
                break

            else:
                #wait 2 more minutes for order to fill
                i=0
                while order_status['order_status'].lower() != 'rejected' and order_status['order_status'].lower() !='complete':
                    order_status=broker.get_order_history(row['Account_ID'],main_order_response['data']['oms_order_id'])
                    time.sleep(10)
                    i=i+1
                    if i==6:
                        mainlogger.info('Changed Market Order Wait Time Exceeded and marked Rejected: ' + str(order_status))
                        order_status['order_status'] = 'rejected' #exit loop logic to avoid infinite waiting
                        order_status['average_price']=0
                        reject_reason='Order Tooke more than 1 minute to Fill. Manage this leg manually. Check Your Broker Terminal.'
                break
    
    if main_order_response['data']['oms_order_id']!=zerodha_failed_order_hardcoded_id:
        validate_response(row['order_tracker_id'],order_status,True)
    
    scheduler_name='Rejected' if order_status['order_status']=='rejected' else 'Complete'
    
    ## Update Order Status in DB
    dic={'order_id':main_order_response['data']['oms_order_id'],
             'order_response':order_status['order_status'],
             'ordertracker_id':row['order_tracker_id'],
             'filled_price':order_status['average_price'],
             'scheduler_name': scheduler_name,
             'scrip':symbol,
             'direction':direction,
             'trigger_price':row['ot_trigger_price'],
             'limit_price':row['ot_limit_price'],
             'filled_qty':qty,
             'last_updated':datetime.datetime.now()
        }
    if reason=='':
        queue_db_updates(row['order_tracker_id'],upd_sql,dic)
    else:
        queue_db_updates(row['order_tracker_id'],upd_sql_sl_hit,dic)
    
    if(order_status['order_status'].lower()=='rejected'):
        msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], symbol, qty,direction,"Exit", None, "Order Close Rejected by Broker")

        send_notification(dic_accounts[row['Account_ID']]['chat_id'],emoji_warning+'\n'+msg)

    else:
        msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], symbol, qty,direction,"Exit", order_status['average_price'], None)
        send_notification(dic_accounts[row['Account_ID']]['chat_id'],msg)
    return 1


def update_trade(row,coredb):
    mainlogger.info('Function:update_trade '+str(row['order_tracker_id']))
    
    upd_hit_sql = ("UPDATE ordertracker set order_type='Close', scheduler_name='Complete', filled_price=%(filled_price)s, filled_qty=%(filled_qty)s, comments=CONCAT_WS('|',comments,'sl is hit') "
           "where ID=%(ordertracker_id)s" 
          )
    
    upd_auth_issue_sql = ("UPDATE ordertracker set order_type='Close', scheduler_name = 'Complete', comments=CONCAT_WS('|',comments,'update_trade auth token/api issue') "
           " where ID=%(ordertracker_id)s" 
          )
    
    upd_reset_sql = ("UPDATE ordertracker set order_type='Close', scheduler_name = case when sched_order_time is not null then DATE_FORMAT(sched_order_time, '%%Y%%m%%d%%H%%i') else 'Manual' end, filled_price=NULL, filled_qty=NULL, comments=CONCAT_WS('|',right(comments,50),'SL update reset_sl') "
           " where ID=%(ordertracker_id)s" 
          )
    
    #auth failed and disabled cases
    if row['Account_ID'] is None or dic_accounts[row['Account_ID']]['token'] is None:
        mainlogger.info('Skipped Order for this account : '+ str(row['Account_ID']))
        try:
            fail_upd_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s "
           " where ID=%(ordertracker_id)s" 
          )
            dic={
                 'ordertracker_id':row['order_tracker_id'],
                 'scheduler_name': 'Error'
                }

            queue_db_updates(row['order_tracker_id'], fail_upd_sql, dic)

            queue_exception('Login token error: Re-verify credentials and Retry Failed Legs', row['order_tracker_id'])
        except Exception as e:
            mainlogger.exception(str(e)) 
        return
    
    
    sl_order=broker.get_order_history(row['Account_ID'],row['order_id'])

    
    if(sl_order['order_status'].lower()=='trigger pending'):
        dic={
        'ordertracker_id':row['order_tracker_id']
        }
        queue_db_updates(row['order_tracker_id'],upd_reset_sql,dic)
    
    elif(sl_order['order_status'].lower()=='open'):
        mainlogger.info('order status update: SL is open, broker unable to exit, so trying closing')
        time.sleep(2)
        close_trade(row,coredb) #,'sl is hit')  
        
    elif sl_order['order_status'].lower()=='complete':
        
        dic={'order_response':sl_order['order_status'],
             'ordertracker_id':row['order_tracker_id'],
             'scheduler_name': 'Complete',
             'filled_price':sl_order['average_price'],
             'filled_qty':int(row['parent_filled_qty'])
            }
        queue_db_updates(row['order_tracker_id'],upd_hit_sql,dic)
        #cursor.execute(upd_hit_sql,dic)
        
    elif sl_order['order_status'].lower() in ('rejected','cancelled'):
        mainlogger.info('order status update: SL was rejected so trying to Squareoff')
        squareoff_trade(row,coredb,'sl is hit')  
         
    else:
        dic={
            'ordertracker_id':row['order_tracker_id']
            }
        queue_db_updates(row['order_tracker_id'],upd_auth_issue_sql,dic)
        
        mainlogger.info('order status update failed')

    return 1

def check_mslc(row,coredb):
    mainlogger.info('Function: check_mslc for ' + str(row['order_tracker_id'])) 
    #check if SL is hit and MSLC needed 
    
    #check disabled
    return 1 
    
def reset_mslc(row,coredb):
    #In the event SL is not hit as per broker, MSLC will be reset by the algo
    #All actions pertaining to MSLC will be reversed
    mainlogger.info('Function: reset_mslc for ' + str(row['order_tracker_id'])) 
    
    try:
        
        #restore SL back as not HIT
        upd_sql1 = ("UPDATE ordertracker set order_type='Close', scheduler_name = case when sched_order_time is not null then DATE_FORMAT(sched_order_time, '%%Y%%m%%d%%H%%i') else 'Manual' end, comments=CONCAT_WS('|',comments,'SL reset_mslc') "
           " where ID=%(ordertracker_id)s" 
          )
        
        upd_sql2 = ("UPDATE ordertracker set scheduler_name = case when sched_order_time is not null then DATE_FORMAT(sched_order_time, '%%Y%%m%%d%%H%%i') else 'Manual' end, comments=CONCAT_WS('|',comments,'SL reset_mslc') "
           " where Account_ID=%(Account_ID)s and order_group_id=%(order_group_id)s and order_type='Close' and scheduler_name='Complete'" 
          )
        
        dic1={'ordertracker_id':row['order_tracker_id']}
        dic2={'Account_ID':row['Account_ID'],'order_group_id':row['order_group_id']}
        
        queue_db_updates(row['order_tracker_id'],upd_sql1,dic1)
        queue_db_updates(row['order_tracker_id'],upd_sql2,dic2)

        
    except Exception as e:
        mainlogger.exception('MSLC reset failed '+str(e))
        

def reset_nowre(row,coredb):
    #In the event SL is not hit as per broker, RE will be reset by the algo
    #All actions pertaining to RE will be reversed
    mainlogger.info('Function: reset_nowre') 
    
    try:
        
        #restore SL back as not HIT
        upd_sql1 = ("UPDATE ordertracker set scheduler_name = case when sched_order_time is not null then DATE_FORMAT(sched_order_time, '%%Y%%m%%d%%H%%i') else 'Manual' end, comments=CONCAT_WS('|',comments,'SL reset_nowre') "
           " where Account_ID=%(Account_ID)s and order_id=%(order_id)s and order_type='Close'" 
          )
        
        #delete RE entry 
        upd_sql2 = ("DELETE from ordertracker"
           " where ID=%(ordertracker_id)s" 
          )
        
        dic1={'Account_ID':row['Account_ID'],'order_id':row['order_id']}
        dic2={'ordertracker_id':row['order_tracker_id']}
        
        queue_db_updates(row['order_tracker_id'],upd_sql1,dic1)
        queue_db_updates(row['order_tracker_id'],upd_sql2,dic2)
        
        
    except Exception as e:
        mainlogger.exception(str(e))
    

def open_sl_trade(row,coredb): 
    mainlogger.info('Function : open_sl_trade '+str([row['Account_ID'],row['order_tracker_id']]))

    cursor=coredb.cursor()
     
    upd_nothit_sql = ("UPDATE ordertracker set scheduler_name= %(scheduler_name)s, sl_scheduler_name= %(sl_scheduler_name)s, order_id= %(order_id)s, order_response=%(order_response)s "
           "where ID=%(ordertracker_id)s" 
          )
    
    upd_sl_reject_sql = ("UPDATE ordertracker set scheduler_name= %(scheduler_name)s, sl_scheduler_name= %(sl_scheduler_name)s, order_id= %(order_id)s, order_type= %(order_type)s"
           "where ID=%(ordertracker_id)s" 
          )
    
    upd_processing = ("UPDATE ordertracker set scheduler_name='Processing' where ID=%(ordertracker_id)s")
    
    #Auth Failed and Disabled casses
    if row['Account_ID'] is None  or dic_accounts[row['Account_ID']]['token'] is None:
        mainlogger.info('Skipped Order for this account : '+ str(row['Account_ID']))
        try:
            fail_upd_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s "
           " where ID=%(ordertracker_id)s" 
          )
            dic={
                 'ordertracker_id':row['order_tracker_id'],
                 'scheduler_name': 'Error'
                }

            queue_db_updates(row['order_tracker_id'], fail_upd_sql, dic)

            queue_exception('Login token error: Re-verify credentials and Retry Failed Legs', row['order_tracker_id'])
        except Exception as e:
            mainlogger.exception(str(e)) 
        return
    
    #sl_order=custom_order_history(slid,alice_blue)
    try:
        sl_order=json.loads(row['sl_order_response_json'])

        triggerPrice=sl_order['trigger_price']
        price=sl_order['price_to_fill']
        qty=sl_order['quantity']
        direction=sl_order['transaction_type'].lower()
        
    except:
        triggerPrice=row['ot_trigger_price']
        price=row['ot_limit_price']
        qty=row['parent_filled_qty']
        direction=row['ot_direction'].lower()
    
    symbol=row['scrip']
    #strategyName=sl_order['order_tag']
    strategyName=row['order_group_id']
    amoFlag=False
    misFlag='NRML'
    
    '''
    #circuit limit check
    circuit_limits=broker.get_circuit_limits(row['Account_ID'], row['exchange'], row['scrip_generic'])

    if price >= circuit_limits['higher_circuit_limit']:
        triggerPrice=circuit_limits['higher_circuit_limit']*0.98
        price=circuit_limits['higher_circuit_limit']*0.98

    elif price <= circuit_limits['lower_circuit_limit']:
        triggerPrice=circuit_limits['lower_circuit_limit']*1.02
        price=circuit_limits['lower_circuit_limit']*1.02
    '''
    
    sl_type=keyword_lookup(row['stop_loss_type'])
    
    ##SQUAREOFF if price is beyond SL: IF SL BUY TP < LTP or IF SL SELL  TP > LTP 
    try:
        ins_ltp=instruments_by_symbol[row['scrip_generic']]['ltp']
        if (direction == 'buy' or  direction == 'b'):
            if (triggerPrice<ins_ltp):
            
                dic={'ordertracker_id':row['order_tracker_id']}
                queue_db_updates(row['order_tracker_id'], upd_processing, dic)
                
                row['scheduler_name']='NOW'
                
                squareoff_trade(row, coredb, 'sl is hit')
                cursor.close()
                return 1
        else:
            if (triggerPrice>ins_ltp):
                dic={'ordertracker_id':row['order_tracker_id']}
                queue_db_updates(row['order_tracker_id'], upd_processing, dic)
                
                row['scheduler_name']='NOW'
                
                squareoff_trade(row, coredb, 'sl is hit')
                cursor.close()
                return 1
        
    except Exception as e:
        mainlogger.exception(str(e))
        dic={'order_response':'Failed to place SL order scheduled',
         'ordertracker_id':row['order_tracker_id'],
         'scheduler_name':'Failed',
         'order_id':'Failed',
         'sl_scheduler_name':'Failed'
        }
        queue_db_updates(row['order_tracker_id'],upd_nothit_sql,dic)
        #cursor.execute(upd_nothit_sql,dic)
        msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], symbol, qty,direction, "Scheduled SL", None, 'Scheduled SL Exit Rejected by Broker')
        send_notification(dic_accounts[row['Account_ID']]['chat_id'], emoji_warning+'\n'+msg)
        return 1
        
    
    if sl_type.startswith('SLM'):
        sl_order_response=broker.punch_order(row['Account_ID'], symbol, direction, qty, triggerPrice, price, 
                'SLM', misFlag, amoFlag, row['exchange'], strategyName)
    else:
        sl_order_response=broker.punch_order(row['Account_ID'], symbol, direction, qty, triggerPrice, price, 
                'SLL', misFlag, amoFlag, row['exchange'], strategyName)
    
    mainlogger.info('Scheduled SL Order Placed Response:' + str(sl_order_response))
    validate_response(row['order_tracker_id'],sl_order_response)
    
    sl_order_status=broker.get_order_history(row['Account_ID'],sl_order_response['data']['oms_order_id'])
    
    dic={'order_response':sl_order_status['order_status'],
         'ordertracker_id':row['order_tracker_id'],
         'scheduler_name':row['sched_order_time'].strftime('%Y%m%d%H%M'), #row['scheduler_name'],
         'order_id':sl_order_response['data']['oms_order_id'],
         'sl_scheduler_name':'Complete'
        }
    
    if(sl_order_status['order_status'].lower()=='rejected'):
        send_notification(dic_accounts[row['Account_ID']]['chat_id'], 'SL Rejected so converted to Algo SL. No manual intervention needed.'+str([row['name'],symbol]))
        
        #dic['scheduler_name']='Rejected'
        dic['sl_scheduler_name']='Rejected'
        dic['order_id']=None
        dic['order_type']='Squareoff'
        queue_db_updates(row['order_tracker_id'],upd_sl_reject_sql,dic)
        #cursor.execute(upd_nothit_sql,dic)
    else:
        queue_db_updates(row['order_tracker_id'],upd_nothit_sql,dic)
        #cursor.execute(upd_nothit_sql,dic)
        
    coredb.commit()
    cursor.close()
    
    return 1


def open_amo_trade(row,coredb):
    
    mainlogger.info('Function : open_amo_trade '+str([row['Account_ID'],row['order_tracker_id']]))
    
    cursor=coredb.cursor()
    
    upd_nothit_sql = ("UPDATE ordertracker set last_updated=last_updated, order_id= %(order_id)s, order_response=%(order_response)s, amo_date=%(amo_date)s "
           "where ID=%(ordertracker_id)s" 
          )
    
    upd_nothit_scheduled_sql = ("UPDATE ordertracker set last_updated=last_updated, sl_order_response_json=%(sl_order_response_json)s, amo_date=%(amo_date)s, sl_scheduler_name=%(sl_scheduler_name)s "
           "where ID=%(ordertracker_id)s" 
          )
    
    upd_hit_sql = ("UPDATE ordertracker set last_updated=last_updated, scheduler_name=%(scheduler_name)s "
           "where ID=%(ordertracker_id)s" 
          )
    
    upd_sl_reject_sql = ("UPDATE ordertracker set order_id= %(order_id)s, order_type= %(order_type)s, amo_date= %(amo_date)s"
           "where ID=%(ordertracker_id)s" 
          )
    
    slid=row['order_id']
    
    #Auth Failed and Disabled casses
    if row['Account_ID'] is None  or dic_accounts[row['Account_ID']]['token'] is None:
        mainlogger.info('Skipped Order for this account : '+ str(row['Account_ID']))
        try:
            fail_upd_sql = ("UPDATE ordertracker set scheduler_name=%(scheduler_name)s "
           " where ID=%(ordertracker_id)s" 
          )
            dic={
                 'ordertracker_id':row['order_tracker_id'],
                 'scheduler_name': 'Error'
                }

            queue_db_updates(row['order_tracker_id'], fail_upd_sql, dic)

            queue_exception('Login token error: Re-verify credentials and Retry Failed Legs', row['order_tracker_id'])
        except Exception as e:
            mainlogger.exception(str(e)) 
        return
    
    #sl_order=custom_order_history(slid,alice_blue)
    sl_order=broker.get_order_history(row['Account_ID'],slid)
    
    
    if(sl_order['order_status'].lower()=='cancelled' and row['schedule_sl_time'] is None):
            
        triggerPrice=sl_order['trigger_price']
        price=sl_order['price_to_fill']
        symbol=row['scrip']
        qty=sl_order['quantity']
        #strategyName=sl_order['order_tag']
        strategyName=row['order_group_id']
        amoFlag=True
        direction=sl_order['transaction_type'].lower()
        misFlag='NRML'
        
        #circuit limit check
        '''
        circuit_limits=broker.get_circuit_limits(row['Account_ID'], row['exchange'], row['scrip_generic'])
    
        if price > circuit_limits['higher_circuit_limit']:
            triggerPrice=circuit_limits['higher_circuit_limit']
            price=circuit_limits['higher_circuit_limit']
                
        elif price < circuit_limits['lower_circuit_limit']:
            triggerPrice=circuit_limits['lower_circuit_limit']
            price=circuit_limits['lower_circuit_limit']
        '''
        sl_type=keyword_lookup(row['stop_loss_type'])
        
        if triggerPrice<=0 or price<=0:
            triggerPrice=1
            price=1
        
        if sl_type.startswith('SLM'):
            sl_order_response=broker.punch_order(row['Account_ID'], symbol, direction, qty, triggerPrice, price, 
                    'SLM', misFlag, amoFlag, row['exchange'], strategyName)
        else:
            sl_order_response=broker.punch_order(row['Account_ID'], symbol, direction, qty, triggerPrice, price, 
                    'SLL', misFlag, amoFlag, row['exchange'], strategyName)
        
 
        if(sl_order_response['status'].lower()!='rejected'):
            
            dic={'order_response':sl_order_response['status'],
                 'ordertracker_id':row['order_tracker_id'],
                 'order_id':sl_order_response['data']['oms_order_id'],
                 'amo_date': datetime.datetime.now().date()
                }
            
            cursor.execute(upd_nothit_sql,dic)
            
        else:
            dic={}
            dic['ordertracker_id']=row['order_tracker_id']
            dic['order_id']=None
            dic['order_type']='Squareoff'
            dic['amo_date']=amo_dummy_date

            cursor.execute(upd_sl_reject_sql,dic)
            
            msg=construct_order_message(row['name'], dic_accounts[row['Account_ID']]['username'], symbol, qty,direction,"AMO SL", None, 'AMO SL Rejected so converted to Algo SL. No manual intervention needed.')
            send_notification(dic_accounts[row['Account_ID']]['chat_id'], emoji_warning+'\n'+msg)
             
        mainlogger.info('AMO SL Order Placed Response:' + str(sl_order_response))
        validate_response(row['order_tracker_id'],sl_order_response)
        
    elif(sl_order['order_status'].lower()=='cancelled' and row['schedule_sl_time']!=None):
        
        dt=datetime.datetime.now(ist)
        delta=global_delta_next_trading_day
        
        
        sl_scheduler_date=(dt+ datetime.timedelta(days=delta)).date()
        sl_scheduler_time=(dt+ datetime.timedelta(days=delta)).strftime("%m-%d-%y")+' '+row['schedule_sl_time']
        
        sl_scheduler_time=datetime.datetime.strptime(sl_scheduler_time, '%m-%d-%y %H:%M')
        sl_scheduler_name=sl_scheduler_time.strftime('%Y%m%d%H%M')
                
            
        dic={'sl_order_response_json':json.dumps(sl_order, indent=4, sort_keys=True, default=str),
             'ordertracker_id':row['order_tracker_id'],
             'amo_date': sl_scheduler_date,
             'sl_scheduler_name':sl_scheduler_name
            }
        
        cursor.execute(upd_nothit_scheduled_sql,dic)
        
        mainlogger.info('SL Scheduled for next trading day')
        
    else:
        dic={
            'ordertracker_id':row['order_tracker_id'],
             'scheduler_name': 'Complete'
            }
        cursor.execute(upd_hit_sql,dic)
        
        mainlogger.info('AMO SL Order Not Placed as SL is HIT')
        
    coredb.commit()
    cursor.close()
    
    return 1


def place_amo_orders():
    mainlogger.info('Function : place_amo_orders ') 
    #ot.amo_date='2000-01-01' is a dummy date to ignore orders
    try:
        coredb = pool.connection()
        sql=("select va.*,ot.id as order_tracker_id,ot.order_group_id,ot.order_type, ot.order_id, ot.scrip, ot.scrip_generic ,i.symbol as base_symbol "
             "from ordertracker ot left outer join vw_activestrategies_all va on (ot.OrderLegs_ID=va.ID and ot.Account_ID=va.Account_ID) left outer join instrument i on (va.base_instrument_id=i.ID) "
             "where (cast(sched_order_time as date) > cast(current_timestamp() as date)) and ot.scheduler_name like '202%' and ot.active_flag=1 and "
             "((ot.sl_scheduler_name is null and  ot.order_type='Close' and (ot.amo_date is null or ot.amo_date <> current_date() or ot.amo_date <>'"+amo_dummy_date+"')) OR (ot.sl_scheduler_name is not null and (ot.amo_date is null or ot.amo_date = current_date())))"
            )
    

        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        try:
            broker.check_login_token(primary_kite)
        except:
            pass
        
        for index,row in df.iterrows():
            try:
                
                broker.check_login_token(row['Account_ID'])
                open_amo_trade(row,coredb)    
            except Exception as e:
                try:
                    handle_exception('Failed to place AMO order account id: '+str(row['Account_ID'])+ ' '+row['name']+' Symbol '+row['scrip']+' OrderID '+row['order_id'],str(e),dic_accounts[row['Account_ID']]['chat_id'],True)
                except Exception as e:
                    mainlogger.exception(str(e))
        coredb.close()
    except Exception as e:
        mainlogger.exception(str(e)) 
        coredb.close()
        


# # Function - Schedule Open Time

# %%

# %%


def exclusion_check(exclude_days_opt):
    mainlogger.info('Function : exclusion_check')
    
    exclude_days_opt=keyword_lookup(exclude_days_opt)
    
    dt=datetime.datetime.now(ist)
    
    if(dt.strftime("%A")==exclude_days_opt):
        return False
    
    if(exclude_days_opt=='NEXT_DAY_HOLIDAY' and (dt + datetime.timedelta(days=1)).strftime("%m-%d-%y") in nse_holdiays): 
        return False
    
    return True


def return_sched_open_time(row,dt):
    mainlogger.info('Function : return_sched_open_time')
    
    if row['open_time']!='Advanced':
        sched_open_time=ist.localize(datetime.datetime(dt.year, dt.month, dt.day, int(row['open_time'].split(":")[0]), int(row['open_time'].split(":")[1]),0,0))
    else:
        sched_open_time=ist.localize(datetime.datetime(dt.year, dt.month, dt.day,15,30,0,0))
    
    return sched_open_time;

def calculate_open_time(row):
    mainlogger.info('Function : calculate_open_time')
    
    dt=datetime.datetime.now(ist)
    trade_days=keyword_lookup(row['open_days'])
    
    #HANDLE FINNIFTY EXPIRY
    if row['symbol']=='BANKNIFTY':
        exp_weekly=exp_weekly_bnf
        exp_monthly=exp_monthly_bnf
        exp_monthly_plus=exp_monthly_plus_bnf
        exp_future_weekly=exp_future_weekly_bnf
        exp_future_monthly=exp_future_monthly_bnf
        EXP_DAY=EXP_DAY_BNF
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_BNF
        
    elif row['symbol']=='FINNIFTY':
        exp_weekly=exp_weekly_fin
        exp_monthly=exp_monthly_fin
        exp_monthly_plus=exp_monthly_plus_fin
        exp_future_weekly=exp_future_weekly_fin
        exp_future_monthly=exp_future_monthly_fin
        EXP_DAY=EXP_DAY_FIN
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_FIN

    elif row['symbol']=='MIDCPNIFTY':
        exp_weekly=exp_weekly_midcp
        exp_monthly=exp_monthly_midcp
        exp_monthly_plus=exp_monthly_plus_midcp
        exp_future_weekly=exp_future_weekly_midcp
        exp_future_monthly=exp_future_monthly_midcp
        EXP_DAY=EXP_DAY_MIDCP
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_MIDCP
    
    elif row['symbol']=='SENSEX':
        exp_weekly=exp_weekly_sensex
        exp_monthly=exp_monthly_sensex
        exp_monthly_plus=exp_monthly_plus_sensex
        exp_future_weekly=exp_future_weekly_sensex
        exp_future_monthly=exp_future_monthly_sensex
        EXP_DAY=EXP_DAY_SENSEX
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_SENSEX
    
    else:
        exp_weekly=exp_weekly_def
        exp_monthly=exp_monthly_def
        exp_monthly_plus=exp_monthly_plus_def
        exp_future_weekly=exp_future_weekly_def
        exp_future_monthly=exp_future_monthly_def
        EXP_DAY=EXP_DAY_DEF
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_DEF
        
    
    if(dt.strftime("%A") in trade_days 
       and dt.strftime("%m-%d-%y") not in nse_holdiays 
       and exclusion_check(row['exclude_days'])):
        
        sched_open_time=return_sched_open_time(row,dt)
        return "TradeOpen",sched_open_time
    
    elif('CONTRACT_EXP_BEFORE' in trade_days
        and row['prepone_on_holiday']==0
        and datetime.datetime.now(ist).date()==((datetime.datetime.strptime(exp_weekly, '%d %b%y').date())-datetime.timedelta(days=row['open_day_diff']))
        and dt.strftime("%m-%d-%y") not in nse_holdiays 
        and exclusion_check(row['exclude_days'])):
        
        sched_open_time=return_sched_open_time(row,dt)
        return "TradeOpen",sched_open_time
    
    elif('CONTRACT_EXP_BEFORE' in trade_days
        and row['prepone_on_holiday']==1
        and datetime.datetime.now(ist).date()==get_day_before_trading_day_delta((datetime.datetime.strptime(exp_weekly, '%d %b%y').date())-datetime.timedelta(days=row['open_day_diff']))
        and dt.strftime("%m-%d-%y") not in nse_holdiays 
        and exclusion_check(row['exclude_days'])):
        
        sched_open_time=return_sched_open_time(row,dt)
        return "TradeOpen",sched_open_time
    
    elif('CONTRACT_EXP_DAY' in trade_days 
       and dt.strftime("%m-%d-%y") not in nse_holdiays 
       and exclusion_check(row['exclude_days'])
       and EXP_DAY in trade_days 
        ):
        
        sched_open_time=return_sched_open_time(row,dt)
        return "TradeOpen",sched_open_time
     
    return None
    
def calculate_scheduled_time():
    mainlogger.info('Function : calculate_scheduled_time')
    
    try:
        coredb = pool.connection()
        sql=("select * from vw_eligibleorders_all order by account_id, strategy_id")
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        cursor=coredb.cursor()
        sql=("INSERT INTO ordertracker (Account_ID,AccountGroup_ID,StrategyGroup_ID,Strategy_ID,OrderLegs_ID,order_group_id,order_type, sched_order_time, scheduler_name, reentry_max,tp_reentry_max)"
               " VALUES (%(Account_ID)s, %(AccountGroup_ID)s, %(StrategyGroup_ID)s,%(Strategy_ID)s, %(ID)s, %(order_group_id)s, %(order_type)s, %(sched_order_time)s, %(scheduler_name)s, %(reentry_max)s,%(tp_reentry_max)s)")

        sql_ordergroup_status=("INSERT INTO ordergroupstatus(Account_ID,AccountGroup_ID,StrategyGroup_ID, Strategy_ID, strategyName, order_group_id, status,show_check_box)"
                                 " VALUES (%(Account_ID)s,%(AccountGroup_ID)s, %(StrategyGroup_ID)s, %(Strategy_ID)s, %(name)s, %(order_group_id)s,'Scheduled',4)"
                                 " ON DUPLICATE KEY update order_group_id=(%(order_group_id)s)")
        
        prevStrategy_ID=''
        
        dt=datetime.datetime.now(ist)

        for index, row in df.iterrows():
            try:
                #print(str(row['Strategy_ID']), str(row['Account_ID']))
                if prevStrategy_ID!=str(row['Strategy_ID'])+str(row['Account_ID']):
                    gid=int(str(row['Strategy_ID'])+str(random.randrange(10000000,100000000)))
                    prevStrategy_ID=str(row['Strategy_ID'])+str(row['Account_ID'])
                    #print(prevStrategy_ID)
                    
                dic=row.to_dict()
                dic['order_type']='Open'
        
                tradeStatus=calculate_open_time(row)   
                

                if tradeStatus is None:
                    dic['order_group_id']=gid
                    dic['scheduler_name']='Not Today'
                    dic['sched_order_time']=None

                    cursor.execute(sql, dic)
                    #cursor.execute(sql_ordergroup_status, dic)

                elif tradeStatus[0]=='TradeOpen' and row['open_time']=='Advanced':
                    sched_open_time=tradeStatus[1]

                    dic['order_group_id']=gid
                    dic['sched_order_time']=sched_open_time
                    dic['scheduler_name']='Advanced'
                    
                    cursor.execute(sql, dic)
                    cursor.execute(sql_ordergroup_status, dic)
                    
                elif tradeStatus[0]=='TradeOpen' and tradeStatus[1]>dt:
                    sched_open_time=tradeStatus[1]

                    dic['order_group_id']=gid
                    dic['sched_order_time']=sched_open_time
                    dic['scheduler_name']=sched_open_time.strftime('%Y%m%d%H%M')

                    cursor.execute(sql, dic)
                    cursor.execute(sql_ordergroup_status, dic)

                elif tradeStatus[0]=='TradeOpen' and tradeStatus[1]<dt:
                    sched_open_time=tradeStatus[1]

                    dic['order_group_id']=gid
                    dic['sched_order_time']=sched_open_time
                    dic['scheduler_name']='Missed'
                    
                    mainlogger.exception(str([tradeStatus[0],tradeStatus[1],dt]))
                    if is_market_open():
                        handle_exception('Strategy Schedule Missed for Account ID '+str(row['Account_ID'])+ ' '+row['name'],'NA',row['Account_ID'], True, True)
                    cursor.execute(sql, dic)
                    cursor.execute(sql_ordergroup_status, dic)
                    
                    
            except Exception as e:
                try:
                    dic['order_group_id']=gid
                    dic['sched_order_time']=None
                    dic['scheduler_name']='Failed'

                    cursor.execute(sql, dic)
                    cursor.execute(sql_ordergroup_status, dic)
                    handle_exception('Strategy Schedule Failed for Account ID '+str(row['Account_ID'])+ ' '+row['name'],str(e), row['Account_ID'], True, True)
                
                except Exception as e:
                    mainlogger.exception(str(e))

        cursor.execute("commit")  
        coredb.close()
    
    except Exception as e:
        mainlogger.exception(str(e)) 
        coredb.close()
        

#calculate_scheduled_time()
# # Queue Exception Messages to DB

# %%


def queue_exception(var, ordertracker_ID):
    mainlogger.info('Function: queue_exception')
    try:
        
        sql_stmt=("insert into ordertracker_errors(ordertracker_ID,error_message) values(%(ordertracker_ID)s,%(error_message)s)")
        dic={'ordertracker_ID':ordertracker_ID,'error_message':str(var).replace('%','%%')[:1000]}
        
        send_to_db_queue({'ordertracker_ID':ordertracker_ID, 'sql_stmt':sql_stmt,'dic':dic})
    except Exception as e:
        mainlogger.exception(e)
    


# %%


def queue_db_updates(ordertracker_ID, sql_stmt, dic):
    mainlogger.info('Function: queue_db_updates')
    try:
        #mainlogger.info(str({'ordertracker_ID':ordertracker_ID, 'sql_stmt':sql_stmt,'dic':dic}))
        
        send_to_db_queue({'ordertracker_ID':ordertracker_ID, 'sql_stmt':sql_stmt,'dic':dic})
    except Exception as e:
        mainlogger.exception('Failed to queue a message for DB update: '+str(e))


# # Function - Attach Scheduler and Run Jobs

# %%


def run_scheduled_one_job(row, tmp_instruments_by_symbol, schedule, base_symbol_ltp=None):
    mainlogger.info('function: run_scheduled_one_job orderid:' + str(row['order_tracker_id']) + ' scheduler:' +str(row['scheduler_name']))
    
    ##print('run_scheduled_one_job entered at:',datetime.datetime.now())
    
    #while loop to wait until scheduled time before firing orders
    if '202' in schedule:
        exit_time = datetime.datetime.strptime(schedule, '%Y%m%d%H%M') - datetime.timedelta(seconds=5)
        while datetime.datetime.now() < exit_time:
            time.sleep(0.25)
                
    upd_sql=("update ordertracker ot set scheduler_name='Failed' where ot.ID=%(order_tracker_id)s")
    
    upd_comments_sql=("update ordertracker ot set events=CONCAT_WS('|',right(ot.events, 50),%(schedule)s) where ot.ID=%(order_tracker_id)s")
    
    dic = {'schedule': row['scheduler_name'], 'order_tracker_id': row['order_tracker_id']}
    queue_db_updates(row['order_tracker_id'],upd_comments_sql,dic)
        
    try:
        if(row['order_type']=='Open'):
            ##print('before check_login_token entered at:',datetime.datetime.now())
            broker.check_login_token(row['Account_ID'],row)
            ##print('after check_login_token entered at:',datetime.datetime.now())
            
            #broker.check_login_token(primary_scrip_generic)
            ##print('before open_trade entered at:',datetime.datetime.now())
            r=open_trade(row,None,None,base_symbol_ltp)
            ##print('after check_login_token entered at:',datetime.datetime.now())
            
            if row['scheduler_name']=='NOWRE' and int(row['reentry_condition']) in (1,2) and r!=-1:
                send_notification(dic_accounts[row['Account_ID']]['chat_id'],'Re-Entry for Strategy Open Triggered '+row['name'])
            
            elif row['scheduler_name']=='NOWRE' and int(row['reentry_condition'])==3 and r!=-1:
                send_notification(dic_accounts[row['Account_ID']]['chat_id'],'Re-Execute for Strategy Open Triggered '+row['name'])
            
            else:
                send_notification(dic_accounts[row['Account_ID']]['chat_id'],'Strategy Open Triggered '+row['name'])

        elif(row['order_type']=='Close'):
            broker.check_login_token(row['Account_ID'],row)
            #broker.check_login_token(primary_scrip_generic)
            close_trade(row,None)
            send_notification(dic_accounts[row['Account_ID']]['chat_id'],'Strategy Close Triggered  '+row['name'])

        elif(row['order_type']=='Modify'):
            broker.check_login_token(row['Account_ID'],row)
            #broker.check_login_token(primary_scrip_generic)
            if modify_trade(row,None)!=-1:
                send_notification(dic_accounts[row['Account_ID']]['chat_id'],'Order Modify Triggered '+row['name'])

        elif(row['order_type']=='Squareoff'):
            broker.check_login_token(row['Account_ID'],row)
            #broker.check_login_token(primary_scrip_generic)
            squareoff_trade(row,None)
            send_notification(dic_accounts[row['Account_ID']]['chat_id'],'Order Close Triggered  '+row['name'])
        
        elif(row['order_type']=='Update'):
            broker.check_login_token(row['Account_ID'],row)
            update_trade(row,None)
            
    except Exception as e:
            mainlogger.exception(str(e))
            var = traceback.format_exc()
            queue_exception(var,row['order_tracker_id'])
            try: 
                handle_exception('Failed to place order account id: '+str([row['Account_ID'], row['order_type'], row['name'], row['scrip']]),str(e),dic_accounts[row['Account_ID']]['chat_id'],True)
                
                dic={'order_tracker_id':row['order_tracker_id']}
                queue_db_updates(row['order_tracker_id'],upd_sql,dic)
            except Exception as e:
                mainlogger.exception(str(e))

'''
##Thread level Parallelization
def old_run_scheduled_jobs_threader(df,tmp_instruments_by_symbol,schedule,mpp_args):
    
    #print("run_scheduled_jobs_threader")
    #sets required global variables in child process

    mpp_gv_restore(mpp_args)
    
    mainlogger.info('Function : run_scheduled_jobs_threader '+schedule) 
    futures_list=[]
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=env_max_workers) as executor:
            #print('executor created at:',datetime.datetime.now())
            
            for index,row in df.iterrows():
                try: 
                    #mainlogger.info('Order Info : '+str(row))
                    #print('released at:',datetime.datetime.now())
                    futures = executor.submit(run_scheduled_one_job, row, tmp_instruments_by_symbol, schedule) #run_scheduled_one_job(coredb,row)
                    futures_list.append(futures)
                except Exception as e:
                    mainlogger.exception(str(e))

            for future in concurrent.futures.as_completed(futures_list):
                try:
                    future.result()
                except Exception as e:
                    mainlogger.exception('Failure in threading run_scheduled_jobs_threader:'+str(e)) 
                    
    except Exception as e:
        mainlogger.exception('ForkProcess Failed: '+str(e))
        
'''
'''
def run_scheduled_worker_finishes(r):
    if r.done:
        try:
            #print("Order Worker Success:", r.result())
        except Exception as e:
            s=traceback.format_exc()
            #print('Order Worked Failed:'+ str(s))
'''
def mpp_gv_copy():
    #print('inside mpp_gv_copy')
    
    mainlogger.info('Function : mpp_gv_copy')
    
    try:
        args=Manager().dict()
        
        #args['pool']=pool
        
        args['broker']=broker
        
        args['nse_holdiays']=nse_holdiays
        
        args['group_chat_id']=group_chat_id
        args['order_tag_prefix']=order_tag_prefix
        args['thread_lock_timeout']=thread_lock_timeout
        
        args['logger_queue']=logger_queue

        args['primary_kite']=primary_kite
        args['primary_broker']=primary_broker
        args['zerodha_failed_order_hardcoded_id']=zerodha_failed_order_hardcoded_id

        args['env_max_workers']=env_max_workers
        args['dic_keywords']=dic_keywords
        args['ist']=ist
        args['amo_dummy_date']=amo_dummy_date
        args['emoji_warning']=emoji_warning

        args['angel_broker_code']=angel_broker_code
        args['zerodha_broker_code']=zerodha_broker_code
        args['finvasia_broker_code']=finvasia_broker_code
        args['alice_broker_code']=alice_broker_code
        args['iifl_broker_code']=iifl_broker_code
        args['flat_broker_code']=flat_broker_code

        args['limit_price_factor_sell']=limit_price_factor_sell
        args['limit_price_factor_buy']=limit_price_factor_buy

        #args['threadlist']=threadlist

        args['Instrument']=Instrument

        args['global_delta_next_trading_day']=global_delta_next_trading_day
        args['global_dt_today']=global_dt_today

        args['dic_accounts']=dic_accounts
        args['instruments_by_symbol']=instruments_by_symbol

        args['kite_instruments']=kite_instruments
        args['angel_instruments']=angel_instruments
        args['shoonya_instruments']=shoonya_instruments
        args['alice_instruments']=alice_instruments
        args['iifl_instruments']=iifl_instruments
        args['flattrade_instruments']=flattrade_instruments

        args['qltp']=qltp
        args['qdb']=qdb
        args['qsms']=qsms
        args['qorders']=qorders
        

        args['exp_weekly_def']=exp_weekly_def
        args['exp_weekly_fin']=exp_weekly_fin
        args['exp_weekly_midcp']=exp_weekly_midcp
        args['exp_weekly_bnf']=exp_weekly_bnf
        args['exp_monthly_def']=exp_monthly_def
        args['exp_monthly_fin']=exp_monthly_fin
        args['exp_monthly_midcp']=exp_monthly_midcp
        args['exp_monthly_bnf']=exp_monthly_bnf
        args['exp_monthly_plus_def']=exp_monthly_plus_def
        args['exp_monthly_plus_fin']=exp_monthly_plus_fin
        args['exp_monthly_plus_midcp']=exp_monthly_plus_midcp
        args['exp_monthly_plus_bnf']=exp_monthly_plus_bnf
        args['exp_future_weekly_def']=exp_future_weekly_def
        args['exp_future_weekly_fin']=exp_future_weekly_fin
        args['exp_future_weekly_midcp']=exp_future_weekly_midcp
        args['exp_future_weekly_bnf']=exp_future_weekly_bnf
        args['exp_future_monthly_def']=exp_future_monthly_def
        args['exp_future_monthly_fin']=exp_future_monthly_fin
        args['exp_future_monthly_midcp']=exp_future_monthly_midcp
        args['exp_future_monthly_bnf']=exp_future_monthly_bnf
        args['EXP_DAY_DEF']=EXP_DAY_DEF
        args['EXP_DAY_FIN']=EXP_DAY_FIN
        args['EXP_DAY_MIDCP']=EXP_DAY_MIDCP
        args['EXP_DAY_BNF']=EXP_DAY_BNF
        args['MONTHLY_EXP_DAY_DEF']=MONTHLY_EXP_DAY_DEF
        args['MONTHLY_EXP_DAY_FIN']=MONTHLY_EXP_DAY_FIN
        args['MONTHLY_EXP_DAY_MIDCP']=MONTHLY_EXP_DAY_MIDCP
        args['MONTHLY_EXP_DAY_BNF']=MONTHLY_EXP_DAY_BNF
        args['exp_weekly_sensex']=exp_weekly_sensex
        args['exp_monthly_sensex']=exp_monthly_sensex
        args['exp_monthly_plus_sensex']=exp_monthly_plus_sensex
        args['exp_future_weekly_sensex']=exp_future_weekly_sensex
        args['exp_future_monthly_sensex']=exp_future_monthly_sensex
        args['EXP_DAY_SENSEX']=EXP_DAY_SENSEX
        args['MONTHLY_EXP_DAY_SENSEX']=MONTHLY_EXP_DAY_SENSEX
    
        args['primary_scrip_generic']=primary_scrip_generic
        
        mainlogger.info('Function : mpp_gv_copy ended')
        return args
    
    except Exception as e:
        mainlogger.exception(str(e))
        #print('Failed:'+ str(e))
        
        return None
    
    
##copies required global variable to pass to child process
def mpp_worker_logger(logger_queue): 
    logging.info('inside mpp_worker_logger')

    try:
        worker_logger_configurer(logger_queue) 
        mainlogger = logging.getLogger('worker')
        mainlogger.info('i am a dedicated order process')
        return mainlogger
    
    except Exception as e:
        print('Failed:'+ str(e))
        
def mpp_gv_restore(args):
    global mainlogger
        
    mainlogger=mpp_worker_logger(args['logger_queue'])
    mainlogger.info('Function : mpp_gv_restore')
    try:
        #print('inside mpp_gv_restore')
        #global pool
        
        global broker
        global pool
        
        global nse_holdiays
        
        global group_chat_id
        global order_tag_prefix

        global thread_lock_timeout
        global primary_kite
        global primary_broker
        global zerodha_failed_order_hardcoded_id

        global env_max_workers
        global dic_keywords
        global ist
        global amo_dummy_date
        global emoji_warning

        global angel_broker_code
        global zerodha_broker_code
        global finvasia_broker_code
        global alice_broker_code
        global iifl_broker_code
        global flat_broker_code

        global limit_price_factor_sell
        global limit_price_factor_buy

        #global threadlist

        global Instrument

        global global_delta_next_trading_day
        global global_dt_today

        global dic_accounts
        global instruments_by_symbol

        global kite_instruments
        global angel_instruments
        global shoonya_instruments
        global alice_instruments
        global iifl_instruments
        global flattrade_instruments

        global qltp
        global qdb
        global qsms
        global qorders
        
        global exp_weekly_def
        global exp_weekly_fin
        global exp_weekly_midcp
        global exp_weekly_bnf
        global exp_monthly_def
        global exp_monthly_fin
        global exp_monthly_midcp
        global exp_monthly_bnf
        global exp_monthly_plus_def
        global exp_monthly_plus_fin
        global exp_monthly_plus_midcp
        global exp_monthly_plus_bnf
        global exp_future_weekly_def
        global exp_future_weekly_fin
        global exp_future_weekly_midcp
        global exp_future_weekly_bnf
        global exp_future_monthly_def
        global exp_future_monthly_fin
        global exp_future_monthly_midcp
        global exp_future_monthly_bnf
        global EXP_DAY_DEF
        global EXP_DAY_FIN
        global EXP_DAY_MIDCP
        global EXP_DAY_BNF
        global MONTHLY_EXP_DAY_DEF
        global MONTHLY_EXP_DAY_FIN
        global MONTHLY_EXP_DAY_MIDCP
        global MONTHLY_EXP_DAY_BNF
        
        global exp_weekly_sensex
        global exp_monthly_sensex
        global exp_monthly_plus_sensex
        global exp_future_weekly_sensex
        global exp_future_monthly_sensex
        global EXP_DAY_SENSEX
        global MONTHLY_EXP_DAY_SENSEX

        global primary_scrip_generic
        
        #mainlogger.info('alice len restore'+str(len(args['kite_instruments'])))
        pool= db_connect(dbname,1)
        redis_server = Redlock([{"host": redis_host, "port": int(redis_port), "db": int(redis_db)}])
        
        broker=args['broker']
        nse_holdiays=args['nse_holdiays']
        
        
        order_tag_prefix=args['order_tag_prefix']
        group_chat_id=args['group_chat_id']
        
        thread_lock_timeout=args['thread_lock_timeout']

        primary_kite=args['primary_kite']
        primary_broker=args['primary_broker']
        zerodha_failed_order_hardcoded_id=args['zerodha_failed_order_hardcoded_id']

        env_max_workers=args['env_max_workers']
        dic_keywords=args['dic_keywords']
        ist=args['ist']
        amo_dummy_date=args['amo_dummy_date']
        emoji_warning=args['emoji_warning']

        angel_broker_code=args['angel_broker_code']
        zerodha_broker_code=args['zerodha_broker_code']
        finvasia_broker_code=args['finvasia_broker_code']
        alice_broker_code=args['alice_broker_code']
        iifl_broker_code=args['iifl_broker_code']
        flat_broker_code=args['flat_broker_code']

        limit_price_factor_sell=args['limit_price_factor_sell']
        limit_price_factor_buy=args['limit_price_factor_buy']

        #threadlist=args['threadlist']

        Instrument=args['Instrument']

        global_delta_next_trading_day=args['global_delta_next_trading_day']
        global_dt_today=args['global_dt_today']

        dic_accounts=args['dic_accounts']
        instruments_by_symbol=args['instruments_by_symbol']

        kite_instruments=args['kite_instruments']
        flattrade_instruments=args['flattrade_instruments']
        angel_instruments=args['angel_instruments']
        shoonya_instruments=args['shoonya_instruments']
        alice_instruments=args['alice_instruments']
        iifl_instruments=args['iifl_instruments']

        qltp=args['qltp']
        qdb=args['qdb']
        qsms=args['qsms']
        qorders=args['qorders']

        exp_weekly_def=args['exp_weekly_def']
        exp_weekly_fin=args['exp_weekly_fin']
        exp_weekly_midcp=args['exp_weekly_midcp']
        exp_weekly_bnf=args['exp_weekly_bnf']
        exp_monthly_def=args['exp_monthly_def']
        exp_monthly_fin=args['exp_monthly_fin']
        exp_monthly_midcp=args['exp_monthly_midcp']
        exp_monthly_bnf=args['exp_monthly_bnf']
        exp_monthly_plus_def=args['exp_monthly_plus_def']
        exp_monthly_plus_fin=args['exp_monthly_plus_fin']
        exp_monthly_plus_midcp=args['exp_monthly_plus_midcp']
        exp_monthly_plus_bnf=args['exp_monthly_plus_bnf']
        exp_future_weekly_def=args['exp_future_weekly_def']
        exp_future_weekly_fin=args['exp_future_weekly_fin']
        exp_future_weekly_midcp=args['exp_future_weekly_midcp']
        exp_future_weekly_bnf=args['exp_future_weekly_bnf']
        exp_future_monthly_def=args['exp_future_monthly_def']
        exp_future_monthly_fin=args['exp_future_monthly_fin']
        exp_future_monthly_midcp=args['exp_future_monthly_midcp']
        exp_future_monthly_bnf=args['exp_future_monthly_bnf']
        EXP_DAY_DEF=args['EXP_DAY_DEF']
        EXP_DAY_FIN=args['EXP_DAY_FIN']
        EXP_DAY_MIDCP=args['EXP_DAY_MIDCP']
        EXP_DAY_BNF=args['EXP_DAY_BNF']
        MONTHLY_EXP_DAY_DEF=args['MONTHLY_EXP_DAY_DEF']
        MONTHLY_EXP_DAY_FIN=args['MONTHLY_EXP_DAY_FIN']
        MONTHLY_EXP_DAY_MIDCP=args['MONTHLY_EXP_DAY_MIDCP']
        MONTHLY_EXP_DAY_BNF=args['MONTHLY_EXP_DAY_BNF']
        
        exp_weekly_sensex=args['exp_weekly_sensex']
        exp_monthly_sensex=args['exp_monthly_sensex']
        exp_monthly_plus_sensex=args['exp_monthly_plus_sensex']
        exp_future_weekly_sensex=args['exp_future_weekly_sensex']
        exp_future_monthly_sensex=args['exp_future_monthly_sensex']
        EXP_DAY_SENSEX=args['EXP_DAY_SENSEX']
        MONTHLY_EXP_DAY_SENSEX=args['MONTHLY_EXP_DAY_SENSEX']
        
        primary_scrip_generic=args['primary_scrip_generic']
        
    except Exception as e:
        mainlogger.exception(str(e))
        #print('Failed:'+ str(e))

def run_scheduled_orders_enqueue(order,schedule,base_symbol_ltp):
    mainlogger.info('Function : run_scheduled_orders_enqueue order_tracker_id:'+str(order['order_tracker_id']))
    try:
        qorders.put({'order':order,'schedule':schedule,'base_symbol_ltp':base_symbol_ltp})
    except Exception as e:
        mainlogger.exception('Failed run_scheduled_orders_enqueue failed to queue order '+str(e))

def run_scheduled_orders_dequeue(qorders):
    mainlogger.info('Function : run_scheduled_orders_dequeue')
    while True:
        try:
            #mainlogger.info('Orders in queue:'+str(qorders.qsize()))
            while not qorders.empty():
                try:
                    item = qorders.get()
                    mainlogger.info('order_tracker_id:'+str(item['order']['order_tracker_id'])+' scheduler:'+item['schedule'])   
                    run_scheduled_one_job(item['order'], None,item['schedule'],item['base_symbol_ltp'])
                except Exception as e:
                    mainlogger.exception('Function: run_scheduled_orders_dequeue failed to process order '+ str(e))

            time.sleep(1)
            
        except Exception as e:
            mainlogger.exception('run_scheduled_orders_dequeue failed '+ str(e))
            send_notification(group_chat_id,'run_scheduled_orders_dequeue failed - will be retried')
            time.sleep(30)
            
def run_scheduled_jobs_parallel(schedule):
    mainlogger.info('Function : run_scheduled_jobs_parallel '+schedule) 
    
    results = []
    
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()
        
        #create temp table for performance
        tmp_drop_sql=("DROP TEMPORARY TABLE IF EXISTS tmp_active_orders")
        
        if schedule=='ALLNOW':
            tmp_sql=("create or replace temporary table tmp_active_orders as select va.*,ot.id as order_tracker_id,ot.order_group_id, ot.order_type, ot.order_id, ot.scrip, ot.scrip_generic, ot.direction as ot_direction, ot.trigger_price as ot_trigger_price, case when ot.sl_trail_y is null then ot.limit_price else ot.sl_trail_y end as ot_limit_price, ot.parent_filled_price, ot.parent_filled_qty, ot.scheduler_name, ot.sched_order_time, ot.ot_order_type, ot.reentry_max as ot_reentry_max, ot.tp_reentry_max as ot_tp_reentry_max, i.symbol as base_symbol, comments, ot.wnt_strategy_end_time   from ordertracker ot left outer join vw_activestrategies_all va on (ot.OrderLegs_ID=va.ID and ot.Account_ID=va.Account_ID and case when va.AccountGroup_ID is not null then ot.AccountGroup_ID =va.AccountGroup_ID and ot.StrategyGroup_ID =va.StrategyGroup_ID else true end) left outer join instrument i on (va.base_instrument_id=i.ID) "
             "where va.Account_ID is not null and ot.scheduler_name like '%%NOW%%' and cast(ot.last_updated as date) >=current_date()-1 and ot.active_flag=1")     
        else:
            tmp_sql=("create or replace temporary table tmp_active_orders as select va.*,ot.id as order_tracker_id,ot.order_group_id, ot.order_type, ot.order_id, ot.scrip, ot.scrip_generic, ot.direction as ot_direction, ot.trigger_price as ot_trigger_price, case when ot.sl_trail_y is null then ot.limit_price else ot.sl_trail_y end as ot_limit_price, ot.parent_filled_price, ot.parent_filled_qty, ot.scheduler_name, ot.sched_order_time, ot.ot_order_type, ot.reentry_max as ot_reentry_max,ot.tp_reentry_max as ot_tp_reentry_max, i.symbol as base_symbol, comments, ot.wnt_strategy_end_time  from ordertracker ot left outer join vw_activestrategies_all va on (ot.OrderLegs_ID=va.ID and ot.Account_ID=va.Account_ID and case when va.AccountGroup_ID is not null then ot.AccountGroup_ID =va.AccountGroup_ID and ot.StrategyGroup_ID =va.StrategyGroup_ID else true end) left outer join instrument i on (va.base_instrument_id=i.ID) "
             "where va.Account_ID is not null and sched_order_time =STR_TO_DATE(%(scheduler_name)s,'%%Y%%m%%d%%H%%i') and (cast(sched_order_time as date) = cast(current_timestamp() as date)) and (scheduler_name like '202%%' or scheduler_name='Advanced' or scheduler_name like '%%NOW%%' or scheduler_name='Processing') and ot.active_flag=1")
                      
        sql=("select * from tmp_active_orders")
        upd_sql=("update ordertracker ot inner join tmp_active_orders t  on t.order_tracker_id=ot.ID set ot.scheduler_name='Processing' where ot.active_flag=1")
        
        #create tmp table
        cursor.execute(tmp_sql, {"scheduler_name":schedule})

        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        tmp_instruments_by_symbol= copy.deepcopy(instruments_by_symbol)
        
        #update records status to processing started
        try:
            cursor.execute(upd_sql)
            coredb.commit()
        except Exception as e:
            time.sleep(1)
            try:
                cursor.execute(upd_sql)
                coredb.commit()
            except Exception as e:
                mainlogger.exception(str(e)) 
        
        for index, row in df.iterrows():
            try:
                base_symbol_ltp=tmp_instruments_by_symbol[row['base_symbol']]['ltp']
                run_scheduled_orders_enqueue(row,schedule,base_symbol_ltp)
            except Exception as e:
                mainlogger.exception('run_scheduled_jobs_parallel: failed to send order '+str(e)) 
               
        cursor.execute(tmp_drop_sql)         
        coredb.close()
        
    except Exception as e:
        mainlogger.exception(str(e)) 
        coredb.close()

def order_processors_start_threads(mpp_args):
    
    mpp_gv_restore(mpp_args)
    
    mainlogger.info('Function : order_processors_start_worker starting order threads')
    
    try:
        futures_list = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=env_max_workers) as executor:
            for i in range(env_max_workers):
                try: 
                    futures = executor.submit(run_scheduled_orders_dequeue,qorders) #run_scheduled_one_job(coredb,row)
                    futures_list.append(futures)
                except Exception as e:
                    mainlogger.exception(str(e))

            for future in concurrent.futures.as_completed(futures_list):
                try:
                    future.result()
                except Exception as e:
                    handle_exception('worker_start_order_processors Failed',str(e),'NA',False)
                    mainlogger.exception('Failure in threading order_processors_start_threads:'+str(e))         
    except Exception as e:
        mainlogger.exception('Order Threads Creation Failed: '+str(e))

def order_processors_start_workers():
    mainlogger.info('Function : order_processors_start_workers starting order processes')
    #copies required global variable to pass to child process
    mpp_args=mpp_gv_copy()
    
    results=[]
    
    with get_context("spawn").Pool(env_max_order_processes) as process_pool:
        for i in range(env_max_workers):
            try:
                #results.append(process_pool.apply_async(run_scheduled_jobs_threader, args=(df[i:i+step],tmp_instruments_by_symbol,schedule,mpp_args, ),))
                mainlogger.info('starting worker process') 
                results.append(process_pool.apply_async(order_processors_start_threads, args=(mpp_args,)))
                time.sleep(5)
            except Exception as e:
                handle_exception('worker_start_order_processors Failed',str(e),'NA',False)
                mainlogger.exception('worker_start_order_processors Failed:'+str(e)) 
                time.sleep(2)
                
        process_pool.close()
        process_pool.join()

        for r in results:
            try:
                r.get()
            except Exception as e:
                handle_exception('Orders Process Failed:',str(e),'NA',False)

'''
#sets required global variables in child process
def old_run_scheduled_jobs_parallel(schedule):
    mainlogger.info('Function : run_scheduled_jobs_parallel '+schedule) 
    
    futures_list = []
    results = []
    
    ##print('run_scheduled_jobs_parallel entered at:',datetime.datetime.now())
    
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()
        
        #create temp table for performance
        tmp_drop_sql=("DROP TEMPORARY TABLE IF EXISTS tmp_active_orders")
        
        if schedule=='ALLNOW':
        #if schedule in ('NOW','EXITNOWUI','EXECUTENOWUI','NOWRE','NOWSE','NOWWNT') :
            tmp_sql=("create or replace temporary table tmp_active_orders as select va.*,ot.id as order_tracker_id,ot.order_group_id, ot.order_type, ot.order_id, ot.scrip, ot.scrip_generic, ot.direction as ot_direction, ot.trigger_price as ot_trigger_price, case when ot.sl_trail_y is null then ot.limit_price else ot.sl_trail_y end as ot_limit_price, ot.parent_filled_price, ot.parent_filled_qty, ot.scheduler_name, ot.sched_order_time, ot.ot_order_type, ot.reentry_max as ot_reentry_max, i.symbol as base_symbol, comments   from ordertracker ot left outer join vw_activestrategies_all va on (ot.OrderLegs_ID=va.ID and ot.Account_ID=va.Account_ID) left outer join instrument i on (va.base_instrument_id=i.ID) "
             "where va.Account_ID is not null and ot.scheduler_name like '%%NOW%%' and cast(ot.last_updated as date) >=current_date()-1 and ot.active_flag=1")
            
            #upd_sql=("update ordertracker ot set ot.scheduler_name='Processing' where ot.scheduler_name like '%%NOW%%' and cast(ot.last_updated as date)=current_date() and ot.active_flag=1")
            
        else:
            tmp_sql=("create or replace temporary table tmp_active_orders as select va.*,ot.id as order_tracker_id,ot.order_group_id, ot.order_type, ot.order_id, ot.scrip, ot.scrip_generic, ot.direction as ot_direction, ot.trigger_price as ot_trigger_price, case when ot.sl_trail_y is null then ot.limit_price else ot.sl_trail_y end as ot_limit_price, ot.parent_filled_price, ot.parent_filled_qty, ot.scheduler_name, ot.sched_order_time, ot.ot_order_type, ot.reentry_max as ot_reentry_max, i.symbol as base_symbol, comments  from ordertracker ot left outer join vw_activestrategies_all va on (ot.OrderLegs_ID=va.ID and ot.Account_ID=va.Account_ID) left outer join instrument i on (va.base_instrument_id=i.ID) "
             "where va.Account_ID is not null and sched_order_time =STR_TO_DATE(%(scheduler_name)s,'%%Y%%m%%d%%H%%i') and (cast(sched_order_time as date) = cast(current_timestamp() as date)) and (scheduler_name like '202%%' or scheduler_name='Advanced' or scheduler_name like '%%NOW%%' or scheduler_name='Processing') and ot.active_flag=1")
            
            #upd_sql=("update ordertracker ot set ot.scheduler_name='Processing' where sched_order_time=STR_TO_DATE(%(scheduler_name)s,'%%Y%%m%%d%%H%%i') and (cast(sched_order_time as date) = cast(current_timestamp() as date)) and (scheduler_name like '202%%' or scheduler_name='Advanced' or scheduler_name like '%%NOW%%') and ot.active_flag=1")
        
        sql=("select * from tmp_active_orders")
        
        upd_sql=("update ordertracker ot inner join tmp_active_orders t  on t.order_tracker_id=ot.ID set ot.scheduler_name='Processing' where ot.active_flag=1")
        
        #create tmp table
        cursor.execute(tmp_sql, {"scheduler_name":schedule})

        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        tmp_instruments_by_symbol= copy.deepcopy(instruments_by_symbol)
        
        try:
            #update records status to processing started
            cursor.execute(upd_sql)
            coredb.commit()
        except Exception as e:
            time.sleep(1)
            try:
                cursor.execute(upd_sql)
                coredb.commit()
            except Exception as e:
                mainlogger.exception(str(e)) 
        
        ##print('executor creation at:',datetime.datetime.now())
        
        ##Process level Parallelization
        total_orders=len(df)
        max_order_processes=env_max_order_processes
        orders_per_process=math.ceil(total_orders/max_order_processes)

        start = 0
        end = total_orders
        step = orders_per_process
        
        #parallel processing     
        if total_orders>0 and env_mpp==1:
            
            #copies required global variable to pass to child process
            mpp_args=mpp_gv_copy()
            
            with get_context("spawn").Pool(max_order_processes) as process_pool:
                for i in range(start,end,step):
                    try:
                        #print("process_pool.apply_async step")
                        results.append(process_pool.apply_async(run_scheduled_jobs_threader, args=(df[i:i+step],tmp_instruments_by_symbol,schedule,mpp_args, ),))
                    except Exception as e:
                        mainlogger.exception(str(e)) 
                        mainlogger.exception('Retry run_scheduled_jobs_threader MPP after 2 seconds') 
                        time.sleep(2)
                        process_pool.apply_async(run_scheduled_jobs_threader, args=(df[i:i+step],tmp_instruments_by_symbol,schedule,mpp_args, ),)
                        
                process_pool.close()
                process_pool.join()
                
                for r in results:
                    try:
                        r.get()
                    except Exception as e:
                        handle_exception('Batch of Orders Process Failed:'+schedule,str(e),'NA',False) 
                    
            ##
            executor = get_reusable_executor(max_workers=4, timeout=0)
            for i in range(start,end,step):
                try:
                    order_worker_r=executor.submit(run_scheduled_jobs_threader, df[i:i+step],tmp_instruments_by_symbol,schedule)
                    order_worker_r.add_done_callback(run_scheduled_worker_finishes)
                except Exception as e:
                    mainlogger.exception(str(e)) 
                    mainlogger.exception('Retry run_scheduled_jobs_threader MPP after 2 seconds') 
                    time.sleep(2)
                    order_worker_r=executor.submit(run_scheduled_jobs_threader, df[i:i+step],tmp_instruments_by_symbol,schedule)
                    order_worker_r.add_done_callback(run_scheduled_worker_finishes)
            ###
        #no parallel processing        
        elif total_orders>0 and env_mpp==0:
            for i in range(start,end,step):
                try:
                    run_scheduled_jobs_threader(df[i:i+step],tmp_instruments_by_symbol,schedule)
                except Exception as e:
                    mainlogger.exception(str(e)) 
                    mainlogger.exception('Retry run_scheduled_jobs_threader after 2 seconds') 
                    time.sleep(2)
                    run_scheduled_jobs_threader(df[i:i+step],tmp_instruments_by_symbol,schedule)
        
        #with Parallel(n_jobs=max_order_processes, backend='multiprocessing') as parallel:
        #    parallel([delayed(run_scheduled_jobs_threader)(df[i:i+step],tmp_instruments_by_symbol,schedule) for i in range(start,end,step)])
        
             
        cursor.execute(tmp_drop_sql)         
        coredb.close()
        #pool.close()
        
    except Exception as e:
        mainlogger.exception(str(e)) 
        coredb.close()
        #pool.close()
        
''' 
'''
def run_sl_scheduled_jobs_old(schedule):
    mainlogger.info('Function : run_sl_scheduled_jobs '+schedule) 
    
    upd_sql=("update ordertracker ot set scheduler_name='Failed' where ot.ID=%(order_tracker_id)s")
    futures_list=[]
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()
        
        #sql=("select va.*,ot.id as order_tracker_id,ot.order_group_id, ot.order_type, ot.ot_order_type, ot.order_id, ot.scrip, ot.scrip_generic, ot.direction as ot_direction, ot.parent_filled_price, ot.parent_filled_qty, ot.trigger_price as ot_trigger_price, ot.limit_price as ot_limit_price, ot.sched_order_time, ot.scheduler_name, ot.sl_order_response_json, i.symbol as base_symbol, comments from ordertracker ot left outer join vw_activestrategies_all va on (ot.OrderLegs_ID=va.ID and ot.Account_ID=va.Account_ID) left outer join instrument i on (va.base_instrument_id=i.ID) "
        #     "where va.Account_ID is not null and (ot.scheduler_name like '202%%' or ot.scheduler_name='Advanced') and ot.sl_scheduler_name=%(scheduler_name)s and (cast(amo_date as date) =  current_date()) and ot.active_flag=1")
        sql = ("select va.*,ot.id as order_tracker_id,ot.order_group_id, ot.order_type, ot.ot_order_type, ot.order_id, ot.scrip, ot.scrip_generic, ot.direction as ot_direction, ot.parent_filled_price, ot.parent_filled_qty, ot.trigger_price as ot_trigger_price, ot.limit_price as ot_limit_price, ot.sched_order_time, ot.scheduler_name, ot.sl_order_response_json, i.symbol as base_symbol, comments from ordertracker ot left outer join vw_activestrategies_all va on (ot.OrderLegs_ID=va.ID and ot.Account_ID=va.Account_ID) left outer join instrument i on (va.base_instrument_id=i.ID) "
             "where va.Account_ID is not null and (ot.scheduler_name like '202%%' or ot.scheduler_name='Advanced') and not exists (select 1 from vw_modifyeligibleorders_v3 vmv where vmv.OrderTrackerID=ot.ID) and ot.sl_scheduler_name=%(scheduler_name)s and (cast(amo_date as date) =  current_date()) and ot.active_flag=1"
              )
        
        df = pd.read_sql(sql, coredb, params={"scheduler_name":schedule})
        
        df = df.replace({np.nan: None})
        
        ##print(df)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
            for index,row in df.iterrows():
                try: 
                    mainlogger.info('Order Info : '+str(row))
                    futures = executor.submit(open_sl_trade, row, coredb) #run_scheduled_one_job(coredb,row)
                    futures_list.append(futures)
                except Exception as e:
                    try: 
                        handle_exception('Failed to place SL order account id: '+str([row['Account_ID'], row['order_type'], row['name'], row['scrip']]),str(e),dic_accounts[row['Account_ID']]['chat_id'],True)
                        cursor.execute(upd_sql,{'order_tracker_id':row['order_tracker_id']})
                        coredb.commit()
                    except Exception as e:
                        mainlogger.exception(str(e))
        
                    
        coredb.close()
    except Exception as e:
        mainlogger.exception(str(e)) 
        coredb.close()
'''
def run_sl_scheduled_jobs(schedule):
    mainlogger.info('Function : run_sl_scheduled_jobs '+schedule) 
    
    upd_sql=("update ordertracker ot set scheduler_name='Failed' where ot.ID=%(order_tracker_id)s")
    futures_list=[]
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()
        
        #create temp table for performance
        tmp_drop_sql=("DROP TEMPORARY TABLE IF EXISTS tmp_sl_active_orders")

        tmp_sql = ("create or replace temporary table tmp_sl_active_orders as select va.*,ot.id as order_tracker_id,ot.order_group_id, ot.order_type, ot.ot_order_type, ot.order_id, ot.scrip, ot.scrip_generic, ot.direction as ot_direction, ot.parent_filled_price, ot.parent_filled_qty, ot.trigger_price as ot_trigger_price, ot.limit_price as ot_limit_price, ot.sched_order_time, ot.scheduler_name, ot.sl_order_response_json, i.symbol as base_symbol, comments from ordertracker ot left outer join vw_activestrategies_all va on (ot.OrderLegs_ID=va.ID and ot.Account_ID=va.Account_ID) left outer join instrument i on (va.base_instrument_id=i.ID) "
             "where va.Account_ID is not null and (ot.scheduler_name like '202%%' or ot.scheduler_name='Advanced') and not exists (select 1 from vw_modifyeligibleorders_v3 vmv where vmv.OrderTrackerID=ot.ID) and ot.sl_scheduler_name=%(scheduler_name)s and (cast(amo_date as date) =  current_date()) and ot.active_flag=1"
            )
        
        sql=("select * from tmp_sl_active_orders")
        
        upd_sql=("update ordertracker ot inner join tmp_sl_active_orders t  on t.order_tracker_id=ot.ID set ot.scheduler_name='Processing' where ot.active_flag=1")
        
        cursor.execute(tmp_sql, {"scheduler_name":schedule})
        
        df = pd.read_sql(sql, coredb)
        df = df.replace({np.nan: None})
        
        #update records status to processing started
        try:
            cursor.execute(upd_sql)
            coredb.commit()
        except Exception as e:
            time.sleep(1)
            try:
                cursor.execute(upd_sql)
                coredb.commit()
            except Exception as e:
                mainlogger.exception(str(e)) 

        ##print(df)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
            for index,row in df.iterrows():
                try: 
                    mainlogger.info('Order Info : '+str(row))
                    futures = executor.submit(open_sl_trade, row, coredb) #run_scheduled_one_job(coredb,row)
                    futures_list.append(futures)
                except Exception as e:
                    try: 
                        handle_exception('Failed to place SL order account id: '+str([row['Account_ID'], row['order_type'], row['name'], row['scrip']]),str(e),dic_accounts[row['Account_ID']]['chat_id'],True)
                        cursor.execute(upd_sql,{'order_tracker_id':row['order_tracker_id']})
                        coredb.commit()
                    except Exception as e:
                        mainlogger.exception(str(e))
        
        cursor.execute(tmp_drop_sql)            
        coredb.close()
    except Exception as e:
        mainlogger.exception(str(e)) 
        coredb.close()

def cleanup_orderstatus():
    mainlogger.info('Function : cleanup_orderstatus')
    #capture latest LTP
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()
        upd_sql=("update ordertracker ot inner join vw_cleanup_orderttracker vo on ot.ID = vo.OrderTrackerID "
                 "set ot.scheduler_name=vo.status , comments=CONCAT_WS('|',ot.comments,'Cleanup orderstatus Job')")
        rows=cursor.execute(upd_sql)
        
        coredb.commit()
        cursor.close()
        
    except Exception as e:
        mainlogger.exception(str(e))
        
def attach_scheduler():
    mainlogger.info('Function : attach_scheduler')
    
    try:
        calculate_scheduled_time()
        
        coredb = pool.connection()
        
        cleanup_orderstatus()
        
        sql=("select distinct scheduler_name from ordertracker"
             " where active_flag=1 and scheduler_name like '202%%' and (cast(sched_order_time as date) = cast(current_timestamp() as date))")
        
        df = pd.read_sql(sql, coredb) 
        df = df.replace({np.nan: None})
  
        for index, row in df.iterrows():
            try: 
                ##print(row['scheduler_name'])
                s=row['scheduler_name']
                
                #Run 10 seconds before the scheduled time
                new_sched_time = (datetime.datetime.strptime(s, '%Y%m%d%H%M') - datetime.timedelta(minutes=1))
                s=new_sched_time.strftime('%Y%m%d%H%M')
                
                if len([j.name for j in scheduler.get_jobs() if j.name == s])>0:
                    continue

                trigger = CronTrigger(
                    year=s[:4], month=s[4:6], day=s[6:8], hour=s[8:10], minute=s[10:12], second="50"
                    #year=s[:4], month=s[4:6], day=s[6:8], hour=s[8:10], minute=s[10:12], second="0"
                    #year=s[:4], month=s[4:6], day="03", hour="00", minute="10", second="0"
                )

                scheduler.add_job(
                    run_scheduled_jobs_parallel,  ##parallel version 
                    trigger=trigger,
                    args=[row['scheduler_name']],
                    name=s,
                )     
            except Exception as e:
                try:
                    handle_exception('Attach Scheduler Failed',str(e),'NA',False)
                except Exception as e:
                    mainlogger.exception(str(e))
        coredb.close()
        
    except Exception as e:
        mainlogger.exception(str(e))
        coredb.close()

def attach_sl_scheduler():
    mainlogger.info('Function : attach_sl_scheduler')
    
    try:
        coredb = pool.connection()
        sql=("select distinct sl_scheduler_name from ordertracker"
             " where active_flag=1 and scheduler_name like '202%%' and sl_scheduler_name like '202%%' and (cast(amo_date as date) =  current_date()) and sl_scheduler_name is not null")
        
        df = pd.read_sql(sql, coredb) 
        df = df.replace({np.nan: None})
  
        for index, row in df.iterrows():
            try: 
                ##print(row['scheduler_name'])
                s=row['sl_scheduler_name']
                
                trigger = CronTrigger(
                    year=s[:4], month=s[4:6], day=s[6:8], hour=s[8:10], minute=s[10:12], second="5"
                    #year=s[:4], month=s[4:6], day="03", hour="00", minute="10", second="0"
                )

                scheduler.add_job(
                    run_sl_scheduled_jobs,
                    trigger=trigger,
                    args=[s],
                    name='SL'+s,
                )     
            except Exception as e:
                try:
                    handle_exception('Attach SL Scheduler Failed',str(e),'NA',False)
                except Exception as e:
                    mainlogger.exception(str(e))
        coredb.close()
        
    except Exception as e:
        mainlogger.exception(str(e))
        coredb.close()
                


# # Function- Get Close DateTime

# %%


def get_next_trading_day_delta():
    mainlogger.info('Function : get_next_trading_day_delta')
    
    d=1
    dt=datetime.datetime.now(ist)
    
    while True:
        t=dt+datetime.timedelta(days=d)
        if t.strftime("%m-%d-%y") not in nse_holdiays and t.weekday() not in (5,6):
            break
        
        d=d+1
    
    return d


def get_day_before_trading_day_delta(dt):
    mainlogger.info('Function : get_day_before_trading_day_delta')
    
    d=0
    #dt=datetime.datetime.now(ist)
    
    while True:
        t=dt-datetime.timedelta(days=d)
        if t.strftime("%m-%d-%y") not in nse_holdiays and t.weekday() not in (5,6):
            return t
            
        d=d+1

def get_day_after_trading_day_delta(dt):
    mainlogger.info('Function : get_day_after_trading_day_delta')
    
    d=0
    #dt=datetime.datetime.now(ist)
    
    while True:
        t=dt+datetime.timedelta(days=d)
        if t.strftime("%m-%d-%y") not in nse_holdiays and t.weekday() not in (5,6):
            return t
            
        d=d+1
        
def get_upcoming_date_by_dayname(d, dayname):
    mainlogger.info('Function : get_upcoming_date_by_dayname')
    
    if dayname=='Monday':
        weekday=0
    elif dayname=='Tuesday':
        weekday=1
    elif dayname=='Wednesday':
        weekday=2
    elif dayname=='Thursday':
        weekday=3
    elif dayname=='Friday':
        weekday=4
        
    days_ahead = weekday - d.weekday()
    if days_ahead <= 0: # Target day already happened this week
        days_ahead += 7
        
    upcoming_date= d + datetime.timedelta(days_ahead)
    upcoming_date= get_day_after_trading_day_delta(upcoming_date) #to hanlde trading holidays

    return upcoming_date
    

def get_close_datetime(row):
    ##print("get_close_datetime")
    mainlogger.info('Function : get_close_datetime')
    
    dt=datetime.datetime.now(ist)
    
    close_day=keyword_lookup(row['close_day']) 
    
    #HANDLE FINNIFTY EXPIRY
    if row['symbol']=='BANKNIFTY':
        exp_weekly=exp_weekly_bnf
        exp_monthly=exp_monthly_bnf
        exp_monthly_plus=exp_monthly_plus_bnf
        exp_future_weekly=exp_future_weekly_bnf
        exp_future_monthly=exp_future_monthly_bnf
        EXP_DAY=EXP_DAY_BNF
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_BNF
        
    elif row['symbol']=='FINNIFTY':
        exp_weekly=exp_weekly_fin
        exp_monthly=exp_monthly_fin
        exp_monthly_plus=exp_monthly_plus_fin
        exp_future_weekly=exp_future_weekly_fin
        exp_future_monthly=exp_future_monthly_fin
        EXP_DAY=EXP_DAY_FIN
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_FIN

    elif row['symbol']=='MIDCPNIFTY':
        exp_weekly=exp_weekly_midcp
        exp_monthly=exp_monthly_midcp
        exp_monthly_plus=exp_monthly_plus_midcp
        exp_future_weekly=exp_future_weekly_midcp
        exp_future_monthly=exp_future_monthly_midcp
        EXP_DAY=EXP_DAY_MIDCP
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_MIDCP
        
    elif row['symbol']=='SENSEX':
        exp_weekly=exp_weekly_sensex
        exp_monthly=exp_monthly_sensex
        exp_monthly_plus=exp_monthly_plus_sensex
        exp_future_weekly=exp_future_weekly_sensex
        exp_future_monthly=exp_future_monthly_sensex
        EXP_DAY=EXP_DAY_SENSEX
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_SENSEX
        
    else:
        exp_weekly=exp_weekly_def
        exp_monthly=exp_monthly_def
        exp_monthly_plus=exp_monthly_plus_def
        exp_future_weekly=exp_future_weekly_def
        exp_future_monthly=exp_future_monthly_def
        EXP_DAY=EXP_DAY_DEF
        MONTHLY_EXP_DAY=MONTHLY_EXP_DAY_DEF
    
    if close_day in ('Monday','Tuesday','Wednesday','Thursday','Friday'):
        
        if row['sched_order_time'].date() == datetime.datetime.now().date():
            d1=get_upcoming_date_by_dayname(dt, close_day)
            return d1.strftime("%m-%d-%y")+' '+row['close_time']
        else:
            d1=get_upcoming_date_by_dayname(row['sched_order_time'], close_day)
            return d1.strftime("%m-%d-%y")+' '+row['close_time']
        
        #d1=get_upcoming_date_by_dayname(dt, close_day)
        #delta=global_delta_next_trading_day-1
        #return d1.strftime("%m-%d-%y")+' '+row['close_time']
        
    elif close_day=='SAME_DAY':
        return dt.strftime("%m-%d-%y")+' '+row['close_time']
    
    elif close_day=='NEXT_DAY':
        if row['sched_order_time'].date() == datetime.datetime.now().date():
            dt=row['sched_order_time'] #next day based on order config time not on entry time WNT scenario
            delta=global_delta_next_trading_day
            return (dt+ datetime.timedelta(days=delta)).strftime("%m-%d-%y")+' '+row['close_time']
        else:
            return datetime.datetime.now().strftime("%m-%d-%y")+' '+row['close_time']
    
    elif close_day=='CONTRACT_EXP_DAY':
        exp=get_contract_expiry(row)
        d1=datetime.datetime.strptime(exp, '%d %b%y').date()
        
        return (d1.strftime("%m-%d-%y")+' '+row['close_time'])
    
    elif close_day=='CONTRACT_EXP_BEFORE':
        exp=get_contract_expiry(row)
        d1=datetime.datetime.strptime(exp, '%d %b%y').date()
        d2=d1-datetime.timedelta(days=row['close_day_diff'])
        d2=get_day_after_trading_day_delta(d2)
        
        return (d2.strftime("%m-%d-%y")+' '+row['close_time'])


##print(get_close_datetime(row))


# # Check Advanced Trades - Candle TA



# %%


def check_advanced_trades():
    mainlogger.info('Function : check_advanced_trades')
    
    coredb=pool.connection()
    
    #refresh_advanced_instruments(coredb)
    
    # Open Advanced Orders and SL SquareOff Orders
    sql=( "select t.*, i.symbol as nfo_symbol  from (  "
         "select distinct i.symbol, i.exchange, indicator_name, candle_duration, expr, params, le.is_volume_indicator, "
         "va.Instrument_ID, va.instrument_type, range_start, range_end, case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as scrip_generic "
         "from vw_activestrategies_all va,orderlegsextension le, instrument i,  ordertracker ot  "
         "where va.ID=le.OrderLeg_ID  and i.ID =va.base_instrument_id and va.ID =ot.OrderLegs_ID  "
         "and  (cast(ot.created_at as date)= current_date()) and (ot.scheduler_name='Advanced') and ot.order_type='Open'  "
         "union  "
         "select distinct i.symbol, i.exchange, indicator_name, candle_duration, expr, params, le.is_volume_indicator, "
         "va.Instrument_ID, va.instrument_type, range_start, range_end, case when ot.scrip_generic is null or va.instrument_type=801 then 'NA' else ot.scrip_generic end as scrip_generic "
         "from vw_activestrategies_all va,orderlegsextension le, instrument i,  ordertracker ot  "
         "where va.ID=le.OrderLeg_ID  and i.ID =va.base_instrument_id and va.ID =ot.OrderLegs_ID  "
         "and  (cast(ot.created_at as date) >= current_date() - interval 30 day) and (ot.scheduler_name like '202%')  "
         "and ot.order_type in ('Squareoff', 'Close') "
         ")as t, instrument i  where t.Instrument_ID=i.ID "
        )
    
    upd_sql=( "update ordertracker ot,vw_activestrategies_all va, orderlegsextension le, instrument i  "
             "set ot.scheduler_name ='NOW', ot.comments=CONCAT_WS('|',ot.comments,'check_advanced_trades') "
             "where va.ID=le.OrderLeg_ID and i.ID =va.base_instrument_id  "
             "and case when monitor_start is not null and monitor_end is not null then  "
             "concat(current_date(), ' ', monitor_start)<=current_timestamp() and current_timestamp()<=concat(current_date(), ' ', monitor_end)  "
             "else true  "
             "end  "
             "and va.ID=ot.OrderLegs_ID and ot.order_type=le.order_type and ot.scheduler_name not in ('Complete', 'Processing', 'Rejected', 'Failed', 'Not Today', 'Cancelled')  "
             "and (cast(ot.created_at as date) >= current_date()- interval 30 day) and (scheduler_name like '202%%' or scheduler_name='Advanced')   "
             "and le.indicator_name=%(indicator_name)s "
             "and le.candle_duration=%(candle_duration)s "
             "and le.params=%(params)s and le.expr=%(expr)s   "
             "and i.symbol=%(symbol)s and i.exchange=%(exchange)s  "
             "and va.instrument_type=%(instrument_type)s "
             "and case when va.instrument_type=800 and ot.scrip_generic is not null then ot.scrip_generic=%(scrip_generic)s else true end  "
             "and le.range_start=%(range_start)s and le.range_end=%(range_end)s " 
    )
    ##BUG it should be i.symbol, i.exchange, indicator_name, candle_duration not just indicator_name
    ## Update only SL or Open orders (SL Order scheudler will not be Advanced) 
    
    cursor=coredb.cursor()
    
    df = pd.read_sql(sql, coredb) 
    df = df.replace({np.nan: None})
    
    rows=0
    
    
    for index, row in df.iterrows():
        try:
            signal=False
            ##print(row)
            
            indicator_name=keyword_lookup(row['indicator_name'])
            candle_duration=keyword_lookup(row['candle_duration'])
            if candle_duration.isnumeric():
                candle_duration=int(candle_duration)
                
            #futures for index volume    -- needs to fix for new indexes and BSE
            if row['is_volume_indicator']==1 and row['symbol'] in nse_indices:
                
                ## CHANGE this need to change to other expiries like MIDCAP etc.
                if "FIN" in row['symbol']:
                    exp=exp_monthly_fin
                else:
                    exp=exp_monthly_def
                    
                ins=broker.custom_get_instrument_for_fno(primary_scrip_generic, row['nfo_symbol'], exp, True, 0, False)
                
            #direct instrument    
            elif row['instrument_type']==800 and row['scrip_generic']!='NA':
                if row['exchange'] == 'NSE':
                    exch='NFO'
                elif row['exchange'] == 'BSE':
                    exch='BFO'
                    
                ins= broker.get_instrument_by_symbol(primary_scrip_generic,exch,row['scrip_generic'])
                
            #underlying instrument     
            else:
                ins=broker.get_instrument_by_symbol(primary_scrip_generic,row['exchange'],row['symbol'])

            ##ORB
            if indicator_name=='orb_high':
                signal=indicator_obj.get_orb_candle_signal(sdf,ins,candle_duration,"ORB_HIGH")
            elif indicator_name=='orb_low':
                signal=indicator_obj.get_orb_candle_signal(sdf,ins,candle_duration,"ORB_LOW")
                
                
            ##RBO
            elif indicator_name=='rbo_high':
                if row['instrument_type']==800:
                    signal=indicator_obj.get_rbo_signal(sdf, sdf_1min, ins, candle_duration,
                    row['range_start'],row['range_end'],"RBO_HIGH",broker.redis_get_ltp(ins[0],ins[2]))
                else:
                    signal=indicator_obj.get_rbo_signal(sdf, sdf_1min, ins, candle_duration,
                    row['range_start'],row['range_end'],"RBO_HIGH",broker.redis_get_ltp(ins[0],ins[2]))

            elif indicator_name=='rbo_low':
                if row['instrument_type']==800:
                    signal=indicator_obj.get_rbo_signal(sdf, sdf_1min, ins, candle_duration,
                    row['range_start'],row['range_end'],"RBO_LOW",broker.redis_get_ltp(ins[0],ins[2]))
                else:
                    signal=indicator_obj.get_rbo_signal(sdf, sdf_1min, ins, candle_duration,
                    row['range_start'],row['range_end'],"RBO_LOW",broker.redis_get_ltp(ins[0],ins[2]))
                
            ##CONS CANDLES
            elif indicator_name=='two_consecutive_candles':
                signal=indicator_obj.get_last_two_consecutive_candle_signal(sdf,ins,candle_duration)
            
            ##DTR   
            elif indicator_name=='dtr_high':
                signal=candle_obj.get_dtr_signal(sdf,ins,candle_duration,"DTR_HIGH",0.55)
            elif indicator_name=='dtr_high_lower':
                signal=candle_obj.get_dtr_signal(sdf,ins,candle_duration,"DTR_HIGH_LOWER",0.35)
            elif indicator_name=='dtr_low':
                signal=candle_obj.get_dtr_signal(sdf,ins,candle_duration,"DTR_LOW",0.35)
            elif indicator_name=='dtr_low_lower':
                signal=candle_obj.get_dtr_signal(sdf,ins,candle_duration,"DTR_LOW_LOWER",0.55)
                
                
            #WTR   
            elif indicator_name=='wtr_high':
                signal=indicator_obj.get_wtr_signal(sdf,ins,candle_duration,"WTR_HIGH")
            elif indicator_name=='wtr_low':
                signal=indicator_obj.get_wtr_signal(sdf,ins,candle_duration,"WTR_LOW")
           
            
            ##Generic TA
            elif indicator_name=='ta':
                #kwargs = {"length": 21}
                ##print(indicator_obj.get_indicator_signal("sma", sdf, instruments[0], candle_durations[0], **kwargs))
                
                #params=ast.literal_eval(row['params'])
                
                if row['params']=='NA':
                    mainlogger.exception('Params cannot be NA for TA')
                    continue
                    
                signal=indicator_obj.get_indicator_signal(sdf, ins, candle_duration, row['expr'], eval(row['params']))
                
            mainlogger.info('Signal Received: '+indicator_name+' '+str(signal))
            ##print(signal)
            if signal==True:
                
                dic={
                    'indicator_name': row['indicator_name'],
                    'candle_duration': row['candle_duration'],
                    'exchange': row['exchange'],
                    'symbol': row['symbol'],
                    'params': row['params'],
                    'expr': row['expr'],
                    'range_start': row['range_start'],
                    'range_end': row['range_end'] ,
                    'instrument_type': row['instrument_type'],
                    'scrip_generic': row['scrip_generic']
                }
                
                mainlogger.info('Signal Received TRUE: '+str(dic)+' '+str(signal))
                
                try:
                    rows+=cursor.execute(upd_sql,dic)
                except:
                    time.sleep(1)
                    rows+=cursor.execute(upd_sql,dic)
                    
                coredb.commit()
                
                mainlogger.info('Total Technical Indicator Strategies to Execute: '+str(rows))
                
        
        except Exception as e:
            mainlogger.exception(str(e))
            #coredb.close()
            
    coredb.commit()
    cursor.close()
    
    
                
#check_advanced_trades()


# # Smart Exit

# %%

'''
def update_ltp_old():
    mainlogger.info('Function : update_ltp')
    
    coredb=pool.connection()
    cursor=coredb.cursor()

    upd_sql = ("INSERT INTO scripltp (exchange, symbol, ltp, open, high, low, close, open_1m, high_1m, low_1m, close_1m, high_nm, low_nm, candle_start_timestamp) "
               "VALUES(%(exchange)s, %(symbol)s, %(ltp)s, %(open)s, %(high)s, %(low)s, %(close)s, %(open_1m)s, %(high_1m)s, %(low_1m)s, %(close_1m)s, %(high_nm)s, %(low_nm)s, %(candle_start_timestamp)s) "
               "ON DUPLICATE KEY UPDATE ltp= %(ltp)s, open= %(open)s, high= %(high)s, low= %(low)s, close= %(close)s, open_1m=%(open_1m)s, high_1m=%(high_1m)s, low_1m=%(low_1m)s, close_1m=%(close_1m)s, high_nm=%(high_nm)s, low_nm=%(low_nm)s, candle_start_timestamp=%(candle_start_timestamp)s "
              )
    
    tmp=copy.deepcopy(instruments_by_symbol)
    tmp=tmp.items()
    
    
    for i in tmp:
        
        try:
            scrip=i[0]
            if i[1]['ltp']!=None:
                ##print(i)
                
                dic={'exchange':i[1]['segment'], 'symbol':i[0].upper(), 
                     'ltp':i[1]['ltp'], 
                     'open':i[1]['open'], 
                     'high':i[1]['high'], 
                     'low':i[1]['low'], 
                     'close':i[1]['close'],
                     
                     #set all to None
                     'open_1m':None, 
                     'high_1m':None, 
                     'low_1m':None, 
                     'close_1m':None, 
                     'high_nm':None, 
                     'low_nm':None,
                     'candle_start_timestamp':None
                     
                    }
                
                try:
                    # 1min candle for that symbole if found
                    #1min candles
                    dic['open_1m']=sdf_1min[scrip]['minute'][-1:]['open'][0], 
                    dic['high_1m']=sdf_1min[scrip]['minute'][-1:]['high'][0], 
                    dic['low_1m']=sdf_1min[scrip]['minute'][-1:]['low'][0], 
                    dic['close_1m']=sdf_1min[scrip]['minute'][-1:]['close'][0],

                    #last n 1 min candles 
                    dic['high_nm']=max(sdf_1min[scrip]['minute']['high']), 
                    dic['low_nm']=min(sdf_1min[scrip]['minute']['low']), 

                    #candle starttimestamp
                    dic['candle_start_timestamp']=min(sdf_1min[scrip]['minute']['date'])
                        
                except:
                    pass
                    
 
                cursor.execute(upd_sql,dic)
                
                instruments_by_symbol[scrip]['high']=None
                instruments_by_symbol[scrip]['low']=None
                
        except Exception as e:
            coredb.commit()
            mainlogger.exception(str(e))
            #dic={'exchange':i[1]['segment'],'symbol':i[0].upper(), 'ltp':None}
            
        #cursor.execute(upd_sql,dic)
    
    coredb.commit()
    cursor.close()
'''    
def update_ltp():
    mainlogger.info('Function : update_ltp')
    
    coredb=pool.connection()
    cursor=coredb.cursor()

    upd_sql = ("INSERT INTO scripltp_ltp (exchange, symbol, ltp, open, high, low, close) "
               "VALUES(%(exchange)s, %(symbol)s, %(ltp)s, %(open)s, %(high)s, %(low)s, %(close)s) "
               "ON DUPLICATE KEY UPDATE ltp= %(ltp)s, open= %(open)s, high= %(high)s, low= %(low)s, close= %(close)s "
              )
    
    global instruments_by_symbol    
    tmp_instruments_by_symbol=copy.deepcopy(instruments_by_symbol)
    
    tmp=tmp_instruments_by_symbol
    tmp=tmp.items()
    
    for i in tmp: 
        try:
            scrip=i[0]
            if i[1]['ltp']!=None:
                ##print(i)
                
                #exch=i[1]['segment']
                
                dic={'exchange':'NFO', 'symbol':i[0].upper(), 
                     'ltp':i[1]['ltp'], 
                     'open':i[1]['open'], 
                     'high':i[1]['high'], 
                     'low':i[1]['low'], 
                     'close':i[1]['close']
                    }            
 
                cursor.execute(upd_sql,dic)
                
                tmp_instruments_by_symbol[scrip]['high']=None
                tmp_instruments_by_symbol[scrip]['low']=None
                
        except Exception as e:
            coredb.commit()
            mainlogger.exception(str(e))
            #dic={'exchange':i[1]['segment'],'symbol':i[0].upper(), 'ltp':None}
            
        #cursor.execute(upd_sql,dic)
            
    instruments_by_symbol.update(tmp_instruments_by_symbol)
    
    coredb.commit()
    cursor.close()
    
def update_ltp_sdf_1m():
    mainlogger.info('Function : update_ltp_sdf_1m')
    
    coredb=pool.connection()
    cursor=coredb.cursor()

    upd_sql = ("INSERT INTO scripltp_1m (exchange, symbol, open_1m, high_1m, low_1m, close_1m, high_nm, low_nm, candle_start_timestamp) "
               "VALUES(%(exchange)s, %(symbol)s, %(open_1m)s, %(high_1m)s, %(low_1m)s, %(close_1m)s, %(high_nm)s, %(low_nm)s, %(candle_start_timestamp)s) "
               "ON DUPLICATE KEY UPDATE open_1m=%(open_1m)s, high_1m=%(high_1m)s, low_1m=%(low_1m)s, close_1m=%(close_1m)s, high_nm=%(high_nm)s, low_nm=%(low_nm)s, candle_start_timestamp=%(candle_start_timestamp)s "
              )
    
    for i in sdf_1min:
        try:
            scrip=i
            dic={'exchange': 'NFO', 'symbol': scrip.upper(),
                     #set all to None
                     'open_1m':None, 
                     'high_1m':None, 
                     'low_1m':None, 
                     'close_1m':None, 
                     'high_nm':None, 
                     'low_nm':None,
                     'candle_start_timestamp':None               
                    }
            try:
                # 1min candle for that symbole if found
                #1min candles
                dic['open_1m']=sdf_1min[scrip]['minute'][-1:]['open'][0], 
                dic['high_1m']=sdf_1min[scrip]['minute'][-1:]['high'][0], 
                dic['low_1m']=sdf_1min[scrip]['minute'][-1:]['low'][0], 
                dic['close_1m']=sdf_1min[scrip]['minute'][-1:]['close'][0],

                #last n 1 min candles 
                dic['high_nm']=max(sdf_1min[scrip]['minute']['high']), 
                dic['low_nm']=min(sdf_1min[scrip]['minute']['low']), 

                #candle starttimestamp
                dic['candle_start_timestamp']=min(sdf_1min[scrip]['minute']['date'])

            except:
                pass

            cursor.execute(upd_sql,dic)
                
                
        except Exception as e:
            coredb.commit()
            mainlogger.exception(str(e))
            #dic={'exchange':i[1]['segment'],'symbol':i[0].upper(), 'ltp':None}
            
        #cursor.execute(upd_sql,dic)
    
    coredb.commit()
    cursor.close()
    
#update_ltp() 
def smart_exit():
    mainlogger.info('Function : smart_exit')
    #capture latest LTP
    
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()
        
        #create temp table for performance
        tmp_sql=("create or replace temporary table vw_modifyeligibleorders_v3 as select * from vw_modifyeligibleorders_v3 vme where not exists ( select 1 from ordertracker ot where ot.id=vme.OrderTrackerID and `ot`.`sched_order_time` < current_timestamp() + interval 1 minute)")
        tmp_drop_sql=("DROP TEMPORARY TABLE IF EXISTS vw_modifyeligibleorders_v3")
        
        #cancel any future OPEN on this strategy group
        upd_sql_cancel_opens=("update ordertracker ot inner join vw_modifyeligibleorders_v3 vo on ot.order_group_id =vo.order_group_id "
                              "set ot.scheduler_name='Cancelled', comments=CONCAT_WS('|',ot.comments,'Cancelled because of MTM') "
                              "where ot.order_type ='Open' and (ot.scheduler_name='Advanced' or ot.scheduler_name='WNT' or ot.scheduler_name like '202%%') and vo.reason like '%COMBINED%'"
                             )
        ##unset mslc for these order group ids 
        sel_sql=("select distinct vo.order_group_id as order_group_id from ordertracker ot inner join vw_modifyeligibleorders_v3 vo on ot.order_group_id=vo.order_group_id " 
"where ot.order_type in ('Close', 'Squareoff') and ot.scheduler_name='Complete' and ot.active_flag=1 and vo.reason='SL_TO_COST'")

        #unset mslc of already processed SL Hit
        upd_sql_unset_mslc=("update ordertracker ot "
"set ot.slcost_processed=1, comments=CONCAT_WS('|',ot.comments,'Triggered MSLC') "
"where ot.order_group_id in %(order_group_id)s and ot.order_type in ('Close', 'Squareoff') and ot.scheduler_name='Complete' and ot.active_flag=1" )

        upd_sql=("update ordertracker ot inner join vw_modifyeligibleorders_v3 vo on ot.ID = vo.OrderTrackerID   "
"set ot.scheduler_name= case when vo.place_order=1 then 'NOWSE' else ot.scheduler_name end, comments=CONCAT_WS('|',ot.comments,vo.exit_type,vo.reason),   "
"ot.slcost_processed=case when vo.slcost_processed is null then ot.slcost_processed else vo.slcost_processed end,  "
"ot.profit_processed=case when vo.profit_processed is null then ot.profit_processed else vo.profit_processed end,  "
"ot.sl_algo_counter=case when vo.sl_algo_counter is null then ot.sl_algo_counter else vo.sl_algo_counter end,  "
"ot.sl_trail_x=case when vo.sl_trail_x is null then ot.sl_trail_x else vo.sl_trail_x end,   "
"ot.sl_trail_y=case when vo.sl_trail_y is null then ot.sl_trail_y else vo.sl_trail_y end,   "
"ot.sl_trail_combined_x=case when vo.sl_trail_combined_x is null then ot.sl_trail_combined_x else vo.sl_trail_combined_x end,   "
"ot.sl_trail_combined_y=case when vo.sl_trail_combined_y is null then ot.sl_trail_combined_y else vo.sl_trail_combined_y end,  "
"ot.order_type=vo.exit_type   "
"where (ot.scheduler_name not like '%%NOW%%' and ot.scheduler_name not in ('Processing','Complete')) and ot.active_flag=1 ")

        #create temp table first
        cursor.execute(tmp_sql)
        
        #cursor.execute(sql_set_state)
        cursor.execute(upd_sql_cancel_opens)
        coredb.commit()

        #unset mslc OrderGroupIDS before the change
        df = pd.read_sql(sel_sql, coredb)
        df = df.replace({np.nan: None})

        #enable NOWSE flag for all eligible
        rows=cursor.execute(upd_sql)
        coredb.commit()

        #unset mslc of SL Hit which are already processed  - dec 11,22
        if len(df)>0:
            order_group_ids=tuple(df['order_group_id'].to_list())
            dic={'order_group_id':order_group_ids}
            rows1=cursor.execute(upd_sql_unset_mslc,dic)
            coredb.commit()
            mainlogger.info('smart_exit : unset mslc of SL Hit which are already processed '+str(rows1))

        if rows>0:
            ##disable re-entry for MTM scenarios but not for MSLC
            #upd_sql=("update ordertracker ot set reentry_max=0 where scheduler_name = 'NOWSE' and (order_type<>'Modify' or not (order_type in ('Close', 'Squareoff') and comments like '%STRATEGY_EXIT_PARTIAL_ORDERS_WITH_RE%'))")
            upd_sql=("update ordertracker ot  set reentry_max=0, tp_reentry_max=0 where scheduler_name = 'NOWSE'  and ( order_type<>'Modify' and not (order_type in ('Close', 'Squareoff')  and comments like '%STRATEGY_EXIT_PARTIAL_ORDERS_WITH_RE%') and not (order_type in ('Close', 'Squareoff')  and comments like '%sl is hit') and not (order_type in ('Close', 'Squareoff')  and comments like '%INDIVIDUAL_TP_HIT'))")
            
            rows=cursor.execute(upd_sql)
                    
            coredb.commit()
            
            #run_scheduled_jobs_parallel('NOWSE')
        
        cursor.execute(tmp_drop_sql) 
        cursor.close()
        
    except Exception as e:
        mainlogger.exception(str(e))
        cursor.execute(tmp_drop_sql)
        coredb.close()
        
def wnt_eligible_orders():
    mainlogger.info('Function : wnt_eligible_orders')
    #capture latest LTP
    
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()

        upd_sql=("update ordertracker ot inner join vw_wnt_eligible_orders vo on ot.ID = vo.OrderTrackerID "
                 "set ot.scheduler_name='NOWWNT', comments=CONCAT_WS('|',right(ot.comments, 50),vo.reason)"
                 "where (ot.scheduler_name not like '%NOW%' and ot.scheduler_name not in ('Processing','Complete')) and ot.active_flag=1 ")

        rows=cursor.execute(upd_sql)
        coredb.commit()
        cursor.close()
        ##print(rows)
        
    except Exception as e:
        mainlogger.exception(str(e))
        coredb.close()


def cleanup_ordertracker():
    mainlogger.info('Function : cleanup_ordertracker')
    #capture latest LTP
    
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()
        
        #update scriptltp_ltp
        upd_cancel_advanced_strats=("update ordertracker set scheduler_name='Cancelled', comments=CONCAT_WS('|',comments,'Cleanup Job') where order_type ='Open' and scheduler_name ='Advanced'")
        upd_scriptltp_previous_day_close = ("update scripltp_ltp set previous_day_close=ltp")
        
        del_cleanup_sql_ot = ("delete from ordertracker where cast(created_at as date) <= current_date() and scheduler_name in ('Not Today', 'Cleanup')")
        
        upd_sql=("update ordertracker ot inner join vw_cleanup_orderttracker vo on ot.ID = vo.OrderTrackerID "
                 "set last_updated =last_updated, ot.scheduler_name=vo.status , comments=CONCAT_WS('|',ot.comments,'Cleanup Job')")

        del_cleanup_sql_oth = ("delete from ordertracker_history where cast(last_updated as date) <= DATE_SUB(current_date() , INTERVAL 5 DAY)")
        
        del_cleanup_sql_account_history = ("delete from account_history where cast(last_updated as date) <= DATE_SUB(current_date() , INTERVAL 15 DAY)")
        del_cleanup_sql_strategy_history = ("delete from strategy_history where cast(last_updated as date) <= DATE_SUB(current_date() , INTERVAL 15 DAY)")
        del_cleanup_sql_orderleg_history = ("delete from orderleg_history where cast(last_updated as date) <= DATE_SUB(current_date() , INTERVAL 15 DAY)")
        del_cleanup_sql_orderlegsextension_history = ("delete from orderlegsextension_history where cast(last_updated as date) <= DATE_SUB(current_date() , INTERVAL 15 DAY)")
        del_cleanup_sql_billing_history = ("delete from billing_history where cast(last_updated as date) <= DATE_SUB(current_date() , INTERVAL 15 DAY)")
        
        
        #create_orderdetails_export_table=("create or replace table orderdetails_export select * from vw_orderdetails_export")
        
        create_orderdetails_export_table=("INSERT INTO orderdetails_export(Login_ID,firstname,lastname,email,username,broker_name,strategy_name,order_group_id,id,scheduler_name,created_at,last_updated,scrip,direction,filled_qty, "
                                        "limit_price,trigger_price,filled_price,order_id,order_error) "
                                        "select Login_ID,firstname,lastname,email,username,broker_name,strategy_name,order_group_id,id,scheduler_name, "
                                        "created_at,last_updated,scrip,direction,filled_qty,limit_price,trigger_price,filled_price,order_id,order_error "
                                        "from vw_orderdetails_export vo "
                                        "ON DUPLICATE KEY UPDATE  "
                                        "Login_ID=vo.Login_ID,firstname=vo.firstname,lastname=vo.lastname, "
                                        "email=vo.email,username=vo.username,broker_name=vo.broker_name,strategy_name=vo.strategy_name,order_group_id=vo.order_group_id, "
                                        "id=vo.id,scheduler_name=vo.scheduler_name,created_at=vo.created_at,last_updated=vo.last_updated, "
                                        "scrip=vo.scrip,direction=vo.direction,filled_qty=vo.filled_qty,limit_price=vo.limit_price,trigger_price=vo.trigger_price, "
                                        "filled_price=vo.filled_price,order_id=vo.order_id,order_error=vo.order_error ")
        #removed optimization
        #upd_sql_oth=("update ordertracker_history set comments='' where comments <>''")
        
        optimize_sql_ot=("OPTIMIZE TABLE ordertracker")
        
        optimize_sql_oth=("OPTIMIZE TABLE ordertracker_history")
        
        flush_hosts=("FLUSH HOSTS")
        
        disable_users=("update billing b set subscr_type=27 where Login_ID in ( "
                        "select id from vw_active_users_orders_payments where last_30_days_orders is not null and subscr_end<=current_date()- interval 3 day and subscr_type <> 27 )")
        
        rows=cursor.execute(upd_cancel_advanced_strats)
        mainlogger.info('cancel  upd_cancel_advanced_strats')
        coredb.commit()
        
        rows=cursor.execute(upd_scriptltp_previous_day_close) 
        mainlogger.info('updates previous day close ltp '+str(rows))
        coredb.commit()
        
        rows=cursor.execute(del_cleanup_sql_ot) 
        mainlogger.info('Number of rows deleted OT '+str(rows))
        coredb.commit()
        
        rows=cursor.execute(upd_sql)
        mainlogger.info('Number of rows updated OT '+str(rows))
        coredb.commit()
        
        rows=cursor.execute(del_cleanup_sql_oth)
        mainlogger.info('Number of rows deleted OTH '+str(rows))
        coredb.commit()
        
        #rows=cursor.execute(upd_sql_oth)
        #mainlogger.info('Number of rows updated comments in OTH '+str(rows))
        #coredb.commit()
        '''
        rows=cursor.execute(optimize_sql_ot)
        mainlogger.info('Optimized OT '+str(rows))
        coredb.commit()
        
        rows=cursor.execute(optimize_sql_oth)
        mainlogger.info('Optimized OTH '+str(rows))
        coredb.commit()
        '''
        rows=cursor.execute(del_cleanup_sql_account_history)
        mainlogger.info('Number of rows updated comments in del_cleanup_sql_account_history '+str(rows))
        coredb.commit()
        
        rows=cursor.execute(del_cleanup_sql_strategy_history)
        mainlogger.info('Number of rows updated comments in del_cleanup_sql_strategy_history '+str(rows))
        coredb.commit()
        
        rows=cursor.execute(del_cleanup_sql_orderleg_history)
        mainlogger.info('Number of rows updated comments in del_cleanup_sql_orderleg_history '+str(rows))
        coredb.commit()
        
        rows=cursor.execute(del_cleanup_sql_orderlegsextension_history)
        mainlogger.info('Number of rows updated comments in del_cleanup_sql_orderlegsextension_history '+str(rows))
        coredb.commit()
        
        rows=cursor.execute(del_cleanup_sql_billing_history)
        mainlogger.info('Number of rows updated comments in del_cleanup_sql_billing_history '+str(rows))
        coredb.commit()
        
        rows=cursor.execute(flush_hosts)
        mainlogger.info('flush hosts')
        coredb.commit()
        
        rows=cursor.execute(disable_users)
        mainlogger.info('disabled users who did not make payment')
        coredb.commit()
        
        rows=cursor.execute(create_orderdetails_export_table)
        mainlogger.info('created  create_orderdetails_export_table')
        coredb.commit()
        
        cursor.close()
        
    except Exception as e:
        mainlogger.exception(str(e))
        coredb.close()
        
def execute_now_jobs():
    mainlogger.info('Function : execute_now_jobs')
    #capture latest LTP
    
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()

        select_sql1=("select 1 from ordertracker ot where scheduler_name ='EXITNOWUI' and cast(ot.last_updated as date)>=current_date() - interval 1 day")
        rows1=cursor.execute(select_sql1)
        
        select_sql2=("select 1 from ordertracker ot where scheduler_name like '%%NOW%%' and cast(ot.last_updated as date)>=current_date() - interval 1 day")
        rows2=cursor.execute(select_sql2)
        
        if rows1>0 or rows2>0:
            
            #EXITNOWUI special case
            if rows1>0:
                upd_sql=("update ordertracker ot set reentry_max=0,tp_reentry_max=0 where scheduler_name = 'EXITNOWUI'") ##disable re-entry for UI squreoff scenarios
                rows=cursor.execute(upd_sql)
                
            coredb.commit()
            cursor.close()
            #threading.Thread(target=run_scheduled_jobs_parallel,args=('ALLNOW',),daemon=True).start()
            run_scheduled_jobs_parallel('ALLNOW');
                
    except Exception as e:
        mainlogger.exception(str(e))
        coredb.close()
        
def slbreach_check():
    mainlogger.info('Function : slbreach_check')
    #MUTEX
    #capture latest LTP
    #If Limit price less than LTP for 6 times in a row every 30s == 180s or 3 mins. So it's considered as breach.
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()

        upd_sql1=("update ordertracker ot inner join vw_sl_breached sl on ot.ID = sl.OrderTracker_id "
                 "set ot.slbreach_counter=sl.slbreach_counter "
                 "where ot.slbreach_counter<>sl.slbreach_counter")
        
        upd_sql2=("update ordertracker ot inner join vw_sl_breached sl on ot.ID = sl.OrderTracker_id "
                  "set ot.scheduler_name='NOW', ot.comments=CONCAT_WS('|',ot.comments,'fun:slbreach_check|sl is hit') "
                  "where ot.slbreach_counter>=90") #1s * 90 = 90+s

        cursor.execute(upd_sql1)
        rows=cursor.execute(upd_sql2)
        
        coredb.commit()
        cursor.close()
            
    except Exception as e:
        mainlogger.exception(str(e))
        coredb.close()
         
        
def sl_check():
    mainlogger.info('Function: sl_check to set Complete and filled price')
    #MUTEX
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()
        
        sel_sql=("select 1 from vw_sl_check where slcheck_flag>0")
        
        rows1=cursor.execute(sel_sql)
        
        if rows1>0:
            upd_sql1=("update ordertracker ot inner join vw_sl_check sl on ot.ID = sl.OrderTracker_id "
                      "set scheduler_name='NOW', order_type='Update', filled_price=sl.tmp_filled_price, filled_qty=sl.tmp_filled_qty, comments =CONCAT_WS('|',right(ot.comments,50),'SL Check') "
                      "where sl.slcheck_flag>0")

            rows=cursor.execute(upd_sql1)
            coredb.commit()
            
        cursor.close()
        
    except Exception as e:
        mainlogger.exception(str(e))
        coredb.close()

#smart_exit()

def reentry_eligible_orders():
    mainlogger.info('Function : reentry_eligible_orders')
    
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()

        ins_sql_sl=("INSERT INTO ordertracker (Account_ID,AccountGroup_ID,StrategyGroup_ID,Strategy_ID,OrderLegs_ID,order_group_id,reentry_max,tp_reentry_max,order_type, direction,sched_order_time,scheduler_name,scrip,scrip_generic,order_id,comments,wnt_strategy_end_time)  " 
                 " select * from vw_reentry_eligible_orders")
        
        ins_sql_tp=("INSERT INTO ordertracker (Account_ID,AccountGroup_ID,StrategyGroup_ID,Strategy_ID,OrderLegs_ID,order_group_id,reentry_max,tp_reentry_max,order_type, direction,sched_order_time,scheduler_name,scrip,scrip_generic,order_id,comments,wnt_strategy_end_time)  " 
                 " select * from vw_tp_reentry_eligible_orders")

        rows=cursor.execute(ins_sql_sl)
        
        coredb.commit()
        
        rows=cursor.execute(ins_sql_tp)
        
        coredb.commit()
        
        cursor.close()
            
    except Exception as e:
        mainlogger.exception(str(e))
        coredb.close()

        
def update_ordergroup_status():
    mainlogger.info('Function : update_ordergroup_status')
    
    try:
        coredb = pool.connection()
        cursor=coredb.cursor()
        
        ins_upd_sql=("INSERT INTO ordergroupstatus(Account_ID, username, brokerName, Strategy_ID, strategyName, order_group_id, " "order_group_last_updated, status, pnl, show_check_box) "
"select vo.Account_ID, vo.username, vo.brokerName, vo.Strategy_ID, vo.strategyName, vo.order_group_id, vo.last_updated, vo.status, vo.pnl, vo.show_check_box "
"from vw_ordergroup_status vo " 
"ON DUPLICATE KEY UPDATE "
"Account_ID=vo.Account_ID, username=vo.username,  brokerName=vo.brokerName, Strategy_ID=vo.Strategy_ID, "
"strategyName=vo.strategyName, order_group_id=vo.order_group_id, order_group_last_updated=vo.last_updated, "
"status=vo.status, show_check_box=vo.show_check_box"
)
        upd_sql=("update ordergroupstatus os join vw_livemtm vl on os.Account_ID =vl.Account_ID and os.order_group_id =vl.order_group_id " 
"set os.AccountGroup_ID=vl.AccountGroup_ID, os.StrategyGroup_ID=vl.StrategyGroup_ID, os.pnl=vl.pnl, os.last_pnl= case when os.status not in (select orderstatus from orderstatus where is_final_status=1) then vl.pnl else last_pnl end"
)
        rows=cursor.execute(ins_upd_sql)
        coredb.commit()
        rows=cursor.execute(upd_sql)
        coredb.commit()
        cursor.close()

    except Exception as e:
        mainlogger.exception(str(e))
        coredb.close()
        
#smart_exit()

#reentry_eligible_orders()

#cleanup_ordertracker()

#update_ordergroup_status()


# # PNL Report and Notify Profits

# %%


def upload_orderbook_to_s3(acc_id,orderbook):
    mainlogger.info('Function : upload_orderbook_to_s3')
    try:
        #s3 = boto3.resource('s3', aws_access_key_id='aws_key', aws_secret_access_key='aws_sec_key')
        if len(orderbook)==0:
            orderbook={'msg':'empty orderbook'};
                
        s3 = boto3_session.resource('s3')
        dt=datetime.datetime.today().strftime('%Y%m%d')
        s3object = s3.Object('ksl-obks', str(acc_id)+'/'+dt+'.json')
        s3object.put(
            Body=(bytes(json.dumps(orderbook, indent=4, sort_keys=True, default=str).encode('UTF-8')))
        )
        
        return 1
 
    except Exception as e:
        mainlogger.exception('failed: upload_orderbook_to_s3 '+str(e))

def update_ordertracker_stats():
    mainlogger.info('Function : update_ordertracker_stats')
    
    coredb=pool.connection()
    cursor=coredb.cursor()
    
    upd_sql = ("UPDATE ordertracker set last_updated=last_updated,filled_price=%(filled_price)s, order_time=%(order_time)s, order_status=%(order_status)s, order_category=%(order_category)s, filled_qty=%(filled_qty)s, remarks=%(remarks)s, scheduler_name=%(scheduler_name)s "
           "where Account_ID=%(Account_ID)s and order_id=%(order_id)s and scheduler_name not in ('NOWRE')"
          )  ## incase of RE order_id is sl_order_id so needs to be ignored
    
    for a in dic_accounts.keys():
        try:
            broker.check_login_token(a)
            orders=broker.get_all_order_history(a)
            
            if orders is None or len(orders)==0:
                mainlogger.info("Empty Orderbook for "+ str(a))
                continue;
                
            #upload_orderbook_to_s3 on daily basis
            upload_orderbook_to_s3(a, orders)
            
            #orders=orders['data']['completed_orders']
            
            for order in orders:
                #order not placed by algo
                 
                dic={}
                if (order['order_status'].lower()=='complete'):
                    
                    dic['filled_price']=order['average_price']
                    
                    dic['order_time']=datetime.datetime.fromtimestamp(order['exchange_time'])
                    
                    dic['order_status']=order['order_status']
                    dic['order_category']=order['order_type']
                    dic['filled_qty']=order['filled_quantity']
                    dic['remarks']=order['rejection_reason']
                    dic['order_id']=order['oms_order_id']
                    dic['Account_ID']=a
                    dic['scheduler_name']='Complete'
                    cursor.execute(upd_sql,dic)
                
                coredb.commit()
                
        except Exception as e:
            send_notification(group_chat_id, emoji_warning+'Exception in order tracker update for account id '+str(a))
            mainlogger.exception(str(a)+ ' ' + str(e))
            
    ##UPDATE ORDERGROUP STATUS SUMMARY PNL

    #update_ordergroup_pnl()

    return 1
#update_ordertracker_stats()


# %%

## This function is NOT USED
'''
def upload_pnl():
    mainlogger.info('Function : upload_PNL')
    
    coredb=pool.connection()
    
    sql=("select * from vw_dailypnlsummary")
    
    df = pd.read_sql(sql, coredb) 
    df.fillna('', inplace=True)
    df = df.replace({np.nan: None})
    
    # The ID and range of a sample spreadsheet.
    SPREADSHEET_ID = gsheetId #'162kEtwRNSEn3jLvesaVRG2jBleYPeQnfhIt0c5FuBMY' #gsheetId
    #SPREADSHEET_ID='162kEtwRNSEn3jLvesaVRG2jBleYPeQnfhIt0c5FuBMY' #prod

    gc = gspread.service_account(filename="service_account.json")
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.get_worksheet(3)

    
    # OVERWRITE DATA TO SHEET
    #ws.clear()
    #set_with_dataframe(worksheet=ws,dataframe=df,include_index=False,include_column_header=True,resize=True)

    # APPEND DATA TO SHEET
    data_list = df.values.tolist()
    ws.append_rows(data_list,value_input_option='USER_ENTERED')        
''' 
#upload_pnl()


# %%


def update_ordergroup_pnl():
    mainlogger.info('Function : update_ordergroup_pnl')
    
    try:
        coredb=pool.connection()
        cursor=coredb.cursor()
        upd_sql2 = ('set sql_mode=""')
        upd_sql3 = ("update ordergroupstatus os inner join  vw_dailymtm mtm on (os.order_group_id)=mtm.order_group_id "
                   "set os.pnl=mtm.pnl "
                   "where mtm.order_group_id <> '999'")
        
        #upd_sql3 = ("update core_dev.ordergroupstatus set last_updated=last_updated")
        upd_sql4 = ("set sql_mode='STRICT_TRANS_TABLES'")

        cursor.execute(upd_sql2)
        updated_rows= cursor.execute(upd_sql3)
        mainlogger.info('Function : update_ordergroup_pnl updated_rows '+str(updated_rows))
        cursor.execute(upd_sql4)

        coredb.commit()
        cursor.close()
        
    except Exception as e:
        mainlogger.exception(str(e))
        
def notify_profits():
    mainlogger.info('Function : notify_profits')
    
    coredb = pool.connection()
    sql=("select * from vw_dailymtm where pnl is not null")
     
    df = pd.read_sql(sql, coredb) 
    df = df.replace({np.nan: None})
    
    for index, row in df.iterrows():
        try: 
            ##print(row)
            if row['pnl']>=0:
                send_notification(row['tele_chat_id'],emoji_dollar+'**MTM PnL Summary** '+emoji_dollar+'\nStrategy: '+ row['name']+'\n'+emoji_profit+'PnL: '+str(row['pnl'])+'\n\nNote: Accurate if all orders executed by Algo.')
            else:
                send_notification(row['tele_chat_id'],emoji_dollar+'**MTM PnL Summary** '+emoji_dollar+'\nStrategy: '+ row['name']+'\n'+emoji_loss+'PnL: '+str(row['pnl'])+'\n\nNote: Accurate if all orders executed by Algo.')

        except Exception as e:
            mainlogger.exception(str(e))
            
    #update_ordergroup_pnl()
#notify_profits()


# %%


def notify_tradingaccount_pnl():
    mainlogger.info('Function : notify_tradingaccount_pnl')
    
    coredb=pool.connection()
    cursor=coredb.cursor()

    ins_sql = ("INSERT into account_pnl_summary(account_id, totalRealizedPNL, totalUnRealizedPNL, maxrealizedPoriftPosition, maxrealizedLossPosition, maxUnrealizedProfitPosition, maxUnrealizedLossPosition) "
               " VALUES (%(account_id)s, %(totalRealizedPNL)s, %(totalUnRealizedPNL)s, %(maxrealizedPoriftPosition)s, %(maxrealizedLossPosition)s, %(maxUnrealizedProfitPosition)s, %(maxUnrealizedLossPosition)s)")
    
    for a in dic_accounts.keys():
        try:
            broker.check_login_token(a)
            pnlSummary=broker.tradingAccount_PNLSummary(a)
            
            if pnlSummary==-1:
                continue
                
            pnlSummary['account_id']=a
            
            cursor.execute(ins_sql,pnlSummary)
            coredb.commit()
            
        except Exception as e:
            mainlogger.exception(str(e))
    
    
    
    sql=("select va.tele_chat_id ,  va.username, p.* from account_pnl_summary p, vw_activeaccounts va where va.ID=p.account_id and va.pnl_subscribe=1 and cast(p.last_updated as date)=current_date()")
     
    df = pd.read_sql(sql, coredb) 
    df = df.replace({np.nan: None})
    
    for index, row in df.iterrows():
        
        message=emoji_dollar+'**Trading Account Summary** Account: '+ row['username']+emoji_dollar+'\n'
        
        try: 
            ##print(row)
            
            if row['totalUnRealizedPNL']<=0:
                message=message+'\n'+emoji_loss+'Total Unrealized PNL: '+str(row['totalUnRealizedPNL'])
                
            message=message+'\n'+emoji_loss+'Max Unrealized Loss Position: '+str(row['maxUnrealizedLossPosition'])
            
            send_notification(row['tele_chat_id'],message)

        except Exception as e:
            mainlogger.exception(str(e))
    
    
#notify_tradingaccount_pnl()


# %%


def reset_account_pnl_threshold():
    mainlogger.info('Function : reset_account_pnl_threshold')
    
    coredb = pool.connection()
    cursor=coredb.cursor()
    
    upd_sql = ("UPDATE account_pnl set pnl_loss_check_count=0, pnl_profit_check_count=0, last_updated=last_updated")
    
    cursor.execute(upd_sql)
    coredb.commit()
    


# # Handle After Market Stuff

# %%
def update_sl_scheduler_name():
    mainlogger.info('Function : update_sl_scheduler_name for algosl scenarios')
    
    coredb = pool.connection()
    cursor=coredb.cursor()
    
    upd_sql=("update ordertracker set sl_scheduler_name=DATE_FORMAT(STR_TO_DATE(sl_scheduler_name,'%%Y%%m%%d%%H%%i')+ interval %(delta_next_trading_day)s day,'%%Y%%m%%d%%H%%i') " 
         "where scheduler_name like '202%%' and sl_scheduler_name like '202%%' and order_type='Squareoff' and sl_scheduler_name is not null "
         "and cast(STR_TO_DATE(sl_scheduler_name,'%%Y%%m%%d%%H%%i') as date)=current_date()")
    
    dic={'delta_next_trading_day':global_delta_next_trading_day}
    
    cursor.execute(upd_sql,dic)
    coredb.commit()


# %%


        
def after_market():
    mainlogger.info('Function : after_market')
    send_notification(group_chat_id, 'after_market:started')
    
    coredb = pool.connection()
    cursor=coredb.cursor()
    
    dt=datetime.datetime.now(ist).date()
    
    sql=("select jobname from jobtracker jt where (cast( jt.lastRun as date)<cast(current_timestamp() as date))")
    df = pd.read_sql(sql, coredb) 
    df = df.replace({np.nan: None})
    
    upd_sql = ("UPDATE jobtracker set lastrun= %(lastrun)s "
           "where jobname=%(jobname)s" 
              )

    for index,row in df.iterrows():
        try:

            if row['jobname']=='place_amo_orders':
                place_amo_orders()
                send_notification(group_chat_id, '1. after_market:place_amo_orders done')
                #print('place_amo_orders')
                
            elif row['jobname']=='update_sl_scheduler_name':
                update_sl_scheduler_name()
                send_notification(group_chat_id, '2. after_market:update_sl_scheduler_name done')
                #print('update_sl_scheduler_name')
                
            elif row['jobname']=='update_ordertracker_stats':
                update_ordertracker_stats()
                update_ordergroup_status() #final ordergroup status update
                update_ordergroup_pnl() #final pnl update of ordergroup
                reset_account_pnl_threshold() #reset_account_pnl_threshold
                
                send_notification(group_chat_id, '3. after_market:update_ordertracker_stats done')
                #print('update_ordertracker_stats')
                    
            elif row['jobname']=='notify_profits':
                notify_profits()
                time.sleep(30)
                send_notification(group_chat_id, '4. after_market:notify_profits done')
                #print('notify_profits')

            elif row['jobname']=='upload_pnl':
                #upload_pnl() - we are not using Google Sheet anymore
                print('upload_pnl')
                
            elif row['jobname']=='update_options_strike_step':
                #update_options_strike_step()
                print('update_options_strike_step')   
                
            elif row['jobname']=='notify_tradingaccount_pnl':
                notify_tradingaccount_pnl()
                send_notification(group_chat_id, '5. after_market:notify_tradingaccount_pnl done')
                #print('notify_tradingaccount_pnl')
            
            elif row['jobname']=='cleanup_ordertracker':
                cleanup_ordertracker()
                update_ordergroup_status() #final ordergroup status update
                update_ordergroup_pnl() #final pnl update of ordergroup
                #print('cleanup_ordertracker')
                send_notification(group_chat_id, '6. after_market:cleanup_ordertracker done')
                
                load_to_oracle()
                send_notification(group_chat_id, '7. after_market:load_to_oracle done')
                #print('load OT to oracle')
            
            elif row['jobname']=='cleanup_timescaledb':
                pass
                #cleanup_timescaledb()
                #send_notification(group_chat_id, '8. after_market:cleanup_timescaledb done')
                
            dic={'lastrun':dt, 'jobname':row['jobname']}
            cursor.execute(upd_sql,dic)
            coredb.commit()
        
        except Exception as e:
            send_notification(group_chat_id,'After Market Task Failed:'+str(row['jobname'])+' '+str(e))
            mainlogger.exception(str(e))


# ## OPTIONS STRIKE PRICE STEP INCREMENT

# %%
def load_to_oracle():
    mainlogger.info('Function : load_to_oracle')
    
    try:
        config = cp.ConfigParser()
        config.read(env_config_filename)
        
        mysql_tablename     = config.get("env", "mysql_report_table")
        oracle_tablename    = config.get("env", "oracle_report_table")
        csv_filename        = config.get("env", "report_temp_file")
        
        #mysql_tablename     = 'pnl_reporting_raw'
        #oracle_tablename    = 'pnl_reporting_raw_prod'
        #csv_filename        = './pnl_reporting_raw_prod.csv'
        env_filename        = env_config_filename
        oracle_record_count = 0
        mysql_record_count  = 0
        import_data         = ImportDataToOracle()
        import_data.get_csv_data_from_mysql(mysql_tablename, csv_filename, env_filename)
        oracle_record_count = import_data.get_table_count_from_oracle(oracle_tablename, env_filename)
        import_data.truncate_table_from_oracle(oracle_tablename, env_filename)
        oracle_record_count = import_data.get_table_count_from_oracle(oracle_tablename, env_filename)
        import_data.import_csv_to_oracle(oracle_tablename, csv_filename, 5000, env_filename)
        oracle_record_count = import_data.get_table_count_from_oracle(oracle_tablename, env_filename)
        mysql_record_count  = import_data.get_record_count_from_csv(csv_filename)
        
        if mysql_record_count != 0 and mysql_record_count == oracle_record_count:
            mainlogger.info("Migration successful",mysql_record_count,oracle_record_count,)
            import_data.delete_csv_file(csv_filename)
        else:
            mainlogger.info("Migration failed")
            import_data.delete_csv_file(csv_filename)
            raise
                            
    except Exception as e:
            mainlogger.exception('Loading failed '+str(e))
            raise

def update_options_strike_step():
    mainlogger.info('Function : update_options_strike_step')
    
    coredb = pool.connection()
    cursor=coredb.cursor()
    
    dt=datetime.datetime.now(ist).date()
    
    sql=("select id, symbol from instrument i where instrument_type ='OPTION'")
    df = pd.read_sql(sql, coredb) 
    df = df.replace({np.nan: None})
    
    upd_sql = ("UPDATE instrument set round_figure= %(round_figure)s "
           "where id=%(id)s" 
              )
    
    for index,row in df.iterrows():
        try:
            round_figure=int(datafeed().get_options_strike_step(row['symbol'].lower(),datetime.datetime.strptime(exp_monthly_def, '%d %b%y').date().strftime('%d %b %Y')))
            dic={'round_figure':round_figure, 'id':row['id']}
            cursor.execute(upd_sql,dic)
            coredb.commit()
            
            mainlogger.info('Updated round_figure value for '+row['symbol']+str(round_figure))
            
        except Exception as e:
            mainlogger.exception('Failed for '+row['symbol']+str(e))
            
            


# %%


def unlock_db():
    mainlogger.info('Function : unlock_db')
    
    coredb = pool.connection()
    cursor=coredb.cursor()
    upd_sql=("update dblock set locked=0")
    rows=cursor.execute(upd_sql)
    coredb.commit()
    mainlogger.info('Algo shutting down so DB is unlocked')
    coredb.close()
    
#unlock_db()


# # Global count of orders per leg

# %%


def disable_erroneous_startegies():
    #If there are more than 10 entries per orderleg in OT table. Cancel All orders in OT and Disable strategy.
    mainlogger.info('Function : disable_erroneous_startegies')
    
    try:
        sel_sql=("select * from vw_daily_total_orders_per_leg")

        upd_sql=("update ordertracker ot, strategy s "
                 "set ot.active_flag=0, ot.scheduler_name ='Cancelled', ot.comments=CONCAT_WS('|',ot.comments,'Global count of orders per leg > 10'), "
                 "s.active_flag=0 "
                 "where cast(ot.created_at as date)= current_date() and ot.Strategy_ID in (select Strategy_ID from vw_daily_total_orders_per_leg) "
                 "and ot.Strategy_ID=s.id")

        coredb = pool.connection()

        df = pd.read_sql(sel_sql, coredb)
        df = df.replace({np.nan: None})

        if len(df)>0:
            cursor=coredb.cursor()
            rows=cursor.execute(upd_sql)
            coredb.commit()

            for index,row in df.iterrows():
                handle_exception('Error is suspected please contact us. System Disabled strategy '+row['strategy_name'],'System Disabled Strategy',row['tele_chat_id'],True) 

        coredb.close()
        
    except Exception as e:
        mainlogger.exception(str(e))
    
#disable_erroneous_startegies()


# # Account Level MTM

# %%


def mtm_account_alert():
    #Account level MTM threshold breach.
    mainlogger.info('Function : mtm_account_alert')
    
    sel_sql=("select * from vw_mtm_account_threshold_breach")
    
    upd_sql=("update account_pnl a join vw_mtm_account_threshold_breach va on a.Account_ID =va.Account_ID set "
         "a.pnl_profit_check_count=case when va.total_account_pnl>0 then a.pnl_profit_check_count+1 else 0 end , "
         "a.pnl_loss_check_count=case when va.total_account_pnl<0 then a.pnl_loss_check_count+1 else 0 end, "
         "a.pnl_profit_sent=case when a.pnl_profit_check_count>6 and va.total_account_pnl>0 then current_timestamp() else a.pnl_profit_sent end , "
         "a.pnl_loss_sent=case when a.pnl_loss_check_count>6 and va.total_account_pnl<0 then current_timestamp() else a.pnl_loss_sent end, "
         "a.last_updated=a.last_updated ")   
    
    try:
        coredb = pool.connection()

        df = pd.read_sql(sel_sql, coredb)
        df = df.replace({np.nan: None})

        if len(df)>0:

            cursor=coredb.cursor()
            cursor.execute(upd_sql)
            coredb.commit()

            for index,row in df.iterrows():
                if row['total_account_pnl']>0 and row['pnl_profit_check_count']>6:
                    send_notification(row['tele_chat_id'],emoji_dollar+'**Account Hit PnL Threshold** '+emoji_dollar+'\nAccount: '+ row['username']+'\n'+emoji_profit+'Current PnL: '+str(row['total_account_pnl'])+'\n\nNote: Accurate if all orders executed by Algo.')
                elif row['total_account_pnl']<0 and row['pnl_loss_check_count']>6:
                    send_notification(row['tele_chat_id'],emoji_dollar+'**Account Hit PnL Threshold** '+emoji_dollar+'\nAccount: '+ row['username']+'\n'+emoji_loss+'Current PnL: '+str(row['total_account_pnl'])+'\n\nNote: Accurate if all orders executed by Algo.')
                #handle_exception('Account '+str(row['username'])+' has hit a PNL threshold of Rs. '+str(row['total_account_pnl']),'System Disabled Strategy',row['tele_chat_id'],True) 


        coredb.close()
        
    except Exception as e:
        mainlogger.exception(str(e))
            
#mtm_account_alert()


# # Create Workers for Background Jobs

# %%
if __name__ == "__main__":

    #Turn-on the worker thread for start DB tasks.
    t=threading.Thread(target=worker_db_update, daemon=True)
    t.start()
    threadlist.append({'object':t,'function':'worker_db_update'})

        
#Turn-on the worker thread for SL Checks

def worker_sl_check():
    mainlogger.info('wroker started: worker_sl_check')
    while True:
        try:
            dt=datetime.datetime.now(ist)
            if (dt>ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 9,15,0,0)) and 
                dt<ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 15,29,0,0))):
                
                sl_check()
        
        except Exception as e:
            mainlogger.exception(str(e))
            
        time.sleep(1)

if __name__ == "__main__":
    t=threading.Thread(target=worker_sl_check, daemon=True)
    t.start()
    threadlist.append({'object':t,'function':'worker_sl_check'})

#Turn-on the worker thread for check_polling_strategies

def worker_check_polling_strategies():
    mainlogger.info('wroker started: worker_check_polling_strategies')
    while True:
        try:
            dt=datetime.datetime.now(ist)
            if (dt>ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 9,15,0,0)) and 
                dt<ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 15,29,0,0))):
                
                check_polling_strategies()
        
        except Exception as e:
            mainlogger.exception(str(e))
            
        time.sleep(60)

if __name__ == "__main__":
    t=threading.Thread(target=worker_check_polling_strategies, daemon=True)
    t.start()
    threadlist.append({'object':t,'function':'worker_check_polling_strategies'})


    
#Turn-on the worker thread for Smart Exit

def worker_smart_exit():
    mainlogger.info('wroker started: worker_smart_exit')
    
    while True:
        try:
            dt=datetime.datetime.now(ist)
    
            if (dt>ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 9,15,0,0)) and
                dt<ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 15,29,0,0))):
            
                smart_exit() 
                
        except Exception as e:
            mainlogger.exception(str(e))
            
        time.sleep(1)
        
if __name__ == "__main__":
    t=threading.Thread(target=worker_smart_exit, daemon=True)
    t.start()
    threadlist.append({'object':t,'function':'worker_smart_exit'})


#Turn-on the worker thread for WNT

def worker_wnt_eligible_orders():
    mainlogger.info('wroker started: worker_wnt_eligible_orders')
    while True:
        try:
            dt=datetime.datetime.now(ist)
    
            if (dt>ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 9,15,0,0)) and
                dt<ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 15,29,0,0))):
            
                wnt_eligible_orders() 
                
        except Exception as e:
            mainlogger.exception(str(e))
            
        time.sleep(5)
        
if __name__ == "__main__":
    t=threading.Thread(target=worker_wnt_eligible_orders, daemon=True)
    t.start()
    threadlist.append({'object':t,'function':'worker_wnt_eligible_orders'})

#Turn-on the worker thread for RE and REX

def worker_reentry_eligible_orders():
    mainlogger.info('wroker started: worker_reentry_eligible_orders')
    while True:
        try:
            dt=datetime.datetime.now(ist)
    
            if (dt>ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 9,15,0,0)) and
                dt<ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 15,29,0,0))):

                reentry_eligible_orders() 
                
        except Exception as e:
            mainlogger.exception(str(e))
            
        time.sleep(5)

if __name__ == "__main__":
    t=threading.Thread(target=worker_reentry_eligible_orders, daemon=True)
    t.start()
    threadlist.append({'object':t,'function':'worker_reentry_eligible_orders'})

##This process will sync variables from main to worker process. Runs as a thread inside a worker process.
def worker_sync_variables():
    mainlogger.info('wroker started: worker_sync_variables')
    global global_instruments_1min
    
    while True:
        try:
            while not q_var_global_instruments_1min.empty():
                try:
                    ins=q_var_global_instruments_1min.get()
                    global_instruments_1min.append(ins)
                except Exception as e:
                    mainlogger.exception('Q worker_sync_variables failed '+ str(e))

            time.sleep(10)
            
        except Exception as e:
            mainlogger.exception('worker_sync_variables failed '+ str(e))
            send_notification(group_chat_id,'worker_sync_variables failed - will be retried')
            time.sleep(60)
            
def worker_refresh_advanced_instruments():
    mainlogger.info('wroker started: worker_refresh_advanced_instruments')
    
    coredb=pool.connection()
 
    while True:
        try:
            refresh_advanced_instruments(coredb)
            time.sleep(1)       
        except Exception as e:
            mainlogger.exception('worker_refresh_advanced_instruments failed '+ str(e))
            send_notification(group_chat_id,'worker_refresh_advanced_instruments failed - will be retried')
            time.sleep(10)
            
def process_worker_check_advanced_trades():
    mainlogger.info('wroker process started: process_worker_check_advanced_trades')
    
    
    #create a new DB pool for chil process
    global pool
    
    pool = PooledDB(
    creator=pymysql,  #Modules that use linked databases
    maxconnections=3,  #The maximum number of connections allowed in the connection pool, 0 and None means no limit on the number of connections
    mincached=2,  #At least one free link created in the link pool during initialization, 0 means not created
    maxcached=2,  #The most idle link in the link pool, 0 and None are not restricted
    maxshared=0,  #The maximum number of links shared in the link pool, 0 and None indicate all shares. PS: Useless, because the threadsafety of modules such as pymysql and MySQLdb is 1. If all values are set, _maxcached will always be 0, so all links will always be shared. 
    blocking=True,#Whether to block waiting if there is no connection available in the connection pool. True, wait; False, don't wait and then report error
    maxusage=None, #The number of times a link is reused at most, None means unlimited
    setsession=["SET collation_connection = 'latin1_swedish_ci'",
                "SET tx_isolation = 'READ-COMMITTED'"],  #A list of commands to execute before starting a session. Such as: ["set datestyle to...", "set time zone..."]
    ping=0,
    host=dbhost,
    port=dbport,
    user=dbuser,
    password=dbpass,
    database=dbname
    )    
    
    #Turn-on the worker thread to sync variables from Queue to Sub Process
    t=threading.Thread(target=worker_sync_variables, daemon=True)
    t.start()
    
    #Turn-on the worker thread to update OHLC data in the background
    t=threading.Thread(target=worker_refresh_advanced_instruments, daemon=True)
    t.start()
    #threadlist.append({'object':t,'function':'worker_sync_variables'})
    
    while True:
        
        dt=datetime.datetime.now(ist)

        if (dt>ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 9,15,0,0)) and
            dt<ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 15,28,0,0))):
            
            try:
                check_advanced_trades()
            except Exception as e:
                mainlogger.exception(str(e))
                
            try:
                update_ltp_sdf_1m()
            except Exception as e:
                mainlogger.exception(str(e))

        time.sleep(1)
        
        #mainlogger.info('wroker process variable global_instruments_1min '+str(global_instruments_1min)) 

if __name__ == "__main__":
    p=multiprocessing.Process(target=process_worker_check_advanced_trades)
    p.start()
    threadlist.append({'object':p,'function':'process_worker_check_advanced_trades'})

#Turn-on the worker thread for execute_now_jobs
def worker_execute_now_jobs():
    mainlogger.info('wroker started: worker_execute_now_jobs')
    while True:
        try:
            dt=datetime.datetime.now(ist)
    
            if (dt>ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 9,15,5,0)) and
                dt<ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 15,29,30,0))
                #dt<ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 23,59,30,0))
               ):
            
                execute_now_jobs()
                
        except Exception as e:
            mainlogger.exception(str(e))
            
        time.sleep(2)

if __name__ == "__main__":
    t=threading.Thread(target=worker_execute_now_jobs, daemon=True)
    t.start()
    threadlist.append({'object':t,'function':'worker_execute_now_jobs'})
    
    t=threading.Thread(target=order_processors_start_workers, daemon=True)
    t.start()
    threadlist.append({'object':t,'function':'order_processors_start_workers'})


# # Main Loop

# %%

if __name__ == "__main__":
    executors = {
        'default': ThreadPoolExecutor(100),   # max threads: 90
        'processpool': ProcessPoolExecutor(20)  # max processes 20
    }

    scheduler = BackgroundScheduler(timezone="Asia/Kolkata", job_defaults={'misfire_grace_time': 5*60}, coalesce=True, executors=executors)
    scheduler.start()



    mainlogger.info('RUNNING IN '+mode+' MODE')


# %%

def initiate_shutdown_sequence():
    mainlogger.info('Function: initiate_shutdown_sequence')
    ##Unlock the DB 
    unlock_db()

    mainlogger.info('Algo: Shutting down..')
    send_notification(group_chat_id,'Algo Shutting down')
    #handle_exception('Algo: Shutting down ','NA','NA',False)

    time.sleep(120)

    mainlogger.info('Algo: Good bye!')

    ec2_restart()

    p.kill() #kill sub process Technical Indicator

    ps="sudo kill -9 $(pgrep -f "+algoName+")"
    os.system(ps)

    os.system("sudo kill -9 $(pgrep -f python3.*multiprocessing)")


# %%
def algo_start_reprocess_orders():
    mainlogger.info('Function: algo_start_reprocess_orders')
    
    try:
        coredb = pool.connection()
        cursor = coredb.cursor()
        
        try:
            # SQL Queries
            sql1 = (
                "UPDATE ordertracker ot "
                "SET scheduler_name = SUBSTRING_INDEX(ot.events, '|', -1) "
                "WHERE scheduler_name = 'Processing' "
                "AND active_flag = 1"
            )
            
            sql2 = (
                "UPDATE ordertracker ot "
                "SET comments = CONCAT_WS('|', ot.comments, 'Manual Reprocess Original sched time', ot.sched_order_time), "
                "scheduler_name = DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 1 MINUTE), '%%Y%%m%%d%%H%%i'), "
                "sched_order_time = DATE_ADD(NOW(), INTERVAL 1 MINUTE) - INTERVAL SECOND(NOW()) SECOND "
                "WHERE sched_order_time <= NOW() "
                "AND scheduler_name LIKE '202%%' "
                "AND active_flag = 1"
            )

            sql3 = (
                "SELECT COUNT(1) FROM ordertracker "
                "WHERE scheduler_name = 'Processing' "
                "AND active_flag = 1"
            )
            
            sql4 = (
                "SELECT COUNT(1) FROM ordertracker "
                "WHERE sched_order_time <= NOW() "
                "AND scheduler_name LIKE '202%%' "
                "AND active_flag = 1"
            )

            # Fetch initial counts
            cursor.execute(sql3)
            res1 = cursor.fetchone()
            
            cursor.execute(sql4)
            res2 = cursor.fetchone()
            
            # Default to 0 if result is None
            count_processing = res1[0] if res1 else 0
            count_202 = res2[0] if res2 else 0
            
            # Send notifications
            try:
                send_notification(group_chat_id, f"Orders in processing will be reprocessed: {count_processing}")
                send_notification(group_chat_id, f"Orders scheduled but missed will be reprocessed: {count_202}")
            except Exception as notify_error:
                mainlogger.warning(f"Notification failed: {notify_error}")

            # Perform updates with individual commits
            cursor.execute(sql1)
            coredb.commit()  # Commit for sql1
            
            cursor.execute(sql2)
            coredb.commit()  # Commit for sql2

        except Exception as update_error:
            mainlogger.exception("Error during algo_start_reprocess_orders ", exc_info=True)
            coredb.rollback()  # Rollback changes from the current transaction
            raise update_error  # Re-raise the exception for outer handling
        
        finally:
            # Ensure cursor is closed
            if cursor:
                cursor.close()

        return 1
    
    except Exception as e:
        mainlogger.exception("Exception During: Algo Start Missed Orders Reprocessing", exc_info=True)
        try:
            send_notification(group_chat_id, 'Exception During: Algo Start Missed Orders Reprocessing')
        except Exception as notify_error:
            mainlogger.warning(f"Notification failed during exception handling: {notify_error}")
        return -1

    finally:
        # Ensure connection is closed
        if 'coredb' in locals() and coredb:
            coredb.close()

            
# %%
def algo_start_test_dummy_order():
    mainlogger.info('Function: algo_start_test_dummy_order')
    
    try:
        coredb = pool.connection()
        cursor = coredb.cursor()

        try:
            # Update SQL statement
            upd_sql = (
                "UPDATE ordertracker SET scheduler_name = 'EXECUTENOWUIX' "
                "WHERE Strategy_ID = 1 AND created_at_date = CURRENT_DATE()"
            )
            
            # Select SQL statement
            sel_sql = (
                "SELECT scheduler_name FROM ordertracker o "
                "WHERE Strategy_ID = 1 AND created_at_date = CURRENT_DATE() "
                "ORDER BY o.last_updated DESC LIMIT 1"
            )

            # Perform update and commit
            cursor.execute(upd_sql)
            coredb.commit()  # Commit after the update

            #trigger the job
            run_scheduled_jobs_parallel('ALLNOW');

            for i in range(10):
                try:
                    # Fetch updated status
                    df = pd.read_sql(sel_sql, coredb)
                    df = df.replace({np.nan: None})
                    
                    # Extract status
                    status = df.iloc[0, 0] if not df.empty else None

                    if status in ("Rejected", "Completed"):
                        # Successful condition
                        mainlogger.info('Algo Start Dummy Order Successful')
                        send_notification(group_chat_id, 'Algo Start Dummy Order Successful')
                        return 1
                    
                except Exception as fetch_error:
                    mainlogger.warning(f"Error algo_start_test_dummy_order : {fetch_error}")
                    continue  # Retry fetching in the next iteration

                time.sleep(3)  # Wait before retrying

            # If loop completes without success
            send_notification(group_chat_id, 'FAILED: Algo Start Dummy Order Failed')
            return -1

        except Exception as update_error:
            mainlogger.exception("Error during algo_start_test_dummy_order execution", exc_info=True)
            coredb.rollback()  # Rollback the update if it fails
            raise update_error  # Re-raise for outer exception handling
        
        finally:
            # Ensure cursor is closed
            if cursor:
                cursor.close()

    except Exception as e:
        mainlogger.exception("Exception During: Algo Start Dummy Order", exc_info=True)
        try:
            send_notification(group_chat_id, 'Exception During: Algo Start Dummy Order')
        except Exception as notify_error:
            mainlogger.warning(f"Notification failed during exception handling: {notify_error}")
        return -1

    finally:
        # Ensure connection is closed
        if 'coredb' in locals() and coredb:
            coredb.close()


if __name__ == "__main__":
    try:
        attach_scheduler()

        #start of the algo test a dummy order
        algo_start_test_dummy_order()
        
        #start reprocessing of any missed orders
        algo_start_reprocess_orders()

        # initialize only run once
        global_delta_next_trading_day=get_next_trading_day_delta()
        attach_sl_scheduler()

        #order_processors_start_workers()
        
        while True:
            ##print('while...')
            try:
                attach_scheduler()
                refresh_new_base_symbols()

                dt=datetime.datetime.now(ist)        
                if mode=='prod':
                    if (dt>ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 16,45,0,0))): 
                        if not (dt.strftime("%m-%d-%y") in nse_holdiays or dt.strftime("%A") in ('Saturday','Sunday')):
                           #mainlogger.info('Holiday so after market skipped')
                            after_market()
                            update_ordergroup_status() 
                        #cleanup
                        pool.close()
                        break;


                update_ltp()

                ## AMO SL Order breached by an unusual spike 
                if (ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 9,15,0,0))<dt<ist.localize(datetime.datetime(dt.year, dt.month, dt.day, 9,35,0,0))):
                    slbreach_check() #handle spikes and SL misses (uses Orginal SL placed not modified SLs because of Circuit Limit)

                #Market hours DB tasks
                #db_algo_tasks()

                thread_monitoring_caller() #monitor background threads and restart on failures

            except Exception as e:
                handle_exception('Algo error will be retried: ',str(e),'NA',False)

            time.sleep(1)

        ##initiate_shutdown_sequence
        initiate_shutdown_sequence()
        
    except Exception as e:
        ##Unlock the DB 
        unlock_db()
        handle_exception('Algo Terminated : ',str(e),'NA',False)
        
        ps="sudo kill -9 $(pgrep -f "+algoName+")"
        os.system(ps)
        
        os.system("sudo kill -9 $(pgrep -f python3.*multiprocessing)")

# %%
#run_scheduled_jobs_parallel('202310281250')
#run_scheduled_jobs_parallel('EXECUTENOWUI')
