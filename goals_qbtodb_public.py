import pandas as pd
import numpy as np
import requests
import xml.etree.ElementTree as ET
import pymysql
from  sqlalchemy import create_engine

def authenticate():
    url = 'https://nextsteppcs.quickbase.com/db/main'
    api_call = '?a=API_Authenticate'
    user_pass = '&username=username&password=password'
    time = '&hours=24'
    final = (url+api_call+user_pass+time)
    authenticate = requests.post(final)
    authenticate = authenticate.text
    authenticate = ET.fromstring(authenticate)
    for child in authenticate:
        if (child.tag == 'ticket'):
             ticket = child.text
             return ticket

def do_query(database,qid,clist):
    ticket = '&ticket='+authenticate()
    url = 'https://nextsteppcs.quickbase.com/db/'
    api_call = '?a=API_DoQuery'
    options = '&options=sortorder-A'
    qid = '&qid='+qid
    clist = '&clist='+clist
    final = url+database+api_call+ticket+qid+clist+options
    response = requests.post(final)
    del response.headers
    response = response.text
    response = ET.fromstring(response)
    return response

def xml_to_frame(tree):
    data = []
    for child in tree:
        if (child.tag == 'record'):
            dict = {}
            for subchild in child:
                dict[subchild.tag] = subchild.text
            data.append(dict)
    frame = pd.DataFrame (data)
    return frame

goals = do_query('bk5b6bpss','1','3.20.64.84.93.85.86.87.54')
deliverables = do_query('bpcr7s52u','11','3.74.73.67.6.10.102')
tasks = do_query('bpcr7t7hv','27','3.77.76.64.5.29.10.13.14.73')

goals = xml_to_frame(goals)
deliverables = xml_to_frame(deliverables)
tasks = xml_to_frame(tasks)

goals = goals[goals.protocol == 'ILP']
goals = goals.drop(columns={'update_id','protocol'})
goals["goal_id"] = pd.to_numeric(goals["goal_id"])
goals = goals.set_index('goal_id')

conn_old = pymysql.connect(host='127.0.0.1',
                     user='db_username',
                     passwd='password',
                     db='db_name',)

conn = create_engine('mysql://db_username:password@127.0.0.1/db_name')
goals.to_sql(name='goals', con=conn, if_exists= 'replace', index=True)
deliverables.to_sql(name='deliverables', con=conn, if_exists= 'replace', index=True)
tasks.to_sql(name='tasks', con=conn, if_exists= 'replace', index=True)
