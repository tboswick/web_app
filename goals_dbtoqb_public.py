import csv
import pandas as pd
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

def import_data(records_csv,database,clist):
    ticket = "&ticket="+authenticate()
    url = 'https://nextsteppcs.quickbase.com/db/'
    csv = '&records_csv='+records_csv
    api_call = '?a=API_ImportFromCSV'
    clist = '&clist='+clist
    final = url+database+api_call+ticket+csv+clist
    response = requests.post(final)
    #print (response)

def run_import(database, import_id):
    app_token = 'cwq74rcdzm9drubdbcw3pbsx5zfq'
    url = 'https://nextsteppcs.quickbase.com/db/'
    api_call = '?a=API_RunImport'
    id = '&id='+import_id
    token = '&apptoken='+app_token
    ticket = "&ticket="+authenticate()
    final = url+database+api_call+id+token+ticket
    response = requests.post(final)
    print (response)

def prep_csv(frame):
    test_data = ''
    with open(frame, 'r') as csv_file:
        for row in csv_file:
            test_data=test_data+row
    #test_data =  '"""'+test_data+'"""'
    return test_data

conn = create_engine('mysql://username:password@127.0.0.1/db_name')
sql = 'SELECT * FROM goal_input'
task_sql = 'SELECT * FROM task_images'

goals_raw = pd.read_sql(sql=sql, con=conn)
existing = do_query('table_name','1','3.20.64.65.66.67.68.69.54')
existing = xml_to_frame(existing)
existing = existing[existing.protocol == 'ILP']
existing = existing.drop(columns='update_id')
existing = existing.rename(columns={'tnsid':'ID', 'student_input_ilp_smart_goal':'goal_text','is_the_goal_specific_':'goal_reasons',
                                    'is_the_goal_measurable_':'goal_when_accomplish','is_the_goal_achievable_':'goal_how_accomplish',
                                    'is_the_goal_relevant_':'goal_match','is_the_goal_timely_':'goal_celebrate' })
existing = existing[['ID','goal_text','goal_reasons','goal_when_accomplish','goal_how_accomplish','goal_match','goal_celebrate']]

goals_raw["ID"] = pd.to_numeric(goals_raw["ID"])
existing["ID"] = pd.to_numeric(existing["ID"])

goals = goals_raw.merge(existing.drop_duplicates(),on=['ID','goal_text'],how='left',indicator=True)
goals = goals[goals['_merge'] == 'left_only']
goals = goals.drop(columns={'goal_reasons_y','goal_when_accomplish_y','goal_how_accomplish_y','goal_match_y','goal_celebrate_y','_merge'})
goals['protocol']='ILP'

tasks_raw = pd.read_sql(sql=task_sql, con=conn)
tasks_raw.to_csv('task_input.csv', header=False, index=False)

goals.to_csv('goals.csv', header=False, index=False)

import_data(prep_csv('task_input.csv'),'bphapespk','6.7.8.9')
import_data(prep_csv('goals.csv'),'bpfht5zc2','6.7.8.9.10.11.12.13')

empty_frame = pd.DataFrame(columns=['ID','goal_text','goal_reasons','goal_when_accomplish','goal_how_accomplish','goal_match','goal_celebrate'])
empty_frame.to_sql(name='goal_input', con=conn, if_exists= 'replace', index=False)

empty_frame = pd.DataFrame(columns=['ID','task_id','image_text','image_name'])
empty_frame.to_sql(name='task_images', con=conn, if_exists= 'replace', index=False)
