import os
import sys
import json
import logging
import snowflake.connector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
directory = os.path.dirname(__file__)+'/../Stored-Procedures'

with open(os.path.join(__location__, 'config.json')) as c:
    config = json.load(c)

tasks = []
for task in config['tasks']:
    tasks.append(task['taskName'])

for filename in os.listdir(directory):

    taskname = ".".join(filename.split(".")[:-1])
    warehouse = 'DEFAULT_WH'
    schedule = None
    dependency = None
    comment = None

    with open(directory+'/'+filename) as file:
        sql_command = file.read()

    if taskname in tasks:
        parameters = [x for x in config['tasks'] if x['taskName']==taskname][0]['parameters']
        try:
            warehouse = parameters['warehouse']
            if ';' in warehouse:
                logging.info(f'Invalid character (;) found in warehouse parameter for task, {taskname}. Please revise.')
                sys.exit()
        except KeyError:
            pass
        try:
            schedule = parameters['schedule']
            if ';' in schedule:
                logging.info(f'Invalid character (;) found in schedule parameter for task, {taskname}. Please revise.')
                sys.exit()
        except KeyError:
            pass
        try:
            dependency = parameters['dependency']
            if ';' in dependency:
                logging.info(f'Invalid character (;) found in dependency parameter for task, {taskname}. Please revise.')
                sys.exit()
        except KeyError:
            pass

    task_script = f'''
    CREATE OR REPLACE TASK IF NOT EXISTS {taskname}
    COPY GRANTS
    WAREHOUSE = '{warehouse}' '''
    
    if dependency: task_script += f"\nAFTER {dependency}"
    elif schedule: task_script += f"\nSCHEDULE = '{schedule}'"
    if comment:    task_script += f"\nCOMMENT = '{comment}'"
    
    task_script += f'\nAS\n{sql_command}'

    if task_script[-1] != ';': task_script += ';'

    logging.info(task_script)
    