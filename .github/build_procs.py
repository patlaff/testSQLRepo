import os
import json
import snowflake.connector

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
        for name, parameters in config.items():
            if name==taskname:
                if parameters['warehouse']:
                    warehouse = parameters['warehouse']
                    '''
                    if ';' in warehouse:
                        print('Invalid character (;) found in config file. Please revise.')
                        sys.exit()
                    '''
                if parameters['schedule']:
                    schedule = parameters['schedule']
                if parameters['dependency']:
                    dependency = parameters['dependency']
        warehouse = config['tasks']
        
    task_script = f'''
    CREATE OR REPLACE TASK IF NOT EXISTS {taskname}
    COPY GRANTS
    WAREHOUSE = '{warehouse}'
    '''
    if schedule: # or schedule <> '':
        task_script += f"\nSCHEDULE = '{schedule}'"
    if comment:
        task_script += f"\nCOMMENT = '{comment}'"
    if dependency:
        task_script += f"\nAFTER {dependency}"
    
    task_script += f'AS\n{sql_command}'

    print(task_script)
    