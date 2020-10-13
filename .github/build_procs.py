import os
import json

directory = '../Stored-Procedures'

with open('config.json') as c:
    config = json.load(c)

tasks = []
for task in config['tasks']:
    tasks.append(task['taskName'])

for filename in os.listdir(directory):

    taskname = ".".join(filename.split(".")[:-1]) 

    with open(directory+'/'+filename) as file:
        sql_command = file.read()

    if taskname in tasks:
        for 
        warehouse = config['tasks']

    task_script = f'''
    CREATE OR REPLACE TASK IF NOT EXISTS {taskname}
    WAREHOUSE = 
    SCHEDULE = '* * * * *'
    COPY GRANTS
    COMMENT = '<string_literal>'
    AFTER <string>
    AS
    ''' + sql_command

    #print(task_script)
    