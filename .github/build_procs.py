import os
import sys
import json
import getopt
import logging
import snowflake.connector

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set working directory information
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
directory = os.path.dirname(__file__)+'/../Stored-Procedures'

# Open config file to get task parameters
with open(os.path.join(__location__, 'config.json')) as c:
    config = json.load(c)

### Configure script arguments ##
# Define proper script usage. Display if options not entered properly
def usage():
    print('Usage: python '+sys.argv[0]+' -p, --password <password> [-h | --help]')

# Establish accepted arguments
try: 
    opts, args = getopt.getopt(sys.argv[1:], 'p:h', ['password=', 'help'])
except getopt.GetoptError:
    print('Invalid option')
    usage()
    sys.exit(2)

# Initiate variables
password = None

# Resolve arguments to variables
for opt, arg in opts:
    if opt in ('-h', '--help'):
        usage()
        sys.exit(2)
    if opt in ('-p', '--password'):
        password = arg

if not password:
    logging.info('No password provided, please include as argument')
    usage()

# Snowflake Account Information
env_vars = config['env']
snowflake.connector.paramstyle='qmark'
SF_ACCOUNT = env_vars['sf_account']        # USE account.east-us-2.azure - full qualified account for client since it's on Azure
SF_USER = env_vars['sf_user'] 
SF_PASSWORD = password
SF_ROLE = env_vars['sf_role'] 
SF_WAREHOUSE = env_vars['sf_warehouse'] 
SF_DATABASE = env_vars['sf_database'] 
#SF_AUTHENTICATOR = 'https://vertexinc.okta.com/'

# Connect to Snowflake using the default authenticator, and variables defined above
sf_con = snowflake.connector.connect(
    account = SF_ACCOUNT,
    user = SF_USER,
    password = SF_PASSWORD,
    role = SF_ROLE,
    warehouse = SF_WAREHOUSE
)

# Create Cursor on Snowflake Connection 
sf_cur = sf_con.cursor()

# Use a specific Database, based on variables defined above
sf_cur.execute('USE ' + SF_DATABASE)

# Create list of tasks in config file.
tasks = []
for task in config['tasks']:
    tasks.append(task['taskName'])

# Loop over files in directory established above
for filename in os.listdir(directory):

    taskname = ".".join(filename.split(".")[:-1])
    warehouse = SF_WAREHOUSE
    schedule = None
    dependency = None 
    comment = None

    if ';' in taskname:
        logging.info(f'Invalid character (;) found in file name, {taskname}. Please revise.')
        sys.exit()

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
    
    #COPY GRANTS
    task_script = f'''
    CREATE OR REPLACE TASK {taskname}
    WAREHOUSE = '{warehouse}' '''
    
    if dependency: task_script += f"\nAFTER {dependency}"
    elif schedule: task_script += f"\nSCHEDULE = 'USING CRON {schedule} EST'"
    if comment:    task_script += f"\nCOMMENT = '{comment}'"
    
    task_script += f'\nAS\n{sql_command}'

    if task_script[-1] != ';': task_script += ';'

    sf_cur.execute(task_script)
