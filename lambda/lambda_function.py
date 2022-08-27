import psycopg2
from aws_lambda_powertools.utilities import parameters
from botocore.config import Config

#Define our lambda handler function to be called on S3 upload trigger
def lambda_handler(event, context):

    #add a config from botocore to include the required region
    config = Config(region_name="us-east-1")

    #to retrieve the db password from secrets manager, set the SecretsProvider and pass the config.
    secrets_provider = parameters.SecretsProvider(config=config)

    #Call the secret key and set it to a variable.
    value = secrets_provider.get("test2")

    #print the event (this is really just for your benefit to see what information is included in the event.)
    print(event)
    
    # set the connect variable to execute commands against the database with psycopg2
    conn = psycopg2.connect("host=personsdb.ca6vr9jjlgch.us-east-1.rds.amazonaws.com dbname=postgres user=postgres password={}".format(value))

    #use the psycopg2 cursor to execute the sql commands
    conn.cursor().execute("DROP TABLE IF EXISTS persons; CREATE TABLE persons (id SERIAL PRIMARY KEY, first_name varchar(80), title varchar(80), location varchar(80)); SELECT aws_s3.table_import_from_s3('persons','','(format csv)','(sfs-misc-data,data.csv,us-east-1)');")

    #commit the execution
    conn.commit()