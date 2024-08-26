import psycopg2
import logging
import boto3
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_redshift(user_name, password, dbname="dev", region_name="us-east-1", host="klg-nga-npd-dev.c0idf8qkm7kk.us-east-1.redshift.amazonaws.com", port="5439"):
    """
    Establishes a connection to the Redshift database using credentials from AWS Secrets Manager.
    
    Parameters:
        user_name (str): The username for the Redshift database.
        password (str): The password for the Redshift database.
        dbname (str): The Redshift database name.
        region_name (str): The AWS region where Secrets Manager is located.
        host (str): The Redshift host address.
        port (str): The Redshift port. Default is 5439.
    
    Returns:
        connection (psycopg2.extensions.connection): The connection object, or None if the connection failed.
    """
    try:
        
        logger.info("Connecting to the Redshift database...")
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user_name,
            password=password,
            sslmode='require'  # Enable SSL
        )
        logger.info("Connected to the Redshift database.")
        print("Connection successful.")
        return connection
    except Exception as e:
        logger.error(f"Error: {e}")
        print("Failed to connect to Redshift.")
        return None
    
def connect_snowflake(user_name, password,"):
    """
    Establishes a connection to the Redshift database using credentials from AWS Secrets Manager.
    
    Parameters:
        user_name (str): The username for the Redshift database.
        password (str): The password for the Redshift database.
    
    Returns:
        connection (psycopg2.extensions.connection): The connection object, or None if the connection failed.
    """
    try:
        
        logger.info("Connecting to the Redshift database...")
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user_name,
            password=password,
            sslmode='require'  # Enable SSL
        )
        logger.info("Connected to the Redshift database.")
        print("Connection successful.")
        return connection
    except Exception as e:
        logger.error(f"Error: {e}")
        print("Failed to connect to Redshift.")
        return None
