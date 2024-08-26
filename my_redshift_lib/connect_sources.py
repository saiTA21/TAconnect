import psycopg2
import logging
import boto3
import json
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create a SparkSession object
spark = SparkSession.builder.getOrCreate()

# Initialize logger
logger = logging.getLogger(__name__)

def connect_redshift(user_name=None, password=None, dbname="dev", host="klg-nga-npd-dev.c0idf8qkm7kk.us-east-1.redshift.amazonaws.com", port="5439"):
    """
    Establishes a connection to the Redshift database using credentials from AWS Secrets Manager if user_name and password are not provided.

    Parameters:
        user_name (str, optional): The username for the Redshift database. If not provided, it's fetched from AWS Secrets Manager.
        password (str, optional): The password for the Redshift database. If not provided, it's fetched from AWS Secrets Manager.
        dbname (str): The Redshift database name.
        secret_name (str): The name of the secret in AWS Secrets Manager.
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

def connect_mssql(user_name=None, password=None, url="jdbc:sqlserver://usawscwsql5146.us.kellogg.com\\ANALYTICSDEV:57685;databaseName=BODS_TEST;encrypt=True;trustServerCertificate=true"):
    """
    Establishes a connection to the MS SQL Server database using credentials from AWS Secrets Manager if user_name and password are not provided.

    Parameters:
        user_name (str, optional): The username for the MS SQL Server database. If not provided, it's fetched from AWS Secrets Manager.
        password (str, optional): The password for the MS SQL Server database. If not provided, it's fetched from AWS Secrets Manager.
        secret_name (str): The name of the secret in AWS Secrets Manager.
        region_name (str): The AWS region where Secrets Manager is located.
        url (str): The JDBC URL for the MS SQL Server database.

    Returns:
        A function to execute SQL queries on MS SQL Server via Spark JDBC, or None if the connection failed.
    """
    try:
        def query_mssql(query):
            df = spark.read.format("jdbc").options(
                url=url,
                dbtable=f"({query}) as query",
                user=user_name,
                password=password,
                driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
            ).load()
            return df

        return query_mssql
        print("Connection successful.")

    except Exception as e:
        logger.error(f"Error: {e}")
        print("Failed to connect to MS SQL Server.")
        return None

def connect_rds(user_name=None, password=None, dbname="postgres", host="usawskortexpsdev.cisizjlv9mil.us-east-1.rds.amazonaws.com", port="5700"):
    """
    Establishes a connection to the RDS database using credentials from AWS Secrets Manager if user_name and password are not provided.

    Parameters:
        user_name (str, optional): The username for the RDS database. If not provided, it's fetched from AWS Secrets Manager.
        password (str, optional): The password for the RDS database. If not provided, it's fetched from AWS Secrets Manager.
        dbname (str): The RDS database name.
        secret_name (str): The name of the secret in AWS Secrets Manager.
        region_name (str): The AWS region where Secrets Manager is located.
        host (str): The RDS host address.
        port (str): The RDS port. Default is 5700.

    Returns:
        connection (psycopg2.extensions.connection): The connection object, or None if the connection failed.
    """
    try:

        logger.info("Connecting to the RDS database...")
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user_name,
            password=password,
            sslmode='require'  # Enable SSL
        )
        logger.info("Connected to the RDS database.")
        print("Connection successful.")
        return connection
    except Exception as e:
        logger.error(f"Error: {e}")
        print("Failed to connect to RDS.")
        return None