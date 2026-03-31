from loguru import logger
import mysql.connector
import os
import polars as pl
import sys
import requests

logger.remove(0)
# Define log-level 
logger.add(sys.stderr, level="INFO")

# ************* General helper functions *****************


def get_database_connection():
    """
    Baut die Verbindung auf und gibt das Connection-Objekt zurück.
    Gibt None zurück, falls etwas schiefgeht.
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        if connection.is_connected():
            return connection
    except mysql.connector.Error as e:
        logger.error(f"Verbindungsfehler: {e}")
        return None

def check_latest_matchday_in_db(connection, table_name):
    """
    Check the 
    """
    if connection is None or not connection.is_connected():
        logger.error("Error: No active database connection.")
        return None
    
    mycursor = connection.cursor()
    mycursor.execute(
        """SELECT * 
        FROM customers {}"""
        )
    myresult = mycursor.fetchall()
    
    return NotImplementedError  

def query_db(query: str, connection):
    
    if connection is None or not connection.is_connected():
        logger.error("Error: No active database connection.")
        return None

    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        
        if cursor.description is not None:
            result = cursor.fetchall()
            return pl.DataFrame(result)
        else:
            connection.commit()
            logger.info(f"Query executed successfully. {cursor.rowcount} rows affected.")
            return cursor.rowcount
    except mysql.connector.Error as e:
        logger.error(f"Error executing query: {e}")
        connection.rollback()
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()

# ************* ETL Pipelines *****************

class DataLoader():
    
    def extract_data():
        raise NotImplementedError
    
    def transform_data():
        raise NotImplementedError
    
    def load_data(self, df: pl.DataFrame, table_name: str, connection):
        if connection is None or not connection.is_connected():
            logger.error("No active database connection.")
            return

        if df.is_empty():
            logger.warning("Dataframe is empty. Nothing to insert.")
            return

        columns = ", ".join(column.upper() for column in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        data = df.rows()

        try:
            cursor = connection.cursor()
            cursor.executemany(query, data)
            connection.commit()
            logger.info(f"Successfully inserted {cursor.rowcount} rows into {table_name}.")
        except mysql.connector.Error as e:
            logger.error(f"Error inserting data into {table_name}: {e}")
            connection.rollback()
        finally:
            if 'cursor' in locals():
                cursor.close()
    
    def execute_pipeline(delta_load: False):
        raise NotImplementedError
    

class MatchDataLoader(DataLoader): 

    def extract_data(id=4331, season_from=None, season_to=None):
        api_call = requests.get(f"https://www.thesportsdb.com/api/v1/json/123/eventsseason.php?id={id}&s={season_from}-{season_to}")
        storage = api_call.json()
        match_data_raw = pl.DataFrame(storage["events"])
        return match_data_raw
    
    def transform_data(self, raw_data):

        connection = get_database_connection()
        mapping_query = """ 
            SELECT
                CLUB_ID
                , CLUB_MATCHES_ID
            FROM road_to_draw.club_name_mapping
        """
        mapping_data = query_db(query=mapping_query, connection=connection)
        
        raw_df = raw_data.clone()
        raw_df = raw_df.join(
            mapping_data, 
            how="left", 
            left_on="home_team",
            right_on = "CLUB_MATCHES_ID"
            )
        raw_df = raw_df.drop("home_team").rename({"CLUB_ID":"HOME_TEAM"})
        # Repeat process for away team
        raw_df = raw_df.join(
            mapping_data, 
            how="left", 
            left_on="away_team",
            right_on = "CLUB_MATCHES_ID"
            )
        raw_df = raw_df.drop("away_team").rename({"CLUB_ID":"AWAY_TEAM"})

        transformed_df = raw_df.with_columns(
            pl.struct(["HOME_TEAM", "AWAY_TEAM", "season_from", "season_to", "matchday"]).hash().alias("MATCH_ID"),
            pl.lit(None).alias("match_time") # TODO: Remove Placeholder
        )


        # Sorting necessary for db input
        transformed_df = transformed_df.select(["MATCH_ID", "HOME_TEAM", "AWAY_TEAM", "season_from", "season_to", "matchday", "match_date", "match_time", "rank_home_team", "rank_away_team", "goals_home_team", "goals_away_team", "referee"])

        return transformed_df
# ['',
#  'home_team',
#  'away_team',
#  'referee',
#  'season_to',
#  'matchday',
#  'match_date',
#  'rank_home_team',
#  'rank_away_team',
#  'goals_home_team',
#  'goals_away_team',
#  'season_from']