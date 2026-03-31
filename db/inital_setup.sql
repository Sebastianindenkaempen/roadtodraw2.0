/*
This script aims to setup the necessary tables in the database
*/

DROP TABLE IF EXISTS road_to_draw.match_data;
CREATE TABLE road_to_draw.match_data (
	MATCH_ID VARCHAR(255),
    HOME_TEAM VARCHAR (5),
    AWAY_TEAM VARCHAR(5), 
    SEASON_FROM INT(4),
    SEASON_TO INT(4),
    MATCHDAY INT(2),
    MATCH_DATE TIMESTAMP,
    MATCH_TIME TIME, 
    RANK_HOME_TEAM INT(2),
    RANK_AWAY_TEAM INT(2),
    GOALS_HOME_TEAM INT(2),
    GOALS_AWAY_TEAM INT(2),
    REFEREE VARCHAR(255)
    );

