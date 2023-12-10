# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
                event_id    SERIAL PRIMARY KEY,
                artist      VARCHAR                 NULL,
                auth        VARCHAR                 NULL,
                firstName   VARCHAR                 NULL,
                gender      VARCHAR                 NULL,
                itemInSession VARCHAR               NULL,
                lastName    VARCHAR                 NULL,
                length      VARCHAR                 NULL,
                level       VARCHAR                 NULL,
                location    VARCHAR                 NULL,
                method      VARCHAR                 NULL,
                page        VARCHAR                 NULL,
                registration VARCHAR                NULL,
                session_id INTEGER NOT NULL,
                song        VARCHAR                 NULL,
                status      INTEGER                 NULL,
                ts          BIGINT                  NOT NULL,
                userAgent   VARCHAR                 NULL,
                userId      VARCHAR                 NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs           INTEGER         NULL,
                artist_id           VARCHAR         NOT NULL,
                artist_latitude     VARCHAR         NULL,
                artist_longitude    VARCHAR         NULL,
                artist_location     VARCHAR(500)   NULL,
                artist_name         VARCHAR(500)   NULL,
                song_id             VARCHAR         NOT NULL,
                title               VARCHAR(500)   NULL,
                duration            DECIMAL(9)      NULL,
                year                INTEGER         NULL
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
                songplay_id SERIAL PRIMARY KEY,
                start_time  TIMESTAMP               NOT NULL,
                user_id     INTEGER           NOT NULL,
                song_id     VARCHAR(40)             NOT NULL,
                session_id  VARCHAR(50)             NOT NULL,
                length      FLOAT                   NULL,
                location    VARCHAR(100)            NULL,
                user_agent  VARCHAR(255)            NULL,
                    CONSTRAINT fk_user
                    FOREIGN KEY(user_id) 
                    REFERENCES users(user_id),
                         
                    CONSTRAINT fk_song
                    FOREIGN KEY(song_id) 
                    REFERENCES songs(song_id),
                        
                         
                    CONSTRAINT fk_time 
                    FOREIGN KEY (start_time)
                    REFERENCES time (start_time)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER                 PRIMARY KEY,
                first_name  VARCHAR(50)             NULL,
                last_name   VARCHAR(80)             NULL,
                gender      VARCHAR(10)             NULL
    ) ;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
                song_id     VARCHAR(50)             PRIMARY KEY,
                title       VARCHAR(500)           NOT NULL,
                artist_id           VARCHAR         NOT NULL,
                artist_name   VARCHAR(500)             NOT NULL,
                year        INTEGER                 NOT NULL,
                duration    DECIMAL(9)              NOT NULL,
                location    VARCHAR(500)            NULL,
                CONSTRAINT fk_artist
                    FOREIGN KEY(artist_id, artist_name) 
                    REFERENCES artists(artist_id, name)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
                artist_id   VARCHAR(50)             NOT NULL,
                name        VARCHAR(500)           NULL,
                PRIMARY KEY (artist_id,name)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
                start_time  TIMESTAMP               PRIMARY KEY,
                hour        SMALLINT                NULL,
                day         SMALLINT                NULL,
                week        SMALLINT                NULL,
                month       SMALLINT                NULL,
                year        SMALLINT                NULL,
                weekday     SMALLINT                NULL
    ) ;
""")

# INSERT STAGING TABLES
insert_staging_songs = """
    INSERT INTO staging_songs (num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)
    VALUES (
        %(num_songs)s, %(artist_id)s, %(artist_latitude)s, %(artist_longitude)s, %(artist_location)s, %(artist_name)s, %(song_id)s, %(title)s, %(duration)s, %(year)s
    )
"""

insert_staging_events = """
    INSERT INTO staging_events (
        artist, auth, firstName, gender, itemInSession, lastName, length,
        level, location, method, page, registration, "session_id", song,
        status, ts, userAgent, userId
    ) VALUES (
        %(artist)s, %(auth)s, %(firstName)s, %(gender)s, %(itemInSession)s, %(lastName)s, %(length)s,
        %(level)s, %(location)s, %(method)s, %(page)s, %(registration)s, %(sessionId)s, %(song)s,
        %(status)s, %(ts)s, %(userAgent)s, %(userId)s
    )
"""

# INSERT FACT AND DIMENSION TABLES

songplay_table_insert = ("""
INSERT INTO songplays ( start_time,
                        user_id,
                        song_id,
                        session_id,
                        length,
                        location,
                        user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            CAST(se.userId AS INTEGER)  AS user_id,
            ss.song_id                  AS song_id,
            se.session_id               AS session_id,
            CAST(se.length as FLOAT)    AS length,            
            se.location                 AS location,
            se.userAgent                AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss
    ON (se.artist = ss.artist_name and se.song=ss.title);
""")

user_table_insert = ("""
    INSERT INTO users (                 user_id,
                                        first_name,
                                        last_name,
                                        gender)
    SELECT  DISTINCT CAST(se.userId AS INTEGER) AS user_id,
            se.firstName                AS first_name,
            se.lastName                 AS last_name,
            se.gender                   AS gender
    FROM staging_events AS se 
    WHERE se.userId != '';
""")

song_table_insert = ("""
    INSERT INTO songs ( song_id,
                        title,
                        artist_id,
                        artist_name,
                        year,
                        duration,
                        location)
    SELECT  DISTINCT ss.song_id         AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.artist_name              AS artist_name,
            ss.year                     AS year,
            ss.duration                 AS duration,
            ss.artist_location          AS location
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id,
                        name)
    SELECT   ss.artist_id,  ss.artist_name
    FROM staging_songs AS ss
    GROUP BY artist_name, artist_id;    
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS hour,
        EXTRACT(day FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS day,
        EXTRACT(week FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS week,
        EXTRACT(month FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS month,
        EXTRACT(year FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS year,
        EXTRACT(week FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS weekday
    FROM staging_events AS se;
""")

fill_location_with_other=("""
    UPDATE songs
    SET location = 'Other'
    WHERE location IS NULL OR location = '';
""")
fill_year_with_average=("""
    CREATE OR REPLACE PROCEDURE fill_year_with_average()
    LANGUAGE plpgsql
    AS $$
    BEGIN
        UPDATE songs
        SET year = (SELECT CAST(AVG(songs.year) AS INTEGER) FROM songs WHERE songs.artist_name = artist_name AND year != 0)
        WHERE year = 0;
    END;
    $$;
    CALL fill_year_with_average();
""")
# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,  song_table_drop, artist_table_drop, time_table_drop]

insert_table_queries = [user_table_insert,  artist_table_insert, song_table_insert, time_table_insert, songplay_table_insert]

pre_processing = [fill_location_with_other, fill_year_with_average]