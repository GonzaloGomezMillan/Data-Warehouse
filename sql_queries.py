import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config['S3']['LOG_DATA']
LOG_JSON_PATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']
ARN = config['IAM_ROLE']['ARN']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events
                             (
                                event_id        BIGINT IDENTITY(0,1),
                                artist          VARCHAR,
                                auth            VARCHAR,
                                firstName       VARCHAR,
                                gender          VARCHAR,
                                iteminSession   VARCHAR,
                                lastName        VARCHAR,
                                length          VARCHAR,
                                level           VARCHAR,
                                location        VARCHAR,
                                method          VARCHAR,
                                page            VARCHAR,
                                registration    VARCHAR,
                                sessionId       VARCHAR SORTKEY DISTKEY,
                                song            VARCHAR,
                                status          INTEGER,
                                ts              TIMESTAMP,
                                userAgent       VARCHAR,
                                userId          INTEGER
                             );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                             (
                                num_songs           INTEGER,
                                artist_id           VARCHAR,
                                artist_latitude     VARCHAR,
                                artist_longitude    VARCHAR,
                                artist_location     VARCHAR,
                                artist_name         VARCHAR,
                                song_id             VARCHAR,
                                title               VARCHAR,
                                duration            DECIMAL,
                                year                INTEGER
                             );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay
                        (
                            songplay_id       INTEGER IDENTITY(0,1)     NOT NULL SORTKEY, 
                            start_time        TIMESTAMP                 NOT NULL,
                            user_id           VARCHAR                   NOT NULL DISTKEY,
                            level             VARCHAR                   NOT NULL,
                            song_id           VARCHAR                   NOT NULL,
                            artist_id         VARCHAR                   NOT NULL,
                            session_id        VARCHAR                   NOT NULL,
                            location          VARCHAR,
                            user_agent        VARCHAR
                        );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                    (
                        user_id     INTEGER  NOT NULL   SORTKEY,
                        first_name  VARCHAR  NULL,
                        last_name   VARCHAR  NULL,
                        gender      VARCHAR  NULL,
                        level       VARCHAR  NULL
                    );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song
                    (
                        song_id     VARCHAR     NOT NULL SORTKEY,
                        title       VARCHAR     NOT NULL,
                        artist_id   VARCHAR     NOT NULL,
                        year        INTEGER     NOT NULL,
                        duration    DECIMAL    NOT NULL
                    );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist
                    (
                        artist_id   VARCHAR     NOT NULL SORTKEY,
                        name        VARCHAR     NOT NULL,
                        location    VARCHAR     NOT NULL,
                        latitude    DECIMAL     NOT NULL,
                        longitude   DECIMAL     NOT NULL
                    );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                    (
                        start_time  TIMESTAMP   NOT NULL,
                        hour        INTEGER     NOT NULL,
                        day         INTEGER     NOT NULL,
                        week        INTEGER     NOT NULL,
                        month       INTEGER     NOT NULL,
                        year        INTEGER     NOT NULL,
                        weekday     INTEGER
                    );
""")

# STAGING TABLES

staging_events_copy = ("""
        COPY    staging_events 
            FROM {}
            IAM_ROLE {}
            FORMAT AS JSON {}
            REGION 'us-west-2'
            TIMEFORMAT AS 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSON_PATH)
staging_songs_copy = ("""
        COPY    staging_songs 
            FROM {}
            IAM_ROLE {}
            FORMAT AS JSON 'auto'
            REGION 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT  DISTINCT (e.ts)         AS start_time,
                                    e.userId                AS user_id,
                                    e.level,
                                    s.song_id,
                                    s.artist_id,
                                    e.sessionId             AS session_id,
                                    e.location,
                                    e.userAgent             AS user_agent
                            FROM    staging_events e
                            JOIN    staging_songs s ON (e.song = s.title)
                            WHERE   e.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT (userId)    AS user_id,
                                        firstName   AS first_name,
                                        lastName    AS last_name,
                                        gender,
                                        level
                        FROM            staging_events
                        WHERE           user_id IS NOT NULL
""")

song_table_insert = ("""INSERT INTO song(song_id, title, artist_id, year, duration)
                        SELECT DISTINCT     (song_id),
                                            title,
                                            artist_id,
                                            year,
                                            duration
                        FROM                staging_songs
                        WHERE               song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artist(artist_id, name, location, latitude, longitude)
                            SELECT DISTINCT     (artist_id),
                                                artist_name             AS name,
                                                artist_location         AS location,
                                                artist_latitude         AS latitude,
                                                artist_longitude        AS longitude
                            FROM                staging_songs
                            WHERE               artist_id IS NOT NULL 
                            AND                 artist_latitude IS NOT NULL
                            AND                 artist_location IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT     (start_time) AS start_time,
                                            EXTRACT(hour FROM start_time) AS hour,
                                            EXTRACT(day FROM start_time) AS DAY,
                                            EXTRACT(week FROM start_time) AS week,
                                            EXTRACT(month FROM start_time) AS month,
                                            EXTRACT(year FROM start_time) AS year,
                                            EXTRACT(dayofweek FROM start_time) AS weekday                                       
                        FROM                songplay;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
