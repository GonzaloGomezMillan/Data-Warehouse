import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config['S3']['LOG_DATA']
LOG_JSON_PATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']
ARN = config['IAM_ROLE']['ARN']
# REGION = config['S3']['REGION']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
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
                             )
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

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
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

user_table_create = ("""CREATE TABLE IF NOT EXISTS user
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
                        duration    DEDCIMAL    NOT NULL
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
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json {};
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json {};
""").format(LOG_DATA, IAM_ROLE_ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT  e.song_id,
                                    e.XXXXXXXXXXXXXXXXXXXXXXXXX,
                                    e.userId,
                                    e.level,
                                    s.song_id,
                                    s.artist_id,
                                    e.sessionId,
                                    e.location,
                                    e.userAgent
                            FROM    staging_events e
                            JOIN    staging_songs s ON (e.song = s.title)
""")

user_table_insert = ("""INSERT INTO user(user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT (userId),
                                        firstName,
                                        lastName,
                                        gender,
                                        level
                        FROM            staging_events
""")

song_table_insert = ("""INSERT INTO song(song_id, title, artist_id, year, duration)
                        SELECT DISTINCT     (song_id),
                                            title,
                                            artist_id,
                                            year,
                                            duration
                        FROM                staging_songs
""")

artist_table_insert = ("""INSERT INTO artist(artist_id, name, location, latitude, longitude)
                            SELECT DISTINCT     (artist_id),
                                                artist_name,
                                                artist_location,
                                                artist_latitude,
                                                artist_longitude
                            FROM                staging_songs
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        SELECT XXXXXXXXXXXXXX
                        FROM 
                        JOIN     ON
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
