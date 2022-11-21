import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""create table if not exists staging_events
                             (
                                artist          varchar,
                                auth            varchar,
                                firstName       varchar,
                                gender          varchar,
                                iteminSession   integer,
                                lastName        varchar,
                                length          decimal,
                                level           varchar,
                                location        varchar,
                                method          varchar,
                                page            varchar,
                                registration    integer,
                                sessionId       integer,
                                song            varchar,
                                status          integer,
                                ts              integer,
                                userAgent       varchar,
                                userId          integer
                             )
""")

staging_songs_table_create = ("""create table if not exists staging_songs
                             (
                                num_songs           integer,
                                artist_id           varchar,
                                artist_latitude    integer,
                                artist_longitude    integer,
                                artist_location     varchar,
                                artist_name         varchar,
                                song_id             varchar,
                                title               varchar,
                                duration            decimal,
                                year                integer
                             );
""")

songplay_table_create = ("""create table if not exists songplays
                        (
                            songplay_id       varchar,
                            start_time        bigint,
                            user_id           int,
                            level             varchar,
                            song_id           varchar,
                            artist_id         varchar,
                            session_id        int,
                            location          varchar,
                            user_agent        varchar
                        );
""")

user_table_create = ("""create table if not exists user
                    (
                        user_id     int,
                        first_name  varchar,
                        last_name   varchar,
                        gender      varchar,
                        level       varchar
                    );
""")

song_table_create = ("""create table if not exists song
                    (
                        song_id     varchar,
                        title       varchar,
                        artist_id   varchar,
                        year        int,
                        duration    float
                    );
""")

artist_table_create = ("""create table if not exists artist
                    (
                        artist_id   varchar,
                        name        varchar,
                        location    varchar,
                        latitude    decimal,
                        longitude   decimal
                    );
""")

time_table_create = ("""create table if not exists time
                    (
                        start_time  time,
                        hour        integer,
                        day         integer,
                        week        integer,
                        month       integer,
                        year        integer,
                        weekday     integer
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
""").format(LOG_DATA, IAM_ROLE_ARN, )

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
