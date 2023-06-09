import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE', 'ARN')
S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
create table staging_events (
    artist         text,
    auth           text,
    firstName      text,
    gender         varchar(1),
    itemInSession  smallint,
    lastName       text,
    length         float,
    level          text,
    location       text,
    method         text,
    page           text,
    registration   float,
    sessionId      integer,
    song           text,
    status         integer,
    ts             bigint,
    userAgent      text,
    userId         integer      
)
""")

staging_songs_table_create = ("""
create table staging_songs (
    num_songs         integer, 
    artist_id         text,
    artist_longitude  float,
    artist_latitude   float,
    artist_location   text,
    artist_name       text,
    song_id           text,
    title             text,
    duration          float,
    year              smallint     
)
""")

songplay_table_create = ("""
create table songplays (
    songplay_id  integer,
    start_time   datetime,
    user_id      text,
    level        text,
    song_id      text,
    artist_id    text,
    session_id   text,
    location     text,
    user_agent   text,
    primary key(songplay_id)
)
""")

user_table_create = ("""
create table users (
    user_id      text     not null,
    first_name   text     not null,
    last_name    text     not null,
    gender       text     not null,
    level        text     not null,
    primary key(user_id)
)
""")

song_table_create = ("""
create table songs (
    song_id    text     not null,
    title      text     not null,
    artist_id  text     not null,
    year       smallint    not null,
    duration   float       not null,
    primary key(song_id)
)
""")

artist_table_create = ("""
create table artists (
    artist_id  text      not null,
    name       text      not null,
    location   text      not null,
    latitude   float        ,
    longitude  float        ,
    primary key (artist_id)
)
""")

time_table_create = ("""
create table time (
    start_time  datetime  not null,
    hour        smallint  not null,
    day         smallint  not null,
    week        smallint  not null,
    month       smallint  not null,
    year        smallint  not null,
    weekday     smallint  not null
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto ignorecase';
""").format(S3_LOG_DATA, ARN)

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto ignorecase';
""").format(S3_SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select se.ts, se.userId, se.level, s.song_id, a.artist_id, se.sessionId, se.location, se.userAgent
from staging_events se
left outer join artists a ON se.artist = a.name
left outer join songs s ON se.song = s.title
""")

user_table_insert = ("""
insert into users(user_id, first_name, last_name, gender, level)
select userId, firstName, lastName, gender, level
from staging_events
""")

song_table_insert = ("""
insert into songs(song_id, title, artist_id, year, duration)
select song_id, title, artist_id, year, duration
from staging_songs
""")

artist_table_insert = ("""
insert into artists(artist_id, name, location, latitude, longitude)
select artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from staging_songs
""")

time_table_insert = ("""
insert into time(start_time, hour, day, week, month, year, weekday)
with tss as (
    select distinct timestamp 'epoch' + ts/1000 * interval '1 second' as start_time 
    from staging_events
)
select  start_time,
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time),
        extract(month from start_time),
        extract(year from start_time),
        extract(dow from start_time)
from timestamps
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
