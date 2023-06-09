-- SQL from Data Warehouse project

drop table if exists staging_events;
drop table if exists staging_songs;
drop table if exists songplays;
drop table if exists users;
drop table if exists songs;
drop table if exists artists;
drop table if exists time;


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
);

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
);

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
);

create table users (
    user_id      text     not null,
    first_name   text     not null,
    last_name    text     not null,
    gender       text     not null,
    level        text     not null,
    primary key(user_id)
);

create table songs (
    song_id    text       not null,
    title      text       not null,
    artist_id  text       not null,
    year       smallint   not null,
    duration   float      not null,
    primary key(song_id)
);

create table artists (
    artist_id  text      not null,
    name       text      not null,
    location   text      not null,
    latitude   float     ,
    longitude  float     ,
    primary key (artist_id)
);

create table time (
    start_time  datetime  not null,
    hour        smallint  not null,
    day         smallint  not null,
    week        smallint  not null,
    month       smallint  not null,
    year        smallint  not null,
    weekday     smallint  not null
);
