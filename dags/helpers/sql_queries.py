class SqlQueries:

    songplay_table_insert = ("""
SELECT
  md5(events.sessionid::text || events.start_time::text) AS songplay_id,
  events.start_time,
  events.userid,
  events.level,
  songs.song_id,
  songs.artist_id,
  events.sessionid,
  events.location,
  events.useragent
FROM (
  SELECT
    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    *
  FROM staging_events
  WHERE page = 'NextSong'
) events
LEFT JOIN staging_songs songs
  ON events.song = songs.title
  AND events.artist = songs.artist_name
  AND events.length = songs.duration
    """)

    user_table_insert = ("""
      SELECT userid, MAX(firstname) AS firstname, MAX(lastname) AS lastname,
            MAX(gender) AS gender, MAX(level) AS level
      FROM staging_events
      WHERE page='NextSong' AND userid IS NOT NULL
      GROUP BY userid
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT distinct start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dow from start_time)
        FROM songplays
    """)