-- What Are the Most Popular Tracks?
SELECT
    t.TRACK_ID,
    t.TRACK_NAME,
    AVG(d.POPULARITY) AS AVG_POPULARITY
FROM tracks t
JOIN daily_track_ranking d ON t.TRACK_ID = d.TRACK_ID
GROUP BY t.TRACK_ID, t.TRACK_NAME
ORDER BY AVG_POPULARITY DESC
LIMIT 10;

-- Who Are the Most Popular Artists?
SELECT
    a.ARTIST_ID,
    a.ARTIST_NAME,
    AVG(d.POPULARITY) AS AVG_POPULARITY
FROM artists a
JOIN daily_track_ranking d ON a.ARTIST_ID = d.ARTIST_ID
GROUP BY a.ARTIST_ID, a.ARTIST_NAME
ORDER BY AVG_POPULARITY DESC
LIMIT 10;

-- Which Genres Are the Most Popular?
SELECT
    a.GENRES,
    AVG(d.POPULARITY) AS AVG_POPULARITY
FROM artists a
JOIN daily_track_ranking d ON a.ARTIST_ID = d.ARTIST_ID
GROUP BY a.GENRES
ORDER BY AVG_POPULARITY DESC
LIMIT 10;

-- Track Popularity vs. Stream Rank
SELECT
    t.TRACK_ID,
    t.TRACK_NAME,
    d.POPULARITY,
    d.STREAM_RANK
FROM tracks t
JOIN daily_track_ranking d ON t.TRACK_ID = d.TRACK_ID
ORDER BY d.DATE_RANK DESC, d.STREAM_RANK ASC
LIMIT 10;

-- Popularity rank vs stream rank 
WITH popularity_ranking AS (
    SELECT
        t.TRACK_ID,
        t.TRACK_NAME,
        AVG(d.POPULARITY) AS AVG_POPULARITY,
        RANK() OVER (ORDER BY AVG(d.POPULARITY) DESC) AS POPULARITY_RANK
    FROM tracks t
    JOIN daily_track_ranking d ON t.TRACK_ID = d.TRACK_ID
    GROUP BY t.TRACK_ID, t.TRACK_NAME
),
stream_ranking AS (
    SELECT
        TRACK_ID,
        STREAM_RANK,
        DATE_RANK
    FROM daily_track_ranking
    WHERE DATE_RANK = (SELECT MAX(DATE_RANK) FROM daily_track_ranking)
)
SELECT
    p.TRACK_ID,
    p.TRACK_NAME,
    p.AVG_POPULARITY,
    p.POPULARITY_RANK,
    s.STREAM_RANK
FROM popularity_ranking p
JOIN stream_ranking s ON p.TRACK_ID = s.TRACK_ID
ORDER BY p.POPULARITY_RANK ASC
LIMIT 10;