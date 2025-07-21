SELECT
    game_id AS game_key,
    game_title,
    genre,
    developer,
    release_date
FROM {{ source('gog_raw', 'game_metadata') }}
