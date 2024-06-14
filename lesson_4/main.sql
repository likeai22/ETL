SET schema 'public';

do
$$
declare
table_name text;

begin
for table_name in
select tablename
from pg_tables
where schemaname = 'public' loop
        execute 'DROP TABLE IF EXISTS public.' || quote_ident(table_name) || ' CASCADE';
end loop;
end
$$;


--1. Создайте таблицу movies с полями movies_type, director, year_of_issue, length_in_minutes, rate.
create table movies
(
    movies_type       character varying not null,
    director          character varying not null,
    year_of_issue     smallint          not null,
    length_in_minutes smallint          not null,
    rate              real              not null
);

--2. Сделайте таблицы для горизонтального партицирования по году выпуска (до 1990, 1990 -2000, 2000- 2010, 2010-2020,
-- после 2020)

create table movies_year_of_issue_1990
(
    check (year_of_issue <= 1990)
) inherits(movies);

create table movies_year_of_issue_1990_2000
(
    check (year_of_issue > 1990 and year_of_issue <= 2000)
) inherits(movies);

create table movies_year_of_issue_2000_2010
(
    check (year_of_issue > 2000 and year_of_issue <= 2010)
) inherits(movies);

create table movies_year_of_issue_2010_2020
(
    check (year_of_issue > 2010 and year_of_issue <= 2020)
) inherits(movies);

create table movies_year_of_issue_2020
(
    check (year_of_issue > 2020)
) inherits(movies);

--3. Сделайте таблицы для горизонтального партицирования по длине фильма (до 40 минута, от 40 до 90 минут,
-- от 90 до 130 минут, более 130 минут).

create table movies_length_in_minutes_40
(
    check (length_in_minutes <= 40)
) inherits(movies);

create table movies_length_in_minutes_40_90
(
    check (length_in_minutes > 40 and length_in_minutes <= 90)
) inherits(movies);

create table movies_length_in_minutes_90_130
(
    check (length_in_minutes > 90 and length_in_minutes <= 130)
) inherits(movies);

create table movies_length_in_minutes_130
(
    check (length_in_minutes > 130)
) inherits(movies);

--4. Сделайте таблицы для горизонтального партицирования по рейтингу фильма (ниже 5, от 5 до 8, от 8до 10).

create table movies_rate_5
(
    check (rate <= 5)
) inherits(movies);

create table movies_rate_5_8
(
    check (rate > 5 and rate <= 8)
) inherits(movies);

create table movies_rate_8_10
(
    check (rate > 8 and rate <= 10)
) inherits(movies);

--5. Создайте правила добавления данных для каждой таблицы.

--year_of_issue
create
rule movies_insert_year_of_issue_1990 as on insert to movies
where(year_of_issue <= 1990)
do instead insert into movies_year_of_issue_1990 values(new.*);

create
rule movies_insert_movies_year_of_issue_1990_2000 as on insert to movies
where(year_of_issue > 1990 and year_of_issue <= 2000)
do instead insert into movies_year_of_issue_1990_2000 values(new.*);

create
rule movies_insert_movies_year_of_issue_2000_2010 as on insert to movies
where(year_of_issue > 2000 and year_of_issue <= 2010)
do instead insert into movies_year_of_issue_2000_2010 values(new.*);

create
rule movies_insert_movies_year_of_issue_2010_2020 as on insert to movies
where(year_of_issue > 2010 and year_of_issue <= 2020)
do instead insert into movies_year_of_issue_2010_2020 values(new.*);

create
rule movies_insert_movies_year_of_issue_2020 as on insert to movies
where(year_of_issue > 2020)
do instead insert into movies_year_of_issue_2020 values(new.*);

--length_in_minutes
create
rule movies_insert_length_in_minutes_40 as on insert to movies
where(length_in_minutes <= 40)
do instead insert into movies_length_in_minutes_40 values(new.*);

create
rule movies_insert_length_in_minutes_40_90 as on insert to movies
where(length_in_minutes > 40 and length_in_minutes <= 90)
do instead insert into movies_length_in_minutes_40_90 values(new.*);

create
rule movies_insert_length_in_minutes_90_130 as on insert to movies
where(length_in_minutes > 90 and length_in_minutes <= 130)
do instead insert into movies_length_in_minutes_90_130 values(new.*);

create
rule movies_insert_length_in_minutes_130 as on insert to movies
where(length_in_minutes > 130)
do instead insert into movies_length_in_minutes_130 values(new.*);

--rate
create
rule movies_insert_rate_5 as on insert to movies
where(rate <= 5)
do instead insert into movies_rate_5 values(new.*);

create
rule movies_insert_rate_5_8 as on insert to movies
where(rate > 5 and rate <= 8)
do instead insert into movies_rate_5_8 values(new.*);

create
rule movies_insert_rate_8_10 as on insert to movies
where(rate > 8 and rate <= 10)
do instead insert into movies_rate_8_10 values(new.*);

--6. Добавьте фильмы так, чтобы в каждой таблице было не менее 3 фильмов.
CREATE OR REPLACE FUNCTION generate_random_movies() RETURNS void AS $$
DECLARE
movie_types VARCHAR[] := ARRAY['Drama', 'Comedy', 'Action', 'Horror', 'Sci-Fi'];
    directors VARCHAR[] := ARRAY['Director A', 'Director B', 'Director C', 'Director D', 'Director E'];
    year_ranges INT[][] := ARRAY[
        ARRAY[1970, 1990],
        ARRAY[1991, 2000],
        ARRAY[2001, 2010],
        ARRAY[2011, 2020],
        ARRAY[2021, 2024]
    ];
    length_ranges INT[][] := ARRAY[
        ARRAY[10, 40],
        ARRAY[41, 90],
        ARRAY[91, 130],
        ARRAY[131, 200]
    ];
    rate_ranges REAL[][] := ARRAY[
        ARRAY[0, 5],
        ARRAY[5.1, 8],
        ARRAY[8.1, 10]
    ];
    i INT;
    j INT;
    k INT;
    m INT;
    director VARCHAR;
    movie_type VARCHAR;
    year_start INT;
    year_end INT;
    length_start INT;
    length_end INT;
    rate_start REAL;
    rate_end REAL;
    year_of_issue INT;
    length_in_minutes INT;
    rate REAL;
BEGIN
FOR i IN 1..array_length(year_ranges, 1) LOOP
        year_start := year_ranges[i][1];
        year_end := year_ranges[i][2];
FOR j IN 1..array_length(length_ranges, 1) LOOP
            length_start := length_ranges[j][1];
            length_end := length_ranges[j][2];
FOR k IN 1..array_length(rate_ranges, 1) LOOP
                rate_start := rate_ranges[k][1];
                rate_end := rate_ranges[k][2];
FOR m IN 1..3 LOOP
                    director := directors[ceil(random() * array_length(directors, 1))];
                    movie_type := movie_types[ceil(random() * array_length(movie_types, 1))];
                    year_of_issue := year_start + floor(random() * (year_end - year_start + 1));
                    length_in_minutes := length_start + floor(random() * (length_end - length_start + 1));
                    rate := rate_start + random() * (rate_end - rate_start);

INSERT INTO movies (movies_type, director, year_of_issue, length_in_minutes, rate)
VALUES (movie_type, director, year_of_issue, length_in_minutes, rate);
END LOOP;
END LOOP;
END LOOP;
END LOOP;
END;
$$ LANGUAGE plpgsql;

--7. Добавьте пару фильмов с рейтингом выше 10.
INSERT INTO movies (movies_type, director, year_of_issue, length_in_minutes, rate)
VALUES ('Sci-Fi', 'Director A', 2024, 120, 11);
INSERT INTO movies (movies_type, director, year_of_issue, length_in_minutes, rate)
VALUES ('Action', 'Director B', 2024, 52, 15);

--8. Сделайте выбор из всех таблиц, в том числе из основной.
--Если структура таблиц неизвестна или может изменяться, JSON может быть хорошим вариантом для объединения данных из таблиц с разной структурой

CREATE TEMP TABLE temp_all_data (
    table_name VARCHAR,
    row_data JSONB
);

CREATE OR REPLACE FUNCTION populate_temp_all_data() RETURNS void AS $$
DECLARE
table_name text;
    query text;
BEGIN
FOR table_name IN
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
    LOOP
        query := 'INSERT INTO temp_all_data (table_name, row_data)
                  SELECT ''' || table_name || ''', row_to_json(t) FROM public.' || quote_ident(table_name) || ' t';
EXECUTE query;
END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT populate_temp_all_data();

SELECT * FROM temp_all_data;

--так как структура у таблиц одинаковая, сделаем select во временную таблицу с идентичной основной таблице структурой

CREATE TEMP TABLE temp_all_movies (
    movies_type character varying NOT NULL,
    director character varying NOT NULL,
    year_of_issue smallint NOT NULL,
    length_in_minutes smallint NOT NULL,
    rate real NOT NULL
);

CREATE OR REPLACE FUNCTION populate_temp_all_movies() RETURNS void AS $$
DECLARE
table_name TEXT;
    query TEXT;
BEGIN
FOR table_name IN
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
    LOOP
        query := 'INSERT INTO temp_all_movies (movies_type, director, year_of_issue, length_in_minutes, rate)
                  SELECT movies_type, director, year_of_issue, length_in_minutes, rate
                  FROM public.' || quote_ident(table_name);
EXECUTE query;
END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT populate_temp_all_movies();

SELECT * FROM temp_all_movies;

--9. Сделайте выбор только из основной таблицы.
SELECT * FROM movies;




