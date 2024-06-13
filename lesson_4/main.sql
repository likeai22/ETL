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
where(length_in_minutes <= 5)
do instead insert into movies_rate_5 values(new.*);

create
rule movies_insert_rate_5_8 as on insert to movies
where(rate > 5 and rate <= 8)
do instead insert into movies_rate_5_8 values(new.*);

create
rule movies_insert_rate_8_10 as on insert to movies
where(rate > 8 and rate <= 10)
do instead insert into movies_rate_8_10 values(new.*);

--in progress
--6. Добавьте фильмы так, чтобы в каждой таблице было не менее 3 фильмов.


--7. Добавьте пару фильмов с рейтингом выше 10.


--8. Сделайте выбор из всех таблиц, в том числе из основной.


--9. Сделайте выбор только из основной таблицы.



