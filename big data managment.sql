Πίνακες:

SET session_replication_role TO replica
SET session_replication_role TO 'origin'

CREATE TABLE title (
	tconst varchar,
	titleType char(255),
	primaryTitle varchar,
	originalTitle varchar,
	isAdult bool,
	startYear smallint,
	endYear smallint,
	runtimeMinutes int,
	genres char(255),
	primary key(tconst)
)

COPY title(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) 
FROM 'E:\title_data.tsv'
with DELIMITER E'\t' NULL '\N' QUOTE E'\b' csv header

CREATE TABLE ratings (
	tconst varchar,
	averageRating numeric,
	numVotes int,
	foreign key(tconst) references title(tconst)
)

COPY ratings(tconst, averageRating, numVotes) 
FROM 'E:\ratings_data.tsv'
with DELIMITER E'\t' NULL '\N' csv header

CREATE TABLE name_data (
	nconst varchar,
	primaryName	varchar,
	birthYear smallint,
	deathYear smallint,
	primaryProfession char(255),
	knownForTitles char(255),
	PRIMARY KEY(nconst),
	foreign key(knownfortitles) references title(tconst)
)

COPY name_data(nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles) 
FROM 'E:\name_data.tsv'
with DELIMITER E'\t' NULL '\N' csv header

A).....................................
create INDEX idx_pro_birth
ON name_data(primaryprofession,birthyear)

EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)
select primaryname from name_data 
where birthyear=1939
and 
(primaryprofession like '%,director%'
or 
primaryprofession like 'director%')

553

B)..........................................

EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)
select primarytitle, averagerating from title
inner join ratings
on title.tconst=ratings.tconst
where genres like '%Thriller%' and numvotes>=1000000
order by averagerating desc

create INDEX idx_numvotes
ON ratings(numvotes)

5
C).............................................

create INDEX idx_numvotes
ON ratings(numvotes)

EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)
select primarytitle,
(coalesce(endyear,2021) - startyear)as age, 
case
	when endyear isnull 
	then 'On air' else 'Ended'
End Status
from title
inner join ratings
on title.tconst=ratings.tconst
where titletype='tvSeries' and numvotes>=100000
order by age desc
limit 10

d).................................................

000000000000000000000000000

SELECT pg_size_pretty(PG_RELATION_SIZE('""'))
000000000000000000000000000000

select actors.primaryname,avg(ratings.averagerating) as avg_all
from actors
inner join ratings on actors.tconst=ratings.tconst
where ratings.numvotes>=1500000 and actors.num_of_titles>=4 
group by actors.primaryname
having avg(ratings.averagerating) > 9
order by avg_all desc 

111111111111111111111111111111111111
select actors.primaryname,round(avg(ratings.averagerating),2) as avg_all from
(select nconst, primaryname,
array_length(string_to_array(knownfortitles,','),1) as num_of_titles,
unnest(string_to_array(knownfortitles, ',')) as tconst
from name_data where primaryprofession like '%act%' order by nconst) actors
inner join ratings on actors.tconst=ratings.tconst
where ratings.numvotes>=1500000 and actors.num_of_titles>=4 
group by actors.primaryname
having avg(ratings.averagerating) > 9
order by avg_all desc 

855
