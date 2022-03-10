'''
Get Data: https://www.imdb.com/interfaces/
Data used:
- title_data: title.basics.tsv.gz
- name_data: name.basics.tsv.gz
- ratings_data: title.ratings.tsv.gz

Questions to be answered:
1) Create the nessesery tables and import Data 
2) Implement the sql queries below: 
 A) Find all directors(not including assistant directors etc.) that where born in 1939 and show their names.
 B) Find all Thriller productions (movies, tvseries, etc.) that have the best ratings and at least 1 million reviews. Show titles and rating.
 C) Find 10 longest running tvseries with at least 100.000 reviews. Show titles, their "age" in descending order and if they are still on air.
 D) Find actors (regardless gender) with at least 4 productions that are known for. Calculate thir average rating for the productions that have at least 1.5 million
 reviews. Show their names and avg rating in descending order for those with avg rating bigger than 9.
3) Query optimization with indexes.

'''
#1)
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


#2.A)

select primaryname from name_data 
where birthyear=1939
and 
(primaryprofession like '%,director%'
or 
primaryprofession like 'director%')


#2.B)

select primarytitle, averagerating from title
inner join ratings
on title.tconst=ratings.tconst
where genres like '%Thriller%' and numvotes>=1000000
order by averagerating desc

#2.C)

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

#2.D)

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

#3)
A) create INDEX idx_birthyear ON name_data(birthyear)
B) create INDEX idx_numvotes ON ratings(numvotes)
C) create INDEX idx_numvotes ON ratings(numvotes)
D) create INDEX idx_numvotes ON ratings(numvotes)

