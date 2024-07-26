

CREATE TABLE TripPurpose_B06ID(
	TripPurpose_B06ID smallint NOT NULL,
	label varchar(255) NULL,
PRIMARY KEY  
(
	TripPurpose_B06ID ASC
)
);


INSERT INTO TripPurpose_B06ID
           ("TripPurpose_B06ID", "label")
     VALUES
	(-10,	'DEAD'),
	(-8,	'NA'),
	(1,	'Commuting & escort commuting'),
	(2,	'Business & escort business'),
	(3,	'Education & escort education'),
	(4,	'Shopping & escort shopping / personal business'),
	(5,	'Personal business'),
	(6,	'Leisure'),
	(7,	'Holiday / day trip'),
	(8,	'Other including just walk & escort home (not own) / other');


create view nts.trip2 as
select *, 
case 
		when TripPurpose_B01ID = -10 then -10 --DEAD
		when TripPurpose_B01ID = -8 then -8 --NA
		when TripPurpose_B01ID = 1 then 1 --Commuting -> Commuting & escort commuting
		when TripPurpose_B01ID = 2 then 2 --Business -> Business & escort business
		when TripPurpose_B01ID = 3 then 5 --Other work -> Personal business
		when TripPurpose_B01ID = 4 then 3 --Education -> Education & escort education
		when TripPurpose_B01ID = 5 then 4 --Food shopping -> Shopping & escort shopping / personal business
		when TripPurpose_B01ID = 6 then 4 --Non food shopping -> Shopping & escort shopping / personal business
		when TripPurpose_B01ID = 7 then 5 --Personal business medical -> Personal business 
		when TripPurpose_B01ID = 8 then 5 --Personal business eat / drink -> Personal business
		when TripPurpose_B01ID = 9 then 5 --Personal business other -> Personal business 
		when TripPurpose_B01ID = 10 then 6 --Visit friends at private home -> Leisure
		when TripPurpose_B01ID = 11 then 6 --Eat / drink with friends -> Leisure 
		when TripPurpose_B01ID = 12 then 6 --Other social -> Leisure 
		when TripPurpose_B01ID = 13 then 6 --Entertain / public activity -> Leisure 
		when TripPurpose_B01ID = 14 then 6 --Sport: participate -> Leisure 
		when TripPurpose_B01ID = 15 then 7 --Holiday: base -> Holiday / day trip
		when TripPurpose_B01ID = 16 then 7 --Day trip -> Holiday / day trip
		when TripPurpose_B01ID = 17 then 8 --Just walk -> Other including just walk & escort home (not own) / other
		when TripPurpose_B01ID = 18 then 8 --Other non-escort -> Other including just walk & escort home (not own) / other
		when TripPurpose_B01ID = 19 then 1 --Escort commuting -> Commuting & escort commuting
		when TripPurpose_B01ID = 20 then 2 --Escort business & other work -> Business & escort business
		when TripPurpose_B01ID = 21 then 3 --Escort education -> Education & escort education
		when TripPurpose_B01ID = 22 then 4 --Escort shopping / personal business -> Shopping & escort shopping / personal business
		when TripPurpose_B01ID = 23 then 8 --Escort home (not own) & other escort -> Other including just walk & escort home (not own) / other
		else NULL 
		end as TripPurpose_B06ID 
from nts.trip;


