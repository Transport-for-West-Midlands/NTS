/*=================================================================================

	NTS0205 (Household car availability)
		Owen O'Neill:	Jan 2024

  Notes on the various fields available.
  
  If we join the vehicle table to the household table we get different numbers to the Household.numcar etc fields.

	NumCar	= Number of household 3 and 4 wheeled cars (excludes landrover and jeeps) 
	NumCarVan	= Number of household cars or light vans (including landrover, jeep, minibus etc)

  Joining vehicle table to household table results in a count of vehicles that is 0.17% higher for England than the value in Household.numcar
  
  not all adults in the household participate in the survey ?
  so joining the individual table to the household table results in a smaller number of adults than the Household.HHoldNumAdults
  difference is 1.3-1.6% for all of England.

  households that did not answer the question (household.NumCarVan_B02ID = -8) are excluded.
  a tiny number of those households do have entries in the vehicle table

=================================================================================*/
--use NTS;

DO $$
DECLARE

_numyears constant smallint = 1; --number of years to roll up averages (backwards from date reported in result row)

_statsregID constant  smallint = 0; --set to zero for all regions west midlands=10

_weekToYearCorrectionFactor constant  float = 52.14; -- ((365.0*4.0)+1.0)/4.0/7.0; 
--diary is for 1 week - need to multiply by a suitable factor to get yearly trip rate
--365/7 appears wrong - to include leap years we should use (365*4+1)/4/7
--documentation further rounds this to 52.14, so to get the closest possible match to published national values use 52.14 (even though it's wrong) 	

BEGIN

DROP TABLE IF EXISTS __temp_table;

CREATE TEMP TABLE __temp_table AS

with 


cteLabels (yearID, yearDesc,
			countryID, StatsRegID, StatsRegDesc) 
as
(select distinct psu.SurveyYear_B01ID, psu.SurveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
 			 WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psu.PSUStatsReg_B01ID, statsRegLookup.description
from 
	tfwm_ntsdata.psu psu
	left outer join 
	tfwm_ntslookups.PSUStatsReg_B01ID as statsRegLookup
	on psu.PSUStatsReg_B01ID = statsRegLookup.PSUStatsReg_B01ID
),


cteCountryLabels (yearID, yearDesc,
			countryID, countryDesc) 
as
(select distinct psu.SurveyYear_B01ID, psu.SurveyYear,
		CASE WHEN psu.psucountry_b01id = -10 THEN 1
 			WHEN psu.psucountry_b01id isnull THEN 1
			 ELSE psu.psucountry_b01id
		END,
		countryLookup.description
from 
	tfwm_ntsdata.psu psu
	left outer join 
	tfwm_ntslookups.PSUCountry_B01ID as countryLookup
	on CASE WHEN psu.psucountry_b01id = -10 THEN 1
 			WHEN psu.psucountry_b01id isnull THEN 1
			 ELSE psu.psucountry_b01id
		END = countryLookup.PSUCountry_B01ID
),


--W0	Unweighted interview sample(Household)
--W3	Interview sample household weight (Household)
--W1	Unweighted diary sample(Household)
--W2	Diary sample household weight (Household)
--W5	Trip/Stage weight (Trip)
--W4	LDJ weight (LDJ)
--W6	Attitudes weight(Attitudes)

cteHouseholds (yearID, countryID, statsregID, 
	households_unweighted, households_weighted,
	hholdnumadults_unweighted, hholdnumadults_weighted, 
	numcarvan_unweighted, numcarvan_weighted, 
	unweighted_adultParticipantCount, weighted_adultParticipantCount,
	unweighted_vehicleCount, weighted_vehicleCount,
 Hcar0,
 Hcar1,
 Hcar2)
as
(select SurveyYear_B01ID, 
	CASE WHEN psucountry_b01id = -10 THEN 1
 		WHEN psucountry_b01id isnull THEN 1
		 ELSE psucountry_b01id
	END,
	psustatsreg_b01id,
 	SUM(W0), 							SUM(W3),
 	sum(W0*H.hholdnumadults), 			sum(W3*H.hholdnumadults), 
 	sum(W0*H.numcarvan), 				sum(W3*H.numcarvan), 	
 	SUM(W0*I."adultParticipantCount"),  SUM(W3*I."adultParticipantCount"),
 	SUM(W0*cv."carOrVanCount"),			SUM(W3*cv."carOrVanCount"),
 
/* 	sum(CASE WHEN 0=cv."carOrVanCount" THEN W3
			WHEN cv."carOrVanCount" IS NULL THEN W3
			 ELSE NULL
		END) as car0,
 	sum(CASE WHEN 1=cv."carOrVanCount" THEN W3
			 ELSE NULL
		END) as car1,
 	sum(CASE WHEN 2<=cv."carOrVanCount" THEN W3
			 ELSE NULL
		END) as car2,*/
 
 	sum(CASE WHEN 1=H.NumCarVan_B02ID THEN W3
			 ELSE NULL
		END) as Hcar0,
 	sum(CASE WHEN 2=H.NumCarVan_B02ID THEN W3
			 ELSE NULL
		END) as Hcar1,
 	sum(CASE WHEN 3=H.NumCarVan_B02ID THEN W3
			 ELSE NULL
		END) as Hcar2
 
from 
tfwm_ntsdata.Household as H

left join
tfwm_ntsdata.PSU as P
on H.PSUID = P.PSUID

left join
( select HouseholdID, count(*) "adultParticipantCount" from tfwm_ntsdata.individual 
 where Age_B04ID >= 4 --17 years old and over
 group by HouseholdID ) I
on I.HouseholdID = H.HouseholdID

left join
( select HouseholdID, count(*) "carOrVanCount" from tfwm_ntsdata.vehicle 
 where VehType_B03ID in (1,3,4) --car, landrover/jeep, light van
 group by HouseholdID ) CV
on CV.HouseholdID = H.HouseholdID
 
where 
 H.NumCarVan_B02ID > 0 --exclude households that did not answer the question. 
 
group by SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id
),


cteXyrsHouseholds(yearID, countryID, statsregID, 
	households_unweighted, households_weighted,
	hholdnumadults_unweighted, hholdnumadults_weighted, 
	numcarvan_unweighted, numcarvan_weighted, 
	unweighted_adultParticipantCount, weighted_adultParticipantCount,
	unweighted_vehicleCount, weighted_vehicleCount,
 Hcar0,
 Hcar1,
 Hcar2)
as
(select sy.SurveyYear_B01ID, h.countryID, h.statsregID, 

	sum(households_unweighted), sum(households_weighted),
	sum(hholdnumadults_unweighted), sum(hholdnumadults_weighted), 
	sum(numcarvan_unweighted), sum(numcarvan_weighted), 
	sum(unweighted_adultParticipantCount), sum(weighted_adultParticipantCount),
	sum(unweighted_vehicleCount), sum(weighted_vehicleCount),			  
 
 sum(Hcar0),
 sum(Hcar1),
 sum(Hcar2) 
from 
	tfwm_ntslookups.SurveyYear_B01ID sy
	left join 
	cteHouseholds as H
		on sy.SurveyYear_B01ID -_numyears < H.yearID and sy.SurveyYear_B01ID >= H.yearID
group by sy.SurveyYear_B01ID, countryID, statsregID
),


cteXyrsHouseholdsAllRegions(yearID, countryID, 
	households_unweighted, households_weighted,
	hholdnumadults_unweighted, hholdnumadults_weighted, 
	numcarvan_unweighted, numcarvan_weighted, 
	unweighted_adultParticipantCount, weighted_adultParticipantCount,
	unweighted_vehicleCount, weighted_vehicleCount,
 Hcar0,
 Hcar1,
 Hcar2			 		   
						   )
as
(select yearID, countryID, 
	sum(households_unweighted), sum(households_weighted),
	sum(hholdnumadults_unweighted), sum(hholdnumadults_weighted), 
	sum(numcarvan_unweighted), sum(numcarvan_weighted), 
	sum(unweighted_adultParticipantCount), sum(weighted_adultParticipantCount),
	sum(unweighted_vehicleCount), sum(weighted_vehicleCount),			  
 sum(Hcar0),
 sum(Hcar1),
 sum(Hcar2)  
from 
	cteXyrsHouseholds
group by yearID, countryID
)


-- select query
select  
yearDesc-_numyears+1 "start year", 
yearDesc "end year", 
StatsRegDesc "region",

	households_unweighted,	households_weighted,

	numcarvan_weighted/households_weighted "cars or vans per household (0205b)",
	
	numcarvan_weighted/weighted_adultParticipantCount "cars or vans per adult (aged 17 and over) (0205b)", 

 Hcar0/households_weighted*100.0 "No car or van (%) (0205a)",
 Hcar1/households_weighted*100.0 "One car or van (%) (0205a)",
 Hcar2/households_weighted*100.0 "Two+ car or van (%) (0205a)"
/*
	hholdnumadults_unweighted, unweighted_adultParticipantCount,
	hholdnumadults_weighted, weighted_adultParticipantCount,
	numcarvan_unweighted, unweighted_vehicleCount,
	numcarvan_weighted, weighted_vehicleCount
*/
from 
	cteLabels as L
	left join
	cteXyrsHouseholds as I
		on L.yearID = I.yearID
		and L.countryID = I.countryID
		and L.StatsRegID = I.statsregID

	cross join
	(select min(SurveyYear) "year" from tfwm_ntsdata.psu) minYear
where 
	L.yearDesc + 1 >= minYear.year + _numyears
	and
	(L.statsregID=_statsregID or L.statsregID is null or 0=_statsregID)

union 

select 
yearDesc-_numyears+1 "start year", 
yearDesc "end year", 
CountryDesc "country",

	households_unweighted,	households_weighted,

	numcarvan_weighted/households_weighted "cars or vans per household (0205b)",
	
	numcarvan_weighted/weighted_adultParticipantCount "cars or vans per adult (aged 17 and over) (0205b)", 

 Hcar0/households_weighted*100.0 "No car or van (%) (0205a)",
 Hcar1/households_weighted*100.0 "One car or van (%) (0205a)",
 Hcar2/households_weighted*100.0 "Two+ car or van (%) (0205a)"
/*
	hholdnumadults_unweighted, unweighted_adultParticipantCount,
	hholdnumadults_weighted, weighted_adultParticipantCount,
	numcarvan_unweighted, unweighted_vehicleCount,
	numcarvan_weighted, weighted_vehicleCount
*/
from 
	cteCountryLabels as L
	left join
	cteXyrsHouseholdsAllRegions as I
		on L.yearID = I.yearID
		and L.countryID = I.countryID

	cross join
	(select min(SurveyYear) "year" from tfwm_ntsdata.psu) minYear
where 
	L.yearDesc + 1 >= minYear.year + _numyears

order by 1,2; 

end;
$$;
 
select * from __temp_table;
 
--can't drop the temp table here otherwise I don't get any output from the select statement in the pgadmin window
