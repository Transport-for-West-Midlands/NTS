/*=================================================================================

variant on NTS0908 (Where vehicle parked overnight) - but broken down by region instead of rural/urban classification 
	question in asked in even numbered years only

	not asked from 2004-2006 ?

	Owen O'Neill:	November 2023
	Owen O'Neill:   June 2024 - use special access schema - break down by LA

=================================================================================*/


DO $$
DECLARE

_numyears constant smallint = 16; --number of years to roll up averages (backwards from date reported in result row)

_statsregID constant  smallint = 10; --set to zero for all regions west midlands=10
									--if non-zero generates LA level results as well.

BEGIN

DROP TABLE IF EXISTS __temp_table;

CREATE TEMP TABLE __temp_table AS

with 

cteLabels (yearID, yearDesc,
			countryID, StatsRegID, StatsRegDesc,
			vlID, vlDesc) 
as
(select distinct psu.SurveyYear_B01ID, psu.SurveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
 			 WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psu.PSUStatsReg_B01ID, statsRegLookup.description,
		vl.VehParkLoc_B01ID, vl.description
from 
	tfwm_nts_secureschema.psu psu
	left outer join 
	tfwm_nts_securelookups.PSUStatsReg_B01ID as statsRegLookup
	on psu.PSUStatsReg_B01ID = statsRegLookup.PSUStatsReg_B01ID
	cross join
 	tfwm_nts_securelookups.VehParkLoc_B01ID vl
),


cteCountryLabels (yearID, yearDesc,
			countryID, countryDesc,
			vlID, vlDesc) 
as
(select distinct psu.SurveyYear_B01ID, psu.SurveyYear,
		CASE WHEN psu.psucountry_b01id = -10 THEN 1
 			WHEN psu.psucountry_b01id isnull THEN 1
			 ELSE psu.psucountry_b01id
		END,
		countryLookup.description,
		vl.VehParkLoc_B01ID, vl.description
from 
	tfwm_nts_secureschema.psu psu
	left outer join 
	tfwm_nts_securelookups.PSUCountry_B01ID as countryLookup
	on CASE WHEN psu.psucountry_b01id = -10 THEN 1
 			WHEN psu.psucountry_b01id isnull THEN 1
			 ELSE psu.psucountry_b01id
		END = countryLookup.PSUCountry_B01ID
	cross join
 	tfwm_nts_securelookups.VehParkLoc_B01ID vl
),


--this table is one of the view lookups with a VARCHAR id, that the currently load process doesn't cope with.
lookup_HHoldOSLAUA_B01ID ( ID, description )
as
(
select 'E08000025','Birmingham'
	union
select 'E08000026','Coventry'
	union
select 'E08000027','Dudley'
	union
select 'E08000028','Sandwell'
	union
select 'E08000029','Solihull'
	union
select 'E08000030','Walsall'
	union
select 'E08000031','Wolverhampton'	
),

 	
cteLaLabels (yearID, yearDesc,
			LaID, LaDesc,
			vlID, vlDesc) 
as
(select distinct psu.SurveyYear_B01ID, psu.SurveyYear,
		laLookup.Id,
		laLookup.description description,
		vl.VehParkLoc_B01ID, vl.description
from 
	tfwm_nts_secureschema.psu psu
	cross join
	lookup_HHoldOSLAUA_B01ID laLookup
	cross join
 	tfwm_nts_securelookups.VehParkLoc_B01ID vl
),


--W0	Unweighted interview sample(Household)
--W3	Interview sample household weight (Household)
--W1	Unweighted diary sample(Household)
--W2	Diary sample household weight (Household)
--W5	Trip/Stage weight (Trip)
--W4	LDJ weight (LDJ)
--W6	Attitudes weight(Attitudes)
cteVehicles (yearID, countryID, statsregID, vlID, Vehicles_unweighted, Vehicles_weighted)
as
(select SurveyYear_B01ID, 
	CASE WHEN psucountry_b01id = -10 THEN 1
 		WHEN psucountry_b01id isnull THEN 1
		 ELSE psucountry_b01id
	END,
	psustatsreg_b01id, 
 	VehParkLoc_B01ID, 
 	SUM(W1), SUM(W2)
from 
tfwm_nts_secureschema.vehicle V
left join
tfwm_nts_secureschema.PSU as P
on V.PSUID = P.PSUID
left join
tfwm_nts_secureschema.Household as H
on V.HouseholdID = H.HouseholdID
group by SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id,
 		VehParkLoc_B01ID
),


cteLaVehicles (yearID, laID, vlID, Vehicles_unweighted, Vehicles_weighted)
as
(select SurveyYear_B01ID, 
	HHoldOSLAUA_B01ID, 
 	VehParkLoc_B01ID, 
 	SUM(W1), SUM(W2)
from 
tfwm_nts_secureschema.vehicle V
left join
tfwm_nts_secureschema.PSU as P
on V.PSUID = P.PSUID
left join
tfwm_nts_secureschema.Household as H
on V.HouseholdID = H.HouseholdID
group by SurveyYear_B01ID, 
		HHoldOSLAUA_B01ID,
 		VehParkLoc_B01ID
),


cteXyrsVehicles(yearID, countryID, statsregID, vlID, Vehicles_unweighted, Vehicles_weighted)
as
(select sy.SurveyYear_B01ID, v.countryID, v.statsregID, v.vlID, sum(V.Vehicles_unweighted), sum(V.Vehicles_weighted)
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
	left join 
	cteVehicles as V
		on sy.SurveyYear_B01ID -_numyears < v.yearID and sy.SurveyYear_B01ID >= v.yearID
where 0 = mod(v.yearID, 2) --only asked in even years
group by sy.SurveyYear_B01ID, countryID, statsregID, vlID
),

cteLaXyrsVehicles(yearID, laID, vlID, Vehicles_unweighted, Vehicles_weighted)
as
(select sy.SurveyYear_B01ID, v.laID, v.vlID, sum(V.Vehicles_unweighted), sum(V.Vehicles_weighted)
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
	left join 
	cteLaVehicles as V
		on sy.SurveyYear_B01ID -_numyears < v.yearID and sy.SurveyYear_B01ID >= v.yearID
where 0 = mod(v.yearID, 2) --only asked in even years
group by sy.SurveyYear_B01ID, laID, vlID
),


cteXyrsVehiclesAllRegions(yearID, countryID, vlID, Vehicles_unweighted, Vehicles_weighted)
as
(select yearID, countryID, vlID, sum(Vehicles_unweighted), sum(Vehicles_weighted)
from 
	cteXyrsVehicles
group by yearID, countryID, vlID
)



-- select query
select
yearDesc-_numyears+1 "start year", 
yearDesc "end year", 
StatsRegDesc "region",
vlDesc "location",
	Vehicles_unweighted as Vehicles_UNweighted,
	round(cast(Vehicles_weighted as numeric),1) as Vehicles_weighted

from 
	cteLabels as L
	left join
	cteXyrsVehicles as V
		on L.yearID = V.yearID
		and L.countryID = V.countryID
		and L.StatsRegID = V.statsregID
		and L.vlID = V.vlID

	cross join
	(select min(SurveyYear) "year" from tfwm_nts_secureschema.psu) minYear
where 
	L.yearDesc + 1 >= minYear.year + _numyears
	--and 0 = mod(yearDesc, 2)
	and
	(L.statsregID=_statsregID or L.statsregID is null or 0=_statsregID)

union 

select 
yearDesc-_numyears+1 "start year", 
yearDesc "end year", 
CountryDesc "country",
vlDesc "location",
	Vehicles_unweighted as Vehicles_UNweighted,
	Vehicles_weighted as Vehicles_weighted

from 
	cteCountryLabels as L
	left join
	cteXyrsVehiclesAllRegions as V
		on L.yearID = V.yearID
		and L.countryID = V.countryID
		and L.vlID = V.vlID

	cross join
	(select min(SurveyYear) "year" from tfwm_nts_secureschema.psu) minYear
where 
	L.yearDesc + 1 >= minYear.year + _numyears
	--and 0 = mod(yearDesc, 2)

union

select
yearDesc-_numyears+1 "start year", 
yearDesc "end year", 
laDesc "region",
vlDesc "location",
	Vehicles_unweighted as Vehicles_UNweighted,
	round(cast(Vehicles_weighted as numeric),1) as Vehicles_weighted

from 
	cteLaLabels as L
	left join
	cteLaXyrsVehicles as V
		on L.yearID = V.yearID
		and L.laID = V.laID
		and L.vlID = V.vlID

	cross join
	(select min(SurveyYear) "year" from tfwm_nts_secureschema.psu) minYear
where 
	L.yearDesc + 1 >= minYear.year + _numyears
	--and 0 = mod(yearDesc, 2)
	and
	(0!=_statsregID)


order by 1,2,3;

end;
$$;
 
select * from __temp_table;
 
--can't drop the temp table here otherwise I don't get any output from the select statement in the pgadmin window
