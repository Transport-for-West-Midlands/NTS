/*=================================================================================

NTS0409 (Average number of trips and distance travelled by purpose and main mode)
	Owen O'Neill:	Jan 2024
	Owen O'Neill:	June 2024 use special access schema - adapt to LA geography
    Owen O'Neill:   June 2024 add functionality to skip covid years while keeping number of active years in rollling average the same.
	Owen O'Neill:   April 2025 merged in a bunch of fixes from nts0303 query, added ability to switch between grouping by different trip purposes
						- validated output against published 2023 data.

=================================================================================*/
--use NTS;

DO $$
DECLARE

_numyears constant smallint = 5; --number of years to roll up averages (backwards from date reported in result row)

_skipCovidYears constant smallint = 1; --if enabled skips 2020 + 2021 and extends year window to compensate so number of years aggregated remains the same.

_generateLaResults constant  smallint = 0;	--if non-zero generates LA level results as well.

_statsregID constant  smallint = 0; --set to zero for all regions west midlands=10
									--if non-zero generates LA level results as well.							

_groupByTripPurposeSetting constant smallint = 4;
-- _groupByTripPurposeSetting = 1 (groups by TripPurpose_B01ID)
-- _groupByTripPurposeSetting = 2 (groups by TripPurpose_B02ID)
-- _groupByTripPurposeSetting = 4 (groups by TripPurpose_B04ID)
-- _groupByTripPurposeSetting = 6 (groups by TripPurpose_B06ID)

--the published NTS0403 table uses TripPurpose_B02ID as groupings, but this isn't always very useful (too many small categories).
--other published tables use TripPurpose_B06ID, which is very, very similar to TripPurpose_B04ID, but groups escorting differently.
--TripPurpose_B06ID has been reverse engineered into a bunch of case statements since it's only directly available in more disclosive datasets than we have available.



_combineLocalBusModes  constant smallint = 1; --captured data segregates london bus and other local buses. We need this to compare with national results 
										 -- but want to combine them for our analysis. Use this to switch it on/off 

_combineUndergroundIntoOther  constant smallint = 1; --captured data segregates london underground. For other regions the tram/metro service goes into the 'other PT' category.
										--We need this to compare with national results but want to combine them for our analysis. Use this to switch it on/off 

_excludeShortWalks constant smallint = 0; --table 0403a+c includes short walks, table 0403b+d excludes short walks

_restrictToWorkingAge  constant smallint = 0; --restrict to 16-64 year old (inclusive) 
--age_b01id >= 6 AND age_b01id <= 16 -- 16-64 year old (inclusive) 



_dummyModeIdValue constant  float = 1.5; --walks are split to 'long' walks and all walks - we use this dummy value for the additional 'long walk' category.

_weekToYearCorrectionFactor constant  float = 52.14; -- ((365.0*4.0)+1.0)/4.0/7.0; 
--diary is for 1 week - need to multiply by a suitable factor to get yearly trip rate
--365/7 appears wrong - to include leap years we should use (365*4+1)/4/7
--documentation further rounds this to 52.14, so to get the closest possible match to published national values use 52.14 (even though it's wrong) 	



/*
--there are local authorities that are in multiple StatsRegions
--e.g. E06000057 (Northumberland) has parts of it in StatsRegion 1&2 (Northern, Metropolitan & Northern, Non-metropolitan)
--this means we can't simply aggregate the LA numbers in to stats region numbers.

with las as
(
select distinct h.HHoldOSLAUA_B01ID laCode, psu.psustatsreg_b01id
from tfwm_nts_secureschema.household h
left outer join tfwm_nts_secureschema.psu psu
on h.psuid=psu.psuid
	where h.surveyyear = 2022
)

select laCode, count(*) from las
group by laCode
having count(*) != 1
*/

BEGIN

DROP TABLE IF EXISTS __temp_table;

CREATE TEMP TABLE __temp_table AS

with 

cteTripPurpose_B06ID ( TripPurpose_B06ID, description )
AS
(--SELECT -10,'DEAD' UNION ALL 
 SELECT -8,	'NA'
 UNION ALL SELECT 1,	'Commuting & escort commuting'
 UNION ALL SELECT 2,	'Business & escort business'
 UNION ALL SELECT 3,	'Education & escort education'
 UNION ALL SELECT 4,	'Shopping & escort shopping / personal business'
 UNION ALL SELECT 5,	'Personal business'
 UNION ALL SELECT 6,	'Leisure'
 UNION ALL SELECT 7,	'Holiday / day trip'
 UNION ALL SELECT 8,	'Other including just walk & escort home (not own) / other'
),


cteCovidYears( minCovid, maxCovid, minCovidId, maxCovidId )
as
(
	select 2020, 2021, 26, 27 	
),

--doing this inline get complicated when there are some modes where there is no data for a particular mode in a given years, 
--so generate it seperately and join in later.
ctaFromToYearsId( fromYearId, toYearId, toYear )
as
(
SELECT
L.SurveyYear_B01ID + 1 - 
CASE WHEN _skipCovidYears!=1 THEN _numyears
WHEN cast(L.description as int) -_numyears+1 <= cy.maxCovid AND cast(L.description as int) >= cy.minCovid
THEN _numyears + LEAST( cast(L.description as int) - cy.minCovid+ 1, cy.maxCovid-cy.minCovid+ 1 )
	ELSE _numyears
END,
	L.SurveyYear_B01ID, 
	cast (L.description as int)
FROM 
	tfwm_nts_securelookups.SurveyYear_B01ID L
	CROSS JOIN cteCovidYears cy
WHERE
	L.SurveyYear_B01ID >= (select min(L2.SurveyYear_B01ID) from tfwm_nts_securelookups.SurveyYear_B01ID L2 WHERE L2.SurveyYear_B01ID>=0) + _numyears -1 
	AND 
	(_skipCovidYears!=1 OR cast (L.description as int) < cy.minCovid OR cast (L.description as int) > cy.maxCovid)
),

ctaFromToYears( fromYearId, toYearId, toYear, fromYear )
as
(select 
 	ctaFromToYearsId.*, 
 	cast(L.description as int) 
 from ctaFromToYearsId
inner join
	tfwm_nts_securelookups.SurveyYear_B01ID L
on L.SurveyYear_B01ID = fromYearId
),

cteModeLabel(MainMode_B04ID, description)
as
(select MainMode_B04ID, description, part from tfwm_nts_securelookups.MainMode_B04ID mm   
where (1!=_combineLocalBusModes or 7!=MainMode_B04ID) --exclude london buses if combining is switched on
	and (1!=_combineUndergroundIntoOther or 10!=MainMode_B04ID) --exclude london underground if combining is switched on
	AND part=1
--union all
--select _dummyModeIdValue, 'Walk >=1 mile'
),

ctePurposeLabels (tpID, tpDesc) 
as
(
SELECT tp.TripPurpose_B01ID, tp.description
FROM tfwm_nts_securelookups.TripPurpose_B01ID tp
WHERE _groupByTripPurposeSetting = 1
AND part=1
	
UNION ALL
 
SELECT tp.TripPurpose_B02ID, tp.description
FROM tfwm_nts_securelookups.TripPurpose_B02ID tp
WHERE _groupByTripPurposeSetting = 2
AND part=1
	
UNION ALL
	
SELECT tp.TripPurpose_B04ID, tp.description
FROM tfwm_nts_securelookups.TripPurpose_B04ID tp
WHERE _groupByTripPurposeSetting = 4
AND part=1

UNION ALL
	
SELECT tp.TripPurpose_B06ID, tp.description
FROM cteTripPurpose_B06ID tp
WHERE _groupByTripPurposeSetting = 6
 
),



cteLabels (yearID, yearDesc,
			countryID, StatsRegID, StatsRegDesc,
			tpID, tpDesc,
			mmID, mmDesc) 
as
(
SELECT psu.SurveyYear_B01ID, 
 		psu.SurveyYear,
		psu.psucountry_b01id,
		psu.PSUStatsReg_B01ID, 
 		statsRegLookup.description,
 		tp.tpID, tp.tpDesc,
		mm.MainMode_B04ID, mm.description
FROM 
	(select distinct SurveyYear_B01ID, SurveyYear, 
	 	CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END psucountry_b01id,
	 PSUStatsReg_B01ID from tfwm_nts_secureschema.psu ) as psu
 
	left outer join tfwm_nts_securelookups.PSUStatsReg_B01ID as statsRegLookup
	on psu.PSUStatsReg_B01ID = statsRegLookup.PSUStatsReg_B01ID
	
	cross join ctePurposeLabels tp
	cross join cteModeLabel mm
),


cteCountryLabels (yearID, yearDesc,
			countryID, countryCode, countryDesc,
			tpID, tpDesc,
			mmID, mmDesc) 
as
(select psu.SurveyYear_B01ID,
 		psu.SurveyYear,
		psu.psucountry_b01id,
 		CASE 
		 WHEN 2 = psu.psucountry_b01id THEN 'W92000004'
 		 WHEN 3 = psu.psucountry_b01id THEN 'S92000003'
	     ELSE 'E92000001'
		END,
		countryLookup.description,
 		tp.tpID, tp.tpDesc,
		mm.MainMode_B04ID, mm.description
from 
	(select distinct SurveyYear_B01ID, SurveyYear, 
	 	CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END psucountry_b01id from tfwm_nts_secureschema.psu ) as psu
 
	left outer join 
	tfwm_nts_securelookups.PSUCountry_B01ID as countryLookup
	on psu.psucountry_b01id = countryLookup.PSUCountry_B01ID
 
	cross join ctePurposeLabels tp
	cross join cteModeLabel mm
 WHERE
 	countryLookup.part=1  
),


--this table is one of the view lookups with a VARCHAR id, that the currently load process doesn't cope with.
lookup_HHoldOSLAUA_B01ID ( ID, description )
as
(
select 'E08000025','Birmingham'
union all select 'E08000026','Coventry'
union all select 'E08000027','Dudley'
union all select 'E08000028','Sandwell'
union all select 'E08000029','Solihull'
union all select 'E08000030','Walsall'
union all select 'E08000031','Wolverhampton'	
),

 	
cteLaLabels (yearID, yearDesc,
			LaID, LaDesc,
			tpID, tpDesc,
			mmID, mmDesc) 
as
(select psu.SurveyYear_B01ID, psu.SurveyYear,
		laLookup.Id,
		laLookup.description description,
 		tp.tpID, tp.tpDesc,
		mm.MainMode_B04ID, mm.description
from 
	(select distinct SurveyYear_B01ID, SurveyYear from tfwm_nts_secureschema.psu ) as psu
	cross join lookup_HHoldOSLAUA_B01ID laLookup
 	cross join ctePurposeLabels tp
	cross join cteModeLabel mm
WHERE 
 (0 != _generateLaResults)
),


--JJXSC The number of trips to be counted, grossed for short walks and excluding “Series of Calls” trips. 
--JD The distance of the trip (miles), grossed for short walks.
--JTTXSC The total travelling time of the trip (in minutes), grossed for short walks and excluding “Series of Calls” trips. 
--JOTXSC The overall duration of the trip (in minutes), meaning that it includes both the travelling and waiting times between stages, 
--  grossed for short walks and excluding “Series of Calls” trips.
cteTripsBase (yearID, countryID, statsregID, laID, tpID, mmID,   
		Trips_unweighted , Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
)
as
(select SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id,
 		HHoldOSLAUA_B01ID,
 		CASE WHEN _groupByTripPurposeSetting = 1 THEN TripPurpose_B01ID
 			 WHEN _groupByTripPurposeSetting = 2 THEN TripPurpose_B02ID
 			 WHEN _groupByTripPurposeSetting = 4 THEN TripPurpose_B04ID
 			 WHEN _groupByTripPurposeSetting = 6 THEN TripPurpose_B01ID
			 ELSE NULL
		END,
		CASE WHEN 1 = _combineLocalBusModes and 7 = MainMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = MainMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE MainMode_B04ID
		END, 
		SUM(JJXSC), SUM(W5 * JJXSC),
		SUM(JD), SUM(W5 * JD),
		SUM(JOTXSC), SUM(W5 * JOTXSC),
		SUM(JTTXSC), SUM(W5 * JTTXSC)
		--,SUM(W5 * SD),
		--SUM(W5 * STTXSC)
from tfwm_nts_secureschema.trip T

left join tfwm_nts_secureschema.PSU as P
on T.PSUID = P.PSUID

left join tfwm_nts_secureschema.Household as H
on T.householdid = H.householdid

left join tfwm_nts_secureschema.individual as I
on T.individualID = I.individualID

/*left join
nts.stage S
on T.TripID = S.TripID
where S.StageMain_B01ID = 1 --main stage only*/

WHERE 
(
 1 != _restrictToWorkingAge 
 OR (age_b01id >= 6 AND age_b01id <= 16) -- 16-64 year old (inclusive) 
)
AND
(
	_excludeShortWalks != 1
	OR T.MainMode_B11ID != 1
)

GROUP BY SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id,
 		HHoldOSLAUA_B01ID,
 		CASE WHEN _groupByTripPurposeSetting = 1 THEN TripPurpose_B01ID
 			 WHEN _groupByTripPurposeSetting = 2 THEN TripPurpose_B02ID
 			 WHEN _groupByTripPurposeSetting = 4 THEN TripPurpose_B04ID
 			 WHEN _groupByTripPurposeSetting = 6 THEN TripPurpose_B01ID
			 ELSE NULL
		END,
		CASE WHEN 1 = _combineLocalBusModes and 7 = MainMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = MainMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE MainMode_B04ID
		END 
),


cteTripsBasePurpose (yearID, countryID, statsregID, laID, tpID, mmID,  
		Trips_unweighted , Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
)
as
(SELECT 
yearID, countryID, statsregID, laID,
	case 
 		WHEN _groupByTripPurposeSetting != 6 THEN tpID
		when tpID = -10 then -10 --DEAD
		when tpID = -8 then -8 --NA
		when tpID = 1 then 1 --Commuting -> Commuting & escort commuting
		when tpID = 2 then 2 --Business -> Business & escort business
		when tpID = 3 then 5 --Other work -> Personal business
		when tpID = 4 then 3 --Education -> Education & escort education
		when tpID = 5 then 4 --Food shopping -> Shopping & escort shopping / personal business
		when tpID = 6 then 4 --Non food shopping -> Shopping & escort shopping / personal business
		when tpID = 7 then 5 --Personal business medical -> Personal business 
		when tpID = 8 then 5 --Personal business eat / drink -> Personal business
		when tpID = 9 then 5 --Personal business other -> Personal business 
		when tpID = 10 then 6 --Visit friends at private home -> Leisure
		when tpID = 11 then 6 --Eat / drink with friends -> Leisure 
		when tpID = 12 then 6 --Other social -> Leisure 
		when tpID = 13 then 6 --Entertain / public activity -> Leisure 
		when tpID = 14 then 6 --Sport: participate -> Leisure 
		when tpID = 15 then 7 --Holiday: base -> Holiday / day trip
		when tpID = 16 then 7 --Day trip -> Holiday / day trip
		when tpID = 17 then 8 --Just walk -> Other including just walk & escort home (not own) / other
		when tpID = 18 then 8 --Other non-escort -> Other including just walk & escort home (not own) / other
		when tpID = 19 then 1 --Escort commuting -> Commuting & escort commuting
		when tpID = 20 then 2 --Escort business & other work -> Business & escort business
		when tpID = 21 then 3 --Escort education -> Education & escort education
		when tpID = 22 then 4 --Escort shopping / personal business -> Shopping & escort shopping / personal business
		when tpID = 23 then 8 --Escort home (not own) & other escort -> Other including just walk & escort home (not own) / other
		else NULL 
		end as TripPurpose_B06ID, 
		mmID,

		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
		--,sum(MainStageDistance_weighted),
		--sum(MainStageTravelTime_weighted)

FROM cteTripsBase
GROUP BY
yearID, countryID, statsregID, laID,
 	case 
 		WHEN _groupByTripPurposeSetting != 6 THEN tpID
		when tpID = -10 then -10 --DEAD
		when tpID = -8 then -8 --NA
		when tpID = 1 then 1 --Commuting -> Commuting & escort commuting
		when tpID = 2 then 2 --Business -> Business & escort business
		when tpID = 3 then 5 --Other work -> Personal business
		when tpID = 4 then 3 --Education -> Education & escort education
		when tpID = 5 then 4 --Food shopping -> Shopping & escort shopping / personal business
		when tpID = 6 then 4 --Non food shopping -> Shopping & escort shopping / personal business
		when tpID = 7 then 5 --Personal business medical -> Personal business 
		when tpID = 8 then 5 --Personal business eat / drink -> Personal business
		when tpID = 9 then 5 --Personal business other -> Personal business 
		when tpID = 10 then 6 --Visit friends at private home -> Leisure
		when tpID = 11 then 6 --Eat / drink with friends -> Leisure 
		when tpID = 12 then 6 --Other social -> Leisure 
		when tpID = 13 then 6 --Entertain / public activity -> Leisure 
		when tpID = 14 then 6 --Sport: participate -> Leisure 
		when tpID = 15 then 7 --Holiday: base -> Holiday / day trip
		when tpID = 16 then 7 --Day trip -> Holiday / day trip
		when tpID = 17 then 8 --Just walk -> Other including just walk & escort home (not own) / other
		when tpID = 18 then 8 --Other non-escort -> Other including just walk & escort home (not own) / other
		when tpID = 19 then 1 --Escort commuting -> Commuting & escort commuting
		when tpID = 20 then 2 --Escort business & other work -> Business & escort business
		when tpID = 21 then 3 --Escort education -> Education & escort education
		when tpID = 22 then 4 --Escort shopping / personal business -> Shopping & escort shopping / personal business
		when tpID = 23 then 8 --Escort home (not own) & other escort -> Other including just walk & escort home (not own) / other
		else NULL 
		end,
	mmID
),

cteTrips(yearID, countryID, statsregID, tpID, mmID,  
		Trips_unweighted , Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
)
as (
SELECT yearID, countryID, statsregID, tpID, mmID,  
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted) FROM cteTripsBasePurpose
GROUP BY yearID, countryID, statsregID, tpID, mmID
),

cteLaTrips (yearID, laID, tpID, mmID, 
		Trips_unweighted , Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
)
as
(SELECT yearID, laID, tpID, mmID,  
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted) FROM cteTripsBasePurpose
GROUP BY yearID, laID, tpID, mmID
),



cteStagesBase (yearID, surveyYear, countryID, statsregID, laID, tpID, smID, 
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted
)
as
(
select SurveyYear_B01ID, 
		P.surveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id, 
		HHoldOSLAUA_B01ID,
 		CASE WHEN _groupByTripPurposeSetting = 1 THEN TripPurpose_B01ID
 			 WHEN _groupByTripPurposeSetting = 2 THEN TripPurpose_B02ID
 			 WHEN _groupByTripPurposeSetting = 4 THEN TripPurpose_B04ID
 			 WHEN _groupByTripPurposeSetting = 6 THEN TripPurpose_B01ID
			 ELSE NULL
		END,
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END, 
		SUM(SSXSC), SUM(W5 * SSXSC),
		SUM(W5 * SD),
		SUM(W5 * STTXSC),
		SUM(W5 * SSXSC * CASE WHEN -8 = numboardings THEN 1 ELSE numboardings END)
	      --assume number of boardings is one if question not answered / not applicable
FROM 
tfwm_nts_secureschema.stage S

left join tfwm_nts_secureschema.PSU as P
on S.PSUID = P.PSUID

left join tfwm_nts_secureschema.trip T
on s.TripID = t.TripID

left join tfwm_nts_secureschema.Household as H
on T.householdid = H.householdid

left join tfwm_nts_secureschema.individual as I
on T.individualID = I.individualID

WHERE
(
 1 != _restrictToWorkingAge 
 OR (age_b01id >= 6 AND age_b01id <= 16) -- 16-64 year old (inclusive) 
)
AND
(
	_excludeShortWalks != 1
	OR T.MainMode_B11ID != 1
)

GROUP BY SurveyYear_B01ID, 
		P.surveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id,
		HHoldOSLAUA_B01ID,
 		CASE WHEN _groupByTripPurposeSetting = 1 THEN TripPurpose_B01ID
 			 WHEN _groupByTripPurposeSetting = 2 THEN TripPurpose_B02ID
 			 WHEN _groupByTripPurposeSetting = 4 THEN TripPurpose_B04ID
 			 WHEN _groupByTripPurposeSetting = 6 THEN TripPurpose_B01ID
			 ELSE NULL
		END,
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END 
),

cteStagesBasePurpose (yearID, surveyYear, countryID, statsregID, laID, tpID, smID,  
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted
)
as
(SELECT yearID, surveyYear, countryID, statsregID, laID,
	case 
 		WHEN _groupByTripPurposeSetting != 6 THEN tpID 
		when tpID = -10 then -10 --DEAD
		when tpID = -8 then -8 --NA
		when tpID = 1 then 1 --Commuting -> Commuting & escort commuting
		when tpID = 2 then 2 --Business -> Business & escort business
		when tpID = 3 then 5 --Other work -> Personal business
		when tpID = 4 then 3 --Education -> Education & escort education
		when tpID = 5 then 4 --Food shopping -> Shopping & escort shopping / personal business
		when tpID = 6 then 4 --Non food shopping -> Shopping & escort shopping / personal business
		when tpID = 7 then 5 --Personal business medical -> Personal business 
		when tpID = 8 then 5 --Personal business eat / drink -> Personal business
		when tpID = 9 then 5 --Personal business other -> Personal business 
		when tpID = 10 then 6 --Visit friends at private home -> Leisure
		when tpID = 11 then 6 --Eat / drink with friends -> Leisure 
		when tpID = 12 then 6 --Other social -> Leisure 
		when tpID = 13 then 6 --Entertain / public activity -> Leisure 
		when tpID = 14 then 6 --Sport: participate -> Leisure 
		when tpID = 15 then 7 --Holiday: base -> Holiday / day trip
		when tpID = 16 then 7 --Day trip -> Holiday / day trip
		when tpID = 17 then 8 --Just walk -> Other including just walk & escort home (not own) / other
		when tpID = 18 then 8 --Other non-escort -> Other including just walk & escort home (not own) / other
		when tpID = 19 then 1 --Escort commuting -> Commuting & escort commuting
		when tpID = 20 then 2 --Escort business & other work -> Business & escort business
		when tpID = 21 then 3 --Escort education -> Education & escort education
		when tpID = 22 then 4 --Escort shopping / personal business -> Shopping & escort shopping / personal business
		when tpID = 23 then 8 --Escort home (not own) & other escort -> Other including just walk & escort home (not own) / other
		else NULL 
		end as TripPurpose_B06ID, 
		smID,

		sum(Stages_unweighted), sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted),
		sum(Boardings_weighted)
FROM cteStagesBase
GROUP BY
 yearID, surveyYear, countryID, statsregID, laID,
	case 
 		WHEN _groupByTripPurposeSetting != 6 THEN tpID 
		when tpID = -10 then -10 --DEAD
		when tpID = -8 then -8 --NA
		when tpID = 1 then 1 --Commuting -> Commuting & escort commuting
		when tpID = 2 then 2 --Business -> Business & escort business
		when tpID = 3 then 5 --Other work -> Personal business
		when tpID = 4 then 3 --Education -> Education & escort education
		when tpID = 5 then 4 --Food shopping -> Shopping & escort shopping / personal business
		when tpID = 6 then 4 --Non food shopping -> Shopping & escort shopping / personal business
		when tpID = 7 then 5 --Personal business medical -> Personal business 
		when tpID = 8 then 5 --Personal business eat / drink -> Personal business
		when tpID = 9 then 5 --Personal business other -> Personal business 
		when tpID = 10 then 6 --Visit friends at private home -> Leisure
		when tpID = 11 then 6 --Eat / drink with friends -> Leisure 
		when tpID = 12 then 6 --Other social -> Leisure 
		when tpID = 13 then 6 --Entertain / public activity -> Leisure 
		when tpID = 14 then 6 --Sport: participate -> Leisure 
		when tpID = 15 then 7 --Holiday: base -> Holiday / day trip
		when tpID = 16 then 7 --Day trip -> Holiday / day trip
		when tpID = 17 then 8 --Just walk -> Other including just walk & escort home (not own) / other
		when tpID = 18 then 8 --Other non-escort -> Other including just walk & escort home (not own) / other
		when tpID = 19 then 1 --Escort commuting -> Commuting & escort commuting
		when tpID = 20 then 2 --Escort business & other work -> Business & escort business
		when tpID = 21 then 3 --Escort education -> Education & escort education
		when tpID = 22 then 4 --Escort shopping / personal business -> Shopping & escort shopping / personal business
		when tpID = 23 then 8 --Escort home (not own) & other escort -> Other including just walk & escort home (not own) / other
		else NULL 
		end,
	smID
),


cteStages (yearID, surveyYear, countryID, statsregID, tpID, smID,  
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted
)
as
(
SELECT yearID, surveyYear, countryID, statsregID, tpID, smID,   
		sum(Stages_unweighted), sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted),
		sum(Boardings_weighted)
FROM cteStagesBasePurpose
GROUP BY yearID, surveyYear, countryID, statsregID, tpID, smID 	
),

cteLaStages (yearID, surveyYear, laID, tpID, smID, 
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted
)
as
(
select yearID, surveyYear, laID, tpID, smID, 
		sum(Stages_unweighted) , sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted),
		sum(Boardings_weighted)
from cteStagesBasePurpose
group by yearID, surveyYear, laID, tpID, smID   
),


cteXyrs (yearID,
		 fromYear,
		 toYear,		 
		 countryID, statsregID, tpID, mmID,
		Trips_unweighted, Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted,
		--MainStageDistance_weighted, MainStageTravelTime_weighted,
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted
)
as
(
select L.SurveyYear_B01ID,
	min(S.surveyYear),
	max(S.surveyYear),	
	S.countryID, S.statsregID, 
		COALESCE(S.tpID, T.tpID),
		COALESCE(S.smID, T.mmID),
		sum(T.Trips_unweighted), sum(T.Trips_weighted),
		sum(T.TripDistance_unweighted), sum(T.TripDistance_weighted),
		sum(T.TripDuration_unweighted), sum(T.TripDuration_weighted),
		sum(T.TripTravelTime_unweighted), sum(T.TripTravelTime_weighted),
		--sum(T.MainStageDistance_weighted), sum(T.MainStageTravelTime_weighted,
		sum(S.Stages_unweighted) , sum(S.Stages_weighted),
		sum(S.StageDistance_weighted),
		sum(S.StageTravelTime_weighted),
		sum(S.Boardings_weighted)
from
	tfwm_nts_securelookups.SurveyYear_B01ID L
	
	inner join ctaFromToYears fty
	on L.SurveyYear_B01ID = fty.toYearId
	cross join cteCovidYears cy
	
	left join 
	cteStages as S
		on S.yearID >= fty.fromYearId and S.yearID <= fty.toYearId
		
	full outer join
	cteTrips as T
		on S.yearID = T.yearID and S.countryID = T.countryID and S.statsregID = T.statsregID and S.tpID = T.tpID and S.smID = T.mmID

	where
		(_skipCovidYears!=1 OR S.surveyYear< cy.minCovid OR S.surveyYear> cy.maxCovid)

group by L.SurveyYear_B01ID, S.countryID, S.statsregID, COALESCE(S.tpID, T.tpID), COALESCE(S.smID, T.mmID)
),


cteLaXyrs (yearID, 
		fromYear,
		toYear,		   		   
		laID, tpID, mmID,
		Trips_unweighted, Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted,
		--MainStageDistance_weighted, MainStageTravelTime_weighted,
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted
)
as
(
select L.SurveyYear_B01ID,
	min(S.surveyYear),
	max(S.surveyYear),
	S.laID, 
		COALESCE(S.tpID, T.tpID), 
		COALESCE(S.smID, T.mmID),
		sum(T.Trips_unweighted), sum(T.Trips_weighted),
		sum(T.TripDistance_unweighted), sum(T.TripDistance_weighted),
		sum(T.TripDuration_unweighted), sum(T.TripDuration_weighted),
		sum(T.TripTravelTime_unweighted), sum(T.TripTravelTime_weighted),
		--sum(T.MainStageDistance_weighted), sum(T.MainStageTravelTime_weighted,
		sum(S.Stages_unweighted) , sum(S.Stages_weighted),
		sum(S.StageDistance_weighted),
		sum(S.StageTravelTime_weighted),
		sum(S.Boardings_weighted)
from
	tfwm_nts_securelookups.SurveyYear_B01ID L
	
	inner join ctaFromToYears fty
	on L.SurveyYear_B01ID = fty.toYearId
	cross join cteCovidYears cy
	
	left join 
	cteLaStages as S
		on S.yearID >= fty.fromYearId and S.yearID <= fty.toYearId
		
	full outer join
	cteLaTrips as T
		on S.yearID = T.yearID and S.laID = T.laID and S.tpID = T.tpID and S.smID = T.mmID

where
	(_skipCovidYears!=1 OR S.surveyYear< cy.minCovid OR S.surveyYear> cy.maxCovid)	
	
group by L.SurveyYear_B01ID, S.laID, COALESCE(S.tpID, T.tpID), COALESCE(S.smID, T.mmID)
),


cteXyrsAllRegions (yearID, 
		fromYear,
		toYear,			   				   
		countryID, tpID, mmID,
		Trips_unweighted, Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted,
		--MainStageDistance_weighted,MainStageTravelTime_weighted,
		Stages_unweighted, Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted
)
as
(select yearID, 
		min(fromYear),
		max(toYear),	
 		countryID, tpID, mmID,
		sum(Trips_unweighted), sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted),
		--sum(MainStageDistance_weighted), sum(MainStageTravelTime_weighted)
		sum(Stages_unweighted) , sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted),
 		sum(Boardings_weighted)
from
cteXyrs
group by yearID, countryID, tpID, mmID
),



--W0	Unweighted interview sample(Household)
--W3	Interview sample household weight (Household)
--W1	Unweighted diary sample(Household)
--W2	Diary sample household weight (Household)
--W5	Trip/Stage weight (Trip)
--W4	LDJ weight (LDJ)
--W6	Attitudes weight(Attitudes)
cteIndividualsBase (yearID, countryID, statsregID, laId, Individuals_unweighted, Individuals_weighted)
as
(select SurveyYear_B01ID, 
	CASE WHEN psucountry_b01id = -10 THEN 1
 		WHEN psucountry_b01id isnull THEN 1
		 ELSE psucountry_b01id
	END,
	psustatsreg_b01id, 
 	HHoldOSLAUA_B01ID,
 	SUM(W1), SUM(W2)
from 
tfwm_nts_secureschema.individual I
 
left join tfwm_nts_secureschema.PSU as P
on I.PSUID = P.PSUID
 
left join tfwm_nts_secureschema.Household as H
on I.HouseholdID = H.HouseholdID
 
group by SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id,
 		HHoldOSLAUA_B01ID
),


cteIndividuals (yearID, countryID, statsregID, Individuals_unweighted, Individuals_weighted)
as
(select yearID, countryID, statsregID, sum(Individuals_unweighted), sum(Individuals_weighted)
from cteIndividualsBase
group by yearID, countryID, statsregID
),

cteLaIndividuals (yearID, laID, Individuals_unweighted, Individuals_weighted)
as
(select yearID, laID, sum(Individuals_unweighted), sum(Individuals_weighted)
from cteIndividualsBase
group by yearID, laID
),

cteIndividualsAllRegions (yearID, countryID, Individuals_unweighted, Individuals_weighted)
as
(select yearID, countryID, sum(Individuals_unweighted), sum(Individuals_weighted)
from cteIndividuals
group by yearID, countryID
),

cteXyrsIndividuals(yearID, countryID, statsregID, Individuals_unweighted, Individuals_weighted)
as
(select sy.SurveyYear_B01ID, i.countryID, i.statsregID, sum(I.Individuals_unweighted), sum(I.Individuals_weighted)
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
 
 	inner join ctaFromToYears fty
	on sy.SurveyYear_B01ID = fty.toYearId
	cross join cteCovidYears cy

	left join 
	cteIndividuals as I
 		on I.yearID >= fty.fromYearId and I.yearID <= fty.toYearId
 
where
 sy.SurveyYear_B01ID >=0 
 AND(_skipCovidYears!=1 OR cast(Sy.description as int)< cy.minCovid OR cast(Sy.description as int)> cy.maxCovid)
 AND(_skipCovidYears!=1 OR I.yearID< cy.minCovidId OR I.yearID> cy.maxCovidId)
 
group by sy.SurveyYear_B01ID, countryID, statsregID
),


cteLaXyrsIndividuals(yearID, laID, Individuals_unweighted, Individuals_weighted)
as
(select sy.SurveyYear_B01ID, i.laID, sum(I.Individuals_unweighted), sum(I.Individuals_weighted)
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
 
 	inner join ctaFromToYears fty
	on sy.SurveyYear_B01ID = fty.toYearId
	cross join cteCovidYears cy
 
	left join 
	cteLaIndividuals as I
 		on I.yearID >= fty.fromYearId and I.yearID <= fty.toYearId

 where
 sy.SurveyYear_B01ID >=0 
 AND(_skipCovidYears!=1 OR cast(Sy.description as int)< cy.minCovid OR cast(Sy.description as int)> cy.maxCovid)
 AND(_skipCovidYears!=1 OR I.yearID< cy.minCovidId OR I.yearID> cy.maxCovidId)
 
group by sy.SurveyYear_B01ID, laID
),


cteXyrsIndividualsAllRegions(yearID, countryID, Individuals_unweighted, Individuals_weighted)
as
(select sy.SurveyYear_B01ID, i.countryID, sum(I.Individuals_unweighted), sum(I.Individuals_weighted)
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
 
 	inner join ctaFromToYears fty
	on sy.SurveyYear_B01ID = fty.toYearId
	cross join cteCovidYears cy

	left join 
	cteIndividualsAllRegions as I
 		on I.yearID >= fty.fromYearId and I.yearID <= fty.toYearId
 
where
 sy.SurveyYear_B01ID >=0 
 AND(_skipCovidYears!=1 OR cast(Sy.description as int)< cy.minCovid OR cast(Sy.description as int)> cy.maxCovid)
 AND(_skipCovidYears!=1 OR I.yearID< cy.minCovidId OR I.yearID> cy.maxCovidId)
 
group by sy.SurveyYear_B01ID, countryID 
),


finalQuery as (
select  
fty.fromyear "start year", 
fty.toyear "end year", 

StatsRegDesc "region",
tpDesc "purpose",
mmDesc "mode",
L.mmID "modeId",

	Trips_unweighted as Trips_UNweighted,
--	cast(round(Trips_weighted,2)as float) as Trips_Weighted, 
	Stages_unweighted as Stages_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
	cast(round(cast(Individuals_weighted as numeric),2)as float) as Individuals_Weighted,
	
--round( cast(Trips_unweighted as float)* _weekToYearCorrectionFactor / cast(Individuals_unweighted as float), 3 ) "UNweighted tripRate",	
	
CASE WHEN Individuals_weighted>0 THEN cast(round( cast(Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) ELSE NULL END  "weighted tripRate",

CASE WHEN Individuals_weighted>0 THEN cast(round( cast(Stages_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) ELSE NULL END  "weighted stageRate",

CASE WHEN Individuals_weighted>0 THEN cast(round( cast(Boardings_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) ELSE NULL END  "weighted boardingRate (unpublished)",

CASE WHEN Individuals_weighted>0 THEN cast(round( cast(StageDistance_weighted * _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) ELSE NULL END  "total stage distance per-person-per-year (miles)",

CASE WHEN Trips_weighted>0 THEN cast(round( cast(TripDistance_weighted/Trips_weighted as numeric), 3 )as float) ELSE NULL END  "mean tripDistance (miles)",

CASE WHEN Individuals_weighted>0 THEN cast(round( cast(TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric), 3 )as float) ELSE NULL END  "mean tripDuration per-person-per-year (hours)",

CASE WHEN Trips_weighted>0 THEN cast(round( cast(TripDuration_weighted/Trips_weighted as numeric), 3 )as float) ELSE NULL END  "mean tripDuration (minutes)",

CASE WHEN Individuals_weighted>0 THEN cast(round( cast(StageTravelTime_weighted * _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric), 3 )as float) ELSE NULL END  "total stg travel tm (in veh) p-pers-p-year (hours)(unpublished)"

from 
	cteLabels as L
	inner join ctaFromToYears fty
	on L.yearID = fty.toYearId
	
	left join
	cteXyrsIndividuals as I
		on L.yearID = I.yearID
		and L.countryID = I.countryID
		and L.StatsRegID = I.statsregID
	left join
	cteXyrs as T
		on L.yearID = T.yearID
		and L.countryID = T.countryID
		and L.StatsRegID = T.statsregID
		and L.tpID = T.tpID
		and L.mmID = T.mmID

--	cross join
--	(select min(SurveyYear) "year" from tfwm_nts_secureschema.psu) minYear
where 
	(L.statsregID=_statsregID or L.statsregID is null or 0=_statsregID)
	AND L.statsregID!=14 AND L.statsregID!=15  --exclude scotland and wales as regions, pick them up as countries instead
--	and 	(fty.fromyear = 2003 or fty.fromyear=2012)


union all

select  
fty.fromyear "start year", 
fty.toyear "end year", 

CountryDesc "country",
tpDesc "purpose",
mmDesc "mode",
L.mmID "modeId",

	Trips_unweighted as Trips_UNweighted,
--	cast(round(Trips_weighted,2)as float) as Trips_Weighted, 
	Stages_unweighted as Stages_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
	cast(round(cast(Individuals_weighted as numeric),2)as float) as Individuals_Weighted,
	
--round( cast(Trips_unweighted as float)* _weekToYearCorrectionFactor / cast(Individuals_unweighted as float), 3 ) "UNweighted tripRate",	
	
CASE WHEN Individuals_weighted>0 THEN cast( (Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted) as float) ELSE NULL END  "weighted tripRate (0303a)",

CASE WHEN Individuals_weighted>0 THEN cast(( Stages_weighted* _weekToYearCorrectionFactor / Individuals_weighted )as float) ELSE NULL END  "weighted stageRate (0303b)",

CASE WHEN Individuals_weighted>0 THEN cast(( Boardings_weighted* _weekToYearCorrectionFactor / Individuals_weighted )as float) ELSE NULL END  "weighted boardingRate (unpublished)",
	
CASE WHEN Individuals_weighted>0 THEN cast(( StageDistance_weighted * _weekToYearCorrectionFactor / Individuals_weighted )as float) ELSE NULL END  "total stage distance per-person-per-year (miles)(0303c)",

CASE WHEN Trips_weighted>0 THEN cast(( TripDistance_weighted/Trips_weighted )as float) ELSE NULL END  "mean tripDistance (miles)(0303d)",

CASE WHEN Individuals_weighted>0 THEN cast(( TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / Individuals_weighted )as float) ELSE NULL END  "mean tripDuration per-person-per-year (hours)(0303e)",

CASE WHEN Trips_weighted>0 THEN cast(( TripDuration_weighted/Trips_weighted )as float) ELSE NULL END  "mean tripDuration (minutes)(0303f)",

CASE WHEN Individuals_weighted>0 THEN cast(( StageTravelTime_weighted * _weekToYearCorrectionFactor / 60.0 / Individuals_weighted)as float) ELSE NULL END  "total stg travel tm (in veh) p-pers-p-year (hours)(unpublished)"

from 
	cteCountryLabels as L
	inner join ctaFromToYears fty
	on L.yearID = fty.toYearId
	
	left join
	cteXyrsIndividualsAllRegions as I
		on L.yearID = I.yearID
		and L.countryID = I.countryID
	left join
	cteXyrsAllRegions as T
		on L.yearID = T.yearID
		and L.countryID = T.countryID
		and L.tpID = T.tpID 
		and L.mmID = T.mmID

union all


select  
fty.fromyear "start year", 
fty.toyear "end year", 

LaDesc "region",
tpDesc "purpose",
mmDesc "mode",
L.mmID "modeId",

	Trips_unweighted as Trips_UNweighted,
--	cast(round(Trips_weighted,2)as float) as Trips_Weighted, 
	Stages_unweighted as Stages_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
	cast(round(cast(Individuals_weighted as numeric),2)as float) as Individuals_Weighted,
	
--round( cast(Trips_unweighted as float)* _weekToYearCorrectionFactor / cast(Individuals_unweighted as float), 3 ) "UNweighted tripRate",	
	
CASE WHEN Individuals_weighted>0 THEN cast(round( cast(Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) ELSE NULL END  "weighted tripRate (0303a)",

CASE WHEN Individuals_weighted>0 THEN  cast(round( cast(Stages_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) ELSE NULL END "weighted stageRate (0303b)",

CASE WHEN Individuals_weighted>0 THEN cast(round( cast(Boardings_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) ELSE NULL END  "weighted boardingRate (unpublished)",
	
CASE WHEN Individuals_weighted>0 THEN cast(round( cast(StageDistance_weighted * _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) ELSE NULL END  "total stage distance per-person-per-year (miles)(0303c)",

CASE WHEN Trips_weighted>0 THEN cast(round( cast(TripDistance_weighted/Trips_weighted as numeric), 3 )as float) ELSE NULL END  "mean tripDistance (miles)(0303d)",

CASE WHEN Individuals_weighted>0 THEN cast(round( cast(TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric), 3 )as float) ELSE NULL END  "mean tripDuration per-person-per-year (hours)(0303e)",

CASE WHEN Trips_weighted>0 THEN cast(round( cast(TripDuration_weighted/Trips_weighted as numeric), 3 )as float) ELSE NULL END  "mean tripDuration (minutes)(0303f)",

CASE WHEN Individuals_weighted>0 THEN cast(round( cast(StageTravelTime_weighted * _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric), 3 )as float) ELSE NULL END  "total stg travel tm (in veh) p-pers-p-year (hours)(unpublished)"

from 
	cteLaLabels as L
	inner join ctaFromToYears fty
	on L.yearID = fty.toYearId

	left join
	cteLaXyrsIndividuals as I
		on L.yearID = I.yearID
		and L.laID = I.laID
	left join
	cteLaXyrs as T
		on L.yearID = T.yearID
		and L.laID = T.laID
		and L.tpID = T.tpID 
		and L.mmID = T.mmID

where 
	(0 != _generateLaResults)
--	and 	(fty.fromyear = 2003 or fty.fromyear=2012)
)

select * from finalquery order by 1,2,3,4,6;

end;
$$;
 
select * from __temp_table;
 
--can't drop the temp table here otherwise I don't get any output from the select statement in the pgadmin window

