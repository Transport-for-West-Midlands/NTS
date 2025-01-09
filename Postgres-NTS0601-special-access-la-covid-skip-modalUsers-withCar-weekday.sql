/*=================================================================================

NTS0601 - count number of users using a particular mode during the diary week - including filter on journey / trip distance

short walk: MainMode_B02ID = 1 (replaced by MainMode_B11ID<>1)

	Owen O'Neill:	July 2023
	Owen O'Neill:	June 2024: updated to use restricted licence data.
	Owen O'Neill:   June 2024: added WMCA local authorities - watch out for sample size !
	Owen O'Neill:   June 2024: added option to skip covid years (2020+2021)
	Owen O'Neill:   November 2024: created from NTS0303 query	
	Owen O'Neill:   November 2024: created from NTS0601 query	
	Owen O'Neill:   Dec 2024: added day of week filter
	Owen O'Neill:   Jan 2025: added filtering on trip / stage distance

=================================================================================*/
--use NTS;

DO $$
DECLARE

_numyears constant smallint = 5; --number of years to roll up averages (backwards from date reported in result row)

_statsregID constant  smallint = 10; --set to zero for all regions west midlands=10
									--if non-zero generates LA level results as well.

_dummyModeIdValue constant  float = 1.5; --walks are split to 'long' walks and all walks - we use this dummy value for the additional 'long walk' category.

_weekToYearCorrectionFactor constant  float = 52.14; -- ((365.0*4.0)+1.0)/4.0/7.0; 
--diary is for 1 week - need to multiply by a suitable factor to get yearly trip rate
--365/7 appears wrong - to include leap years we should use (365*4+1)/4/7
--documentation further rounds this to 52.14, so to get the closest possible match to published national values use 52.14 (even though it's wrong) 	

_combineLocalBusModes  constant smallint = 1; --captured data segregates london bus and other local buses. We need this to compare with national results 
										 -- but want to combine them for our analysis. Use this to switch it on/off 

_combineUndergroundIntoOther  constant smallint = 1; --captured data segregates london underground. For other regions the tram/metro service goes into the 'other PT' category.
										--We need this to compare with national results but want to combine them for our analysis. Use this to switch it on/off 

_skipCovidYears constant smallint = 1; --if enabled skips 2020 + 2021 and extends year window to compensate so number of years aggregated remains the same.

_minUnweightedTripsPerWeek constant smallint = 0; --minimum number of unweighted trips per week (on a given mode) that person has to make to be counted in results.
													--since the survey runs for a week then implicitly if an individual appears in the data they have made 1 trip in their survey week
													--journeys that are 'series of calls' are not counted as 'trips' because JJXSC=0 for these journeys. (0.05% of journeys)
													--this is why setting this variable to 0 or 1 generates slightly different results.

_minNumVehicles constant smallint = 0; --minimum number of vehicles in either household or available to the individual (dependent on which bit of SQL enabled in query below)

_minDistanceThresholdToExclude constant float = 1.0; --trips & stages above this distance are excluded from the count. 
													--i.e. to only count trips/stages LESS THAN OR EQUAL TO than 1 mile set this to 1.0     to include all distances set to a large number.
				--when this value causes some rows to be excluded, the 'total_individuals' columns in the result will not be filtered, whereas the 'individuals...' columns will be filtered.
				--on the flip side of this - people use multiple modes, so the sum of the number of individuals using each mode can be larger than the 'total_individual' count 

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

cteCovidYears( minCovid, maxCovid )
as
(
	select 2020, 2021 	
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
(select MainMode_B04ID, description from tfwm_nts_securelookups.MainMode_B04ID mm   
where (1!=_combineLocalBusModes or 7!=MainMode_B04ID) --exclude london buses if combining is switched on
	and (1!=_combineUndergroundIntoOther or 10!=MainMode_B04ID) --exclude london underground if combining is switched on
and part=1
union
select _dummyModeIdValue, 'Walk >=1 mile'
),

cteLabels (yearID, yearDesc,
			countryID, StatsRegID, StatsRegDesc,
			mmID, mmDesc)--,ageID, ageDesc) 
as
(select psu.SurveyYear_B01ID, 
 		psu.SurveyYear,
		psu.psucountry_b01id,
		psu.PSUStatsReg_B01ID, statsRegLookup.description,
		mm.MainMode_B04ID, mm.description
 		--,al.Age_B01ID, al.description
from 
	(select distinct SurveyYear_B01ID, SurveyYear, 
	 	CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END psucountry_b01id,
	 PSUStatsReg_B01ID from tfwm_nts_secureschema.psu ) as psu
 
	left outer join 
	tfwm_nts_securelookups.PSUStatsReg_B01ID as statsRegLookup
	on psu.PSUStatsReg_B01ID = statsRegLookup.PSUStatsReg_B01ID
	cross join cteModeLabel mm
-- 	cross join tfwm_nts_securelookups.Age_B01ID al
 WHERE
 	statsRegLookup.part=1 --and al.part=1 
),


cteCountryLabels (yearID, yearDesc,
			countryID, countryDesc,
			mmID, mmDesc) --,ageID, ageDesc) 
as
(select psu.SurveyYear_B01ID, psu.SurveyYear,
		psu.psucountry_b01id,
		countryLookup.description,
		mm.MainMode_B04ID, mm.description
 		--,al.Age_B01ID, al.description
from 
	(select distinct SurveyYear_B01ID, SurveyYear, 
	 	CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END psucountry_b01id from tfwm_nts_secureschema.psu ) as psu
 
	left outer join 
	tfwm_nts_securelookups.PSUCountry_B01ID as countryLookup
	on psu.psucountry_b01id = countryLookup.PSUCountry_B01ID
	cross join cteModeLabel mm
--    cross join tfwm_nts_securelookups.Age_B01ID al
 WHERE
 	countryLookup.part=1 --and al.part=1
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
			mmID, mmDesc) --,ageID, ageDesc) 
as
(select psu.SurveyYear_B01ID, psu.SurveyYear,
		laLookup.Id,
		laLookup.description description,
		mm.MainMode_B04ID, mm.description
 		--,al.Age_B01ID, al.description
from 
	(select distinct SurveyYear_B01ID, SurveyYear, 
	 	CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END psucountry_b01id from tfwm_nts_secureschema.psu ) as psu
 
	cross join lookup_HHoldOSLAUA_B01ID laLookup
	cross join cteModeLabel mm
-- 	cross join tfwm_nts_securelookups.Age_B01ID al
--WHERE al.part=1
),


--JJXSC The number of trips to be counted, grossed for short walks and excluding “Series of Calls” trips. 
--JD The distance of the trip (miles), grossed for short walks.
--JTTXSC The total travelling time of the trip (in minutes), grossed for short walks and excluding “Series of Calls” trips. 
--JOTXSC The overall duration of the trip (in minutes), meaning that it includes both the travelling and waiting times between stages, 
--  grossed for short walks and excluding “Series of Calls” trips.
cteTripBase (yearID, surveyYear, countryID, statsregID, laID,
		mmID, MainMode_B11ID, ageID, TripDayID,
		Trips_unweighted , Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
)
as
(select SurveyYear_B01ID, 
 		P.surveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id, 
 		HHoldOSLAUA_B01ID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = MainMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = MainMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE MainMode_B04ID
		END, 
 		MainMode_B11ID,
 		I.Age_B01ID,
 		MOD((h.TWSWeekday_B01ID-1) + (t.TravDay-1),7) +1, 
 		--don't have the 'day' table loaded, so work out the day of the week from the day of the week from the travel diary start day and offset by the day number within the diary - need to convert all operands from 1 based to zero based measures, apply modulo, convert back to 1 based.
		SUM(JJXSC), SUM(W5 * JJXSC),
		SUM(JD), SUM(W5 * JD),
		SUM(JOTXSC), SUM(W5 * JOTXSC),
		SUM(JTTXSC), SUM(W5 * JTTXSC)
		--,SUM(W5 * SD),
		--SUM(W5 * STTXSC)
from tfwm_nts_secureschema.trip T

left join tfwm_nts_secureschema.individual as I
on  I.individualid = T.individualid

left join tfwm_nts_secureschema.PSU as P
on T.PSUID = P.PSUID

left join tfwm_nts_secureschema.Household as H
on T.householdid = H.householdid
 
/*left join
nts.stage S
on T.TripID = S.TripID
where S.StageMain_B01ID = 1 --main stage only*/
 
where _minDistanceThresholdToExclude<0 or t.TripDisIncSW<=_minDistanceThresholdToExclude

group by SurveyYear_B01ID, 
 		P.surveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id,
 		HHoldOSLAUA_B01ID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = MainMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = MainMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE MainMode_B04ID
		END,
 		MainMode_B11ID,
 		I.Age_B01ID,
 		MOD((h.TWSWeekday_B01ID-1) + (t.TravDay-1),7) +1 
 		--don't have the 'day' table loaded, so work out the day of the week from the day of the week from the travel diary start day and offset by the day number within the diary - need to convert all operands from 1 based to zero based measures, apply modulo, convert back to 1 based.
),


cteTrips (yearID, surveyYear, countryID, statsregID, mmID, ageID, TripDayID,
		Trips_unweighted , Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted)
as
(select yearID, surveyYear, countryID, statsregID, mmID, ageID, TripDayID,
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
from cteTripBase
-- where TripDayID in (1,2,3,4,5) --mon-fri
group by yearID, surveyYear, countryID, statsregID, mmID, ageID, TripDayID 

union all

--seperate out 'long' walks
select yearID, surveyYear, countryID, statsregID, _dummyModeIdValue, ageID, TripDayID,
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
from cteTripBase
where MainMode_B11ID=2
-- and TripDayID in (1,2,3,4,5) --mon-fri
/*where t.MainMode_B04ID in (1) -- walking trips >=1 mile
and JJXSC != 0
and (JD/cast(JJXSC as float))>1.0
--AND JD>=1.0*/

group by yearID, surveyYear, countryID, statsregID, ageID, TripDayID
),


cteLaTrips (yearID, surveyYear, laID, mmID, ageID, TripDayID, 
		Trips_unweighted , Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
)
as
(select yearID, surveyYear, laID, mmID, ageID, TripDayID,
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
from cteTripBase
--where TripDayID in (1,2,3,4,5) --mon-fri
group by yearID, surveyYear, laID, mmID, ageID, TripDayID 

union all

--seperate out 'long' walks
select yearID, surveyYear, laID, _dummyModeIdValue, ageID, TripDayID,
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
from cteTripBase
where MainMode_B11ID=2
 --and TripDayID in (1,2,3,4,5) --mon-fri
/*where t.MainMode_B04ID in (1) -- walking trips >=1 mile
and JJXSC != 0
and (JD/cast(JJXSC as float))>1.0
--AND JD>=1.0*/

group by yearID, surveyYear, laID, ageID, TripDayID
),


cteStageBase (yearID, surveyYear, countryID, statsregID, laID,
		smID, StageMode_B11ID, ageID, TripDayID,
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
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END, 
		StageMode_B11ID,
		I.Age_B01ID,
		MOD((h.TWSWeekday_B01ID-1) + (t.TravDay-1),7) +1, 
 		--don't have the 'day' table loaded, so work out the day of the week from the day of the week from the travel diary start day and offset by the day number within the diary - need to convert all operands from 1 based to zero based measures, apply modulo, convert back to 1 based.	
		SUM(SSXSC), SUM(W5 * SSXSC),
		SUM(W5 * SD),
		SUM(W5 * STTXSC),
		SUM(W5 * SSXSC * CASE WHEN -8 = numboardings THEN 1 ELSE numboardings END)
	      --assume number of boardings is one if question not answered / not applicable
from 
tfwm_nts_secureschema.stage S

left join tfwm_nts_secureschema.individual as I
on  I.individualid = S.individualid
	
left join tfwm_nts_secureschema.PSU as P
on S.PSUID = P.PSUID

left join tfwm_nts_secureschema.trip T
on s.TripID = t.TripID

left join tfwm_nts_secureschema.Household as H
on T.householdid = H.householdid
	
where _minDistanceThresholdToExclude<0 or s.StageDistance<=_minDistanceThresholdToExclude

group by SurveyYear_B01ID, 
		P.surveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id,
		HHoldOSLAUA_B01ID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END,
		StageMode_B11ID,
		I.Age_B01ID,
		MOD((h.TWSWeekday_B01ID-1) + (t.TravDay-1),7) +1 
 		--don't have the 'day' table loaded, so work out the day of the week from the day of the week from the travel diary start day and offset by the day number within the diary - need to convert all operands from 1 based to zero based measures, apply modulo, convert back to 1 based.
),


cteStages (yearID, surveyYear, countryID, statsregID, smID, ageID, TripDayID,
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted
)
as
(
select yearID, surveyYear, countryID, statsregID, smID, ageID, TripDayID,
		sum(Stages_unweighted) , sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted),
		sum(Boardings_weighted)
from cteStageBase
group by yearID, surveyYear, countryID, statsregID, smID, ageID, TripDayID   

union all

--seperate out 'long' walks
select yearID, surveyYear, countryID, statsregID, _dummyModeIdValue, ageID, TripDayID,
		sum(Stages_unweighted) , sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted),
		sum(Boardings_weighted)
from cteStageBase
where StageMode_B11ID=2
group by yearID, surveyYear, countryID, statsregID, ageID, TripDayID   
),



cteLaStages (yearID, surveyYear, laID, smID, ageID, TripDayID,
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted
)
as
(
select yearID, surveyYear, laID, smID, ageID, TripDayID,
		sum(Stages_unweighted) , sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted),
		sum(Boardings_weighted)
from cteStageBase
group by yearID, surveyYear, laID, smID, ageID, TripDayID   

union all

--seperate out 'long' walks
select yearID, surveyYear, laID, _dummyModeIdValue, ageID, TripDayID,
		sum(Stages_unweighted) , sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted),
		sum(Boardings_weighted)
from cteStageBase
where StageMode_B11ID=2
group by yearID, surveyYear, laID, ageID, TripDayID  
),


cteXyrs (yearID, 
		 fromYear,
		 toYear,
		 countryID, statsregID, mmID, ageID, TripDayID,
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
		COALESCE(S.smID, T.mmID),
		COALESCE(S.ageID, T.ageID),
		COALESCE(S.TripDayID, T.TripDayID),
		sum(T.Trips_unweighted), sum(T.Trips_weighted),
		sum(T.TripDistance_unweighted), sum(T.TripDistance_weighted),
		sum(T.TripDuration_unweighted), sum(T.TripDuration_weighted),
		sum(T.TripTravelTime_unweighted), sum(T.TripTravelTime_weighted),
		--sum(T.MainStageDistance_weighted), sum(T.MainStageTravelTime_weighted,
		sum(S.Stages_unweighted), sum(S.Stages_weighted),
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
		on S.yearID = T.yearID and S.countryID = T.countryID and S.statsregID = T.statsregID and S.smID = T.mmID and S.ageID = T.ageID and S.TripDayID = T.TripDayID
		
	where
		(_skipCovidYears!=1 OR S.surveyYear< cy.minCovid OR S.surveyYear> cy.maxCovid)
	
group by L.SurveyYear_B01ID, S.countryID, S.statsregID, COALESCE(S.smID, T.mmID), COALESCE(S.ageID, T.ageID), COALESCE(S.TripDayID, T.TripDayID) 
),

cteXyrsAllDays (yearID, 
		 fromYear,
		 toYear,
		 countryID, statsregID, mmID, --ageID,
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
(select yearID, 
		min(fromYear),
		max(toYear),	
 countryID, statsregID, mmID, --ageID, 
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
group by yearID, countryID, statsregID, mmID--, ageID
),


cteXyrsMonFriDays (yearID, 
		 fromYear,
		 toYear,
		 countryID, statsregID, mmID, --ageID,
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
(select yearID, 
		min(fromYear),
		max(toYear),	
 countryID, statsregID, mmID, --ageID, 
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
where TripDayID in (1,2,3,4,5)
group by yearID, countryID, statsregID, mmID--, ageID
),


cteLaXyrs (yearID, 
		fromYear,
		toYear,		   
		laID, mmID, ageID, TripDayID,
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
		COALESCE(S.smID, T.mmID), 
		COALESCE(S.ageID, T.ageID),
		COALESCE(S.TripDayID, T.TripDayID),
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
		on S.yearID = T.yearID and S.laID = T.laID and S.smID = T.mmID and S.ageID = T.ageID and S.TripDayID = T.TripDayID

where
	(_skipCovidYears!=1 OR S.surveyYear< cy.minCovid OR S.surveyYear> cy.maxCovid)	
	
group by L.SurveyYear_B01ID, S.laID, COALESCE(S.smID, T.mmID), COALESCE(S.ageID, T.ageID), COALESCE(S.TripDayID, T.TripDayID)

),


cteLaXyrsAllDays (yearID, 
		 fromYear,
		 toYear,
		 laID, mmID, --ageID,
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
(select yearID, 
		min(fromYear),
		max(toYear),	
 laID, mmID, --ageID, 
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
cteLaXyrs
group by yearID, laID, mmID--, ageID
),


cteLaXyrsMonFriDays (yearID, 
		 fromYear,
		 toYear,
		 laID, mmID, --ageID,
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
(select yearID, 
		min(fromYear),
		max(toYear),	
 laID, mmID, --ageID, 
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
cteLaXyrs
where TripDayID in (1,2,3,4,5)
group by yearID, laID, mmID--, ageID
),






cteXyrsAllRegions (yearID, 
		fromYear,
		toYear,			   
		countryID, mmID, ageID, TripDayID,
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
 countryID, mmID, ageID, TripDayID,
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
group by yearID, countryID, mmID, ageID, TripDayID
),

cteXyrsAllRegionsAllDays (yearID, 
		fromYear,
		toYear,			   
		countryID, mmID, --ageID, 
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
 		countryID, mmID, --ageID, 
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
group by yearID, countryID, mmID--, ageID
),

cteXyrsAllRegionsMonFriDays (yearID, 
		fromYear,
		toYear,			   
		countryID, mmID, --ageID, 
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
 		countryID, mmID, --ageID, 
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
where TripDayID in (1,2,3,4,5)
group by yearID, countryID, mmID--, ageID
),





--W0	Unweighted interview sample(Household)
--W3	Interview sample household weight (Household)
--W1	Unweighted diary sample(Household)
--W2	Diary sample household weight (Household)
--W5	Trip/Stage weight (Trip)
--W4	LDJ weight (LDJ)
--W6	Attitudes weight(Attitudes)

--want to pick out individuals that have made a trip on a particular mode (in the survey week)
cteIndividualWithTrip(yearID, surveyYear, countryID, statsregID, laID, mmID, ageID, individualID, householdID,
		  Individual_unweighted, Individual_weighted, 
		  Trips_unweighted, Trips_weighted, 
		  Stages_unweighted, Stages_weighted,
		  StageDistance_weighted, StageTravelTime_weighted, Boardings_weighted,
		numVehicles)
as
(
SELECT  
	SurveyYear_B01ID, 
	P.surveyYear,
	CASE WHEN psucountry_b01id = -10 THEN 1
		WHEN psucountry_b01id isnull THEN 1
		 ELSE psucountry_b01id
	END,
	psustatsreg_b01id, 
	HHoldOSLAUA_B01ID,
	COALESCE(S.smID, T.mmID), 
	Age_B01ID,
	
	I.individualID,
	I.householdid,
	W1, W2,
	COALESCE(T.Trips_unweighted,0), COALESCE(T.Trips_weighted,0),
	COALESCE(S.Stages_unweighted,0), COALESCE(S.Stages_weighted,0),
	COALESCE(S.StageDistance_weighted,0), COALESCE(S.StageTravelTime_weighted,0), COALESCE(S.Boardings_weighted,0),
	COALESCE(vehCount.numVehicles,0)
FROM 
tfwm_nts_secureschema.individual as I

--join trips
left join 
(SELECT individualID, 
	CASE WHEN 1 = _combineLocalBusModes and 7 = MainMode_B04ID THEN 8 --force 'london bus' to 'local bus'
		WHEN 1 = _combineUndergroundIntoOther and 10 = MainMode_B04ID THEN 13 --force 'london underground' to 'other PT'
		ELSE MainMode_B04ID
	END mmID, 
 	SUM(JJXSC) Trips_unweighted, SUM(W5 * JJXSC) Trips_weighted
 FROM tfwm_nts_secureschema.trip
 where _minDistanceThresholdToExclude<0 or TripDisIncSW<=_minDistanceThresholdToExclude
 GROUP BY individualID,
 	CASE WHEN 1 = _combineLocalBusModes and 7 = MainMode_B04ID THEN 8 --force 'london bus' to 'local bus'
		WHEN 1 = _combineUndergroundIntoOther and 10 = MainMode_B04ID THEN 13 --force 'london underground' to 'other PT'
		ELSE MainMode_B04ID
	END
) as T		
on  I.individualid = T.individualid

--join stages
left join 
(SELECT S.individualID, 
	CASE WHEN 1 = _combineLocalBusModes and 7 = MainMode_B04ID THEN 8 --force 'london bus' to 'local bus'
		WHEN 1 = _combineUndergroundIntoOther and 10 = MainMode_B04ID THEN 13 --force 'london underground' to 'other PT'
		ELSE StageMode_B04ID
	END smID, 
		SUM(SSXSC) Stages_unweighted, SUM(W5 * SSXSC) Stages_weighted,
		SUM(W5 * SD) StageDistance_weighted,
		SUM(W5 * STTXSC) StageTravelTime_weighted,
		SUM(W5 * SSXSC * CASE WHEN -8 = numboardings THEN 1 ELSE numboardings END) Boardings_weighted
	      --assume number of boardings is one if question not answered / not applicable
 
 FROM tfwm_nts_secureschema.stage S
 LEFT OUTER JOIN tfwm_nts_secureschema.trip T ON S.tripid=T.tripid 
 where _minDistanceThresholdToExclude<0 or s.StageDistance<=_minDistanceThresholdToExclude
 GROUP BY S.individualID,
 	CASE WHEN 1 = _combineLocalBusModes and 7 = MainMode_B04ID THEN 8 --force 'london bus' to 'local bus'
		WHEN 1 = _combineUndergroundIntoOther and 10 = MainMode_B04ID THEN 13 --force 'london underground' to 'other PT'
		ELSE StageMode_B04ID
	END
) as S		
on  I.individualid = S.individualid
AND S.smID = T.mmID
	
	
	
left join tfwm_nts_secureschema.PSU as P
on I.PSUID = P.PSUID
	
left join tfwm_nts_secureschema.Household as H
on I.householdid = H.householdid
	

--we could view vehicles as being either available to the household, or to the individual, depending on what type of analysis we want to carry out (especially for school travel)	

--*** household car
left join 	
(select householdid, count(*) numVehicles from tfwm_nts_secureschema.vehicle where vehavail_b01id in (1,3) group by householdid) as vehCount
on I.householdid = vehCount.householdid
/*
--*** individual car	
--If the vehicle has a main driver, the IndividualID is provided, and will be blank otherwise. only 0.4% of vehicles do not have a IndividualID
left join 	
(select
 case when individualID='NA' then null else cast(individualID as bigint) end individualID, count(*) numVehicles 
 from tfwm_nts_secureschema.vehicle where vehavail_b01id in (1,3) group by case when individualID='NA' then null else cast(individualID as bigint) end) as vehCount
on I.individualID = vehCount.individualID
*/
	
),


cteIndividuals (yearID, countryID, statsregID, --ageID, 
				Individuals_unweighted, Individuals_weighted, 
				Trips_unweighted, Trips_weighted,
  				Stages_unweighted, Stages_weighted,
  				StageDistance_weighted, StageTravelTime_weighted, 
  				Boardings_weighted)
as
(SELECT  yearID, countryID, statsregID, --ageID, 
 sum(Individual_unweighted), sum(Individual_weighted), 
  sum(Trips_unweighted) , sum(Trips_weighted) , 
  sum(Stages_unweighted) , sum(Stages_weighted) ,
  sum(StageDistance_weighted) , sum(StageTravelTime_weighted) , 
  sum(Boardings_weighted) 
 
 FROM 
 --add up all the trips for an individual across all modes
 (SELECT  yearID, countryID, statsregID, ageID, 
  max(Individual_unweighted) Individual_unweighted, max(Individual_weighted) Individual_weighted, 
  sum(Trips_unweighted) Trips_unweighted, sum(Trips_weighted) Trips_weighted ,
  sum(Stages_unweighted) Stages_unweighted, sum(Stages_weighted) Stages_weighted,
  sum(StageDistance_weighted) StageDistance_weighted, sum(StageTravelTime_weighted) StageTravelTime_weighted, 
  sum(Boardings_weighted) Boardings_weighted

  FROM cteIndividualWithTrip
 GROUP BY yearID, countryID, statsregID, ageID, individualID) as I
 
 GROUP BY yearID, countryID, statsregID--, ageID
),

cteLaIndividuals (yearID, laID, --ageID, 
				Individuals_unweighted, Individuals_weighted, 
				Trips_unweighted, Trips_weighted,
  				Stages_unweighted, Stages_weighted,
  				StageDistance_weighted, StageTravelTime_weighted, 
  				Boardings_weighted)
as
(SELECT  yearID, laID, --ageID, 
  sum(Individual_unweighted), sum(Individual_weighted), 
  sum(Trips_unweighted) , sum(Trips_weighted),  
  sum(Stages_unweighted) , sum(Stages_weighted) ,
  sum(StageDistance_weighted) , sum(StageTravelTime_weighted) , 
  sum(Boardings_weighted) 
 FROM 
 --add up all the trips for an individual across all modes
 (SELECT  yearID, laID, ageID, 
  max(Individual_unweighted) Individual_unweighted, max(Individual_weighted) Individual_weighted, 
  sum(Trips_unweighted) Trips_unweighted, sum(Trips_weighted) Trips_weighted ,
  sum(Stages_unweighted) Stages_unweighted, sum(Stages_weighted) Stages_weighted,
  sum(StageDistance_weighted) StageDistance_weighted, sum(StageTravelTime_weighted) StageTravelTime_weighted, 
  sum(Boardings_weighted) Boardings_weighted
  FROM cteIndividualWithTrip
 GROUP BY yearID, laID, ageID, individualID) as I
 
 GROUP BY yearID, laID--, ageID
),

cteIndividualsAllRegions (yearID, countryID, --ageID, 
				Individuals_unweighted, Individuals_weighted, 
				Trips_unweighted, Trips_weighted,
  				Stages_unweighted, Stages_weighted,
  				StageDistance_weighted, StageTravelTime_weighted, 
  				Boardings_weighted)
as
(SELECT  yearID, countryID, --ageID, 
  sum(Individuals_unweighted), sum(Individuals_weighted), 
  sum(Trips_unweighted) , sum(Trips_weighted),  
  sum(Stages_unweighted) , sum(Stages_weighted) ,
  sum(StageDistance_weighted) , sum(StageTravelTime_weighted) , 
  sum(Boardings_weighted) 
 FROM cteIndividuals
 GROUP BY yearID, countryID--, ageID
),



cteIndividualsWithTrip(yearID, surveyYear, countryID, statsregID, mmID, --ageID, 
				Individuals_unweighted, Individuals_weighted, 
				Trips_unweighted, Trips_weighted,
  				Stages_unweighted, Stages_weighted,
  				StageDistance_weighted, StageTravelTime_weighted, 
  				Boardings_weighted)
as
(
	SELECT yearID, surveyYear, countryID, statsregID, mmID, --ageID,
  sum(Individual_unweighted), sum(Individual_weighted), 
  sum(Trips_unweighted) , sum(Trips_weighted),  
  sum(Stages_unweighted) , sum(Stages_weighted) ,
  sum(StageDistance_weighted) , sum(StageTravelTime_weighted) , 
  sum(Boardings_weighted) 

	FROM cteIndividualWithTrip
	WHERE Trips_unweighted >= _minUnweightedTripsPerWeek
		AND numVehicles >= _minNumVehicles
	GROUP BY yearID, surveyYear, countryID, statsregID, mmID--, ageID
),

cteIndividualsWithTripLa(yearID, surveyYear, countryID, laID, mmID, --ageID, 
				Individuals_unweighted, Individuals_weighted, 
				Trips_unweighted, Trips_weighted,
  				Stages_unweighted, Stages_weighted,
  				StageDistance_weighted, StageTravelTime_weighted, 
  				Boardings_weighted)
as
(
	SELECT yearID, surveyYear, countryID, laID, mmID, --ageID,
  sum(Individual_unweighted), sum(Individual_weighted), 
  sum(Trips_unweighted) , sum(Trips_weighted),  
  sum(Stages_unweighted) , sum(Stages_weighted) ,
  sum(StageDistance_weighted) , sum(StageTravelTime_weighted) , 
  sum(Boardings_weighted) 
	FROM cteIndividualWithTrip
	
/*	left outer join
	(Select yearID, surveyYear, countryID, laID, mmID, ageID,
	max(Individual_weighted)
	 FROM cteIndividualWithTrip
	 group by householdID
	)*/
	
	WHERE Trips_unweighted >= _minUnweightedTripsPerWeek
		AND numVehicles >= _minNumVehicles
	GROUP BY yearID, surveyYear, countryID, laID, mmID--, ageID
),

cteIndividualsWithTripAllRegions(yearID, surveyYear, countryID, mmID, --ageID, 
				Individuals_unweighted, Individuals_weighted, 
				Trips_unweighted, Trips_weighted,
  				Stages_unweighted, Stages_weighted,
  				StageDistance_weighted, StageTravelTime_weighted, 
  				Boardings_weighted)
as
(SELECT  yearID, surveyYear, countryID, mmID, --ageID, 
   sum(Individuals_unweighted), sum(Individuals_weighted), 
  sum(Trips_unweighted) , sum(Trips_weighted),  
  sum(Stages_unweighted) , sum(Stages_weighted) ,
  sum(StageDistance_weighted) , sum(StageTravelTime_weighted) , 
  sum(Boardings_weighted) 
 FROM cteIndividualsWithTrip
 
 GROUP BY yearID, surveyYear, countryID, mmID--, ageID
),




cteXyrsIndividuals(yearID, countryID, statsregID, --ageID, 
				   mmId,
				   total_Individuals_unweighted, total_Individuals_weighted,
				  Individuals_unweighted, Individuals_weighted, 
				Trips_unweighted, Trips_weighted,
  				Stages_unweighted, Stages_weighted,
  				StageDistance_weighted, StageTravelTime_weighted, 
  				Boardings_weighted)
as
(select sy.SurveyYear_B01ID, i.countryID, i.statsregID, --i.ageID, 
 ml.MainMode_B04ID, 
 sum(I.Individuals_unweighted), sum(I.Individuals_weighted),
 sum(COALESCE(IWT.Individuals_unweighted,0)), sum(COALESCE(IWT.Individuals_weighted,0)),
 sum(COALESCE(IWT.Trips_unweighted,0)), sum(COALESCE(IWT.Trips_weighted,0)),
 sum(COALESCE(IWT.Stages_unweighted,0)), sum(COALESCE(IWT.Stages_weighted,0)),
 sum(COALESCE(IWT.StageDistance_weighted,0)), sum(COALESCE(IWT.StageTravelTime_weighted,0)),
 sum(COALESCE(IWT.Boardings_weighted,0))
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
 
 	inner join ctaFromToYears fty
	on sy.SurveyYear_B01ID = fty.toYearId
	cross join cteCovidYears cy

	left join 
	cteIndividuals as I
 		on I.yearID >= fty.fromYearId and I.yearID <= fty.toYearId
 
 	cross join cteModeLabel ml 

 	left join
	cteIndividualsWithTrip as IWT
 		on I.yearID=IWT.yearID AND I.countryID=IWT.countryID AND I.statsregID=IWT.statsregID AND ml.MainMode_B04ID=IWT.mmID --AND I.ageID=IWT.ageID
 
where
 sy.SurveyYear_B01ID >=0 AND
	(_skipCovidYears!=1 OR cast(Sy.description as int)< cy.minCovid OR cast(Sy.description as int)> cy.maxCovid)
 
group by sy.SurveyYear_B01ID, i.countryID, i.statsregID, --i.ageID, 
 ml.MainMode_B04ID
),


cteLaXyrsIndividuals(yearID, laID, --ageID, 
					 mmId,
					 total_Individuals_unweighted, total_Individuals_weighted,
					 Individuals_unweighted, Individuals_weighted, 
				Trips_unweighted, Trips_weighted,
  				Stages_unweighted, Stages_weighted,
  				StageDistance_weighted, StageTravelTime_weighted, 
  				Boardings_weighted)
as
(select sy.SurveyYear_B01ID, i.laID, --i.ageID, 
 ml.MainMode_B04ID, 
 sum(I.Individuals_unweighted), sum(I.Individuals_weighted),
 sum(COALESCE(IWT.Individuals_unweighted,0)), sum(COALESCE(IWT.Individuals_weighted,0)),
 sum(COALESCE(IWT.Trips_unweighted,0)), sum(COALESCE(IWT.Trips_weighted,0)),
 sum(COALESCE(IWT.Stages_unweighted,0)), sum(COALESCE(IWT.Stages_weighted,0)),
 sum(COALESCE(IWT.StageDistance_weighted,0)), sum(COALESCE(IWT.StageTravelTime_weighted,0)),
 sum(COALESCE(IWT.Boardings_weighted,0))
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
 
 	inner join ctaFromToYears fty
	on sy.SurveyYear_B01ID = fty.toYearId
	cross join cteCovidYears cy
 
	left join 
	cteLaIndividuals as I
 		on I.yearID >= fty.fromYearId and I.yearID <= fty.toYearId

 	cross join cteModeLabel ml 
 
 	left join
	cteIndividualsWithTripLa as IWT
 		on I.yearID=IWT.yearID AND I.laID=IWT.laID AND ml.MainMode_B04ID=IWT.mmID --AND I.ageID=IWT.ageID
 
 where
 sy.SurveyYear_B01ID >=0 AND
	(_skipCovidYears!=1 OR cast(Sy.description as int)< cy.minCovid OR cast(Sy.description as int)> cy.maxCovid)
 
group by sy.SurveyYear_B01ID, i.laID, --i.ageID, 
 ml.MainMode_B04ID
),


cteDebugDayOfWeek(
yearID, surveyYear, /*countryID, statsregID,*/ TripDayID,
Trips_unweighted , Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted

) as 
(select
yearID, surveyYear, /*countryID, statsregID,*/ TripDayID,
sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
from 
cteTripBase
group by yearID, surveyYear, /*countryID, statsregID,*/ TripDayID
),


cteXyrsIndividualsAllRegions(yearID, countryID, --ageID, 
							 mmId,
				   total_Individuals_unweighted, total_Individuals_weighted,
				  Individuals_unweighted, Individuals_weighted, 
				Trips_unweighted, Trips_weighted,
  				Stages_unweighted, Stages_weighted,
  				StageDistance_weighted, StageTravelTime_weighted, 
  				Boardings_weighted)
as
(select sy.SurveyYear_B01ID, i.countryID, --i.ageID, 
 ml.MainMode_B04ID, 
 sum(I.Individuals_unweighted), sum(I.Individuals_weighted),
 sum(COALESCE(IWT.Individuals_unweighted,0)), sum(COALESCE(IWT.Individuals_weighted,0)),
 sum(COALESCE(IWT.Trips_unweighted,0)), sum(COALESCE(IWT.Trips_weighted,0)),
 sum(COALESCE(IWT.Stages_unweighted,0)), sum(COALESCE(IWT.Stages_weighted,0)),
 sum(COALESCE(IWT.StageDistance_weighted,0)), sum(COALESCE(IWT.StageTravelTime_weighted,0)),
 sum(COALESCE(IWT.Boardings_weighted,0))
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
 
 	inner join ctaFromToYears fty
	on sy.SurveyYear_B01ID = fty.toYearId
	cross join cteCovidYears cy

	left join 
	cteIndividualsAllRegions as I
 		on I.yearID >= fty.fromYearId and I.yearID <= fty.toYearId

 	cross join cteModeLabel ml 
 
 	left join
	cteIndividualsWithTripAllRegions as IWT
 		on I.yearID=IWT.yearID AND I.countryID=IWT.countryID AND ml.MainMode_B04ID=IWT.mmID --AND I.ageID=IWT.ageID 
 
where
 sy.SurveyYear_B01ID >=0 AND
	(_skipCovidYears!=1 OR cast(Sy.description as int)< cy.minCovid OR cast(Sy.description as int)> cy.maxCovid)
 
group by sy.SurveyYear_B01ID, i.countryID, --i.ageID, 
 ml.MainMode_B04ID
),


finalQuery as
(
-- select query
select  
fty.fromyear "start year", 
fty.toyear "end year", 

StatsRegDesc "region",
--AgeDesc "age",
mmDesc "mode",
L.mmID "modeId",
--L.ageID "ageId",
	total_Individuals_unweighted as total_Individuals_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
	cast(round(cast(total_Individuals_weighted as numeric),2)as float) as total_Individuals_Weighted,
	cast(round(cast(Individuals_weighted as numeric),2)as float) as Individuals_Weighted,

	
	T.Trips_unweighted as Trips_UNweighted,
	T.Trips_weighted as Trips_weighted,
	WT.Trips_weighted as TripsMF_weighted,
--	cast(round(Trips_weighted,2)as float) as Trips_Weighted, 
	T.Stages_unweighted as Stages_UNweighted,
	
--round( cast(Trips_unweighted as float)* _weekToYearCorrectionFactor / cast(Individuals_unweighted as float), 3 ) "UNweighted tripRate",	
	
cast(round( cast(T.Trips_weighted* _weekToYearCorrectionFactor / total_Individuals_weighted as numeric), 3 )as float) "weighted tripRate population (0303a)",
cast(round( cast(WT.Trips_weighted* _weekToYearCorrectionFactor / total_Individuals_weighted as numeric), 3 )as float) "weighted tripRateMF population ()",
	
CASE WHEN Individuals_weighted=0 THEN null ELSE cast(round( cast(I.Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) END  "weighted tripRate using mode (unpublished)",
	
cast(round( cast(T.Stages_weighted* _weekToYearCorrectionFactor / total_Individuals_weighted as numeric), 3 )as float) "weighted stageRate population (0303b)",

cast(round( cast(T.Boardings_weighted* _weekToYearCorrectionFactor / total_Individuals_weighted as numeric), 3 )as float) "weighted boardingRate population (unpublished)",
CASE WHEN Individuals_weighted=0 THEN null ELSE cast(round( cast(I.Boardings_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) END  "weighted boardingRate using mode (unpublished)",

cast(round( cast(T.StageDistance_weighted * _weekToYearCorrectionFactor / total_Individuals_weighted as numeric), 3 )as float) "total stage distance per-person-per-year (miles)(0303c)",

--cast(round( cast(T.TripDistance_weighted/T.Trips_weighted as numeric), 3 )as float) "mean tripDistance population (miles)(0303d)",

cast(round( cast(T.TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / total_Individuals_weighted as numeric), 3 )as float) "mean tripDuration per-person-per-year (hours)(0303e)",

--cast(round( cast(T.TripDuration_weighted/T.Trips_weighted as numeric), 3 )as float) "mean tripDuration (minutes)(0303f)",

cast(round( cast(T.StageTravelTime_weighted * _weekToYearCorrectionFactor / 60.0 / total_Individuals_weighted as numeric), 3 )as float) "total stg travel tm (in veh) p-pers-p-year (hours)(unpublished)"
	
	
from 
	cteLabels as L
	inner join ctaFromToYears fty
	on L.yearID = fty.toYearId
	
	left join
	cteXyrsIndividuals as I
		on L.yearID = I.yearID
		and L.countryID = I.countryID
		and L.StatsRegID = I.statsregID
		and L.mmID = I.mmID 
		--and L.ageID = I.ageID
	left join
	cteXyrsAllDays as T
		on L.yearID = T.yearID
		and L.countryID = T.countryID
		and L.StatsRegID = T.statsregID
		and L.mmID = T.mmID 
		--and L.ageID = T.ageID
	left join
	cteXyrsMonFriDays as WT
		on L.yearID = WT.yearID
		and L.countryID = WT.countryID
		and L.StatsRegID = WT.statsregID
		and L.mmID = WT.mmID 
		--and L.ageID = WT.ageID

WHERE
	(L.statsregID=_statsregID or L.statsregID is null or 0=_statsregID)
	--and 	(fty.fromyear = 2003 or fty.fromyear=2012)


union 

select  
fty.fromyear "start year", 
fty.toyear "end year", 

CountryDesc "country",
--AgeDesc "age",
mmDesc "mode",
L.mmID "modeId",
--L.ageID "ageId",	
	total_Individuals_unweighted as total_Individuals_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
	cast(round(cast(total_Individuals_weighted as numeric),2)as float) as total_Individuals_Weighted,		
	cast(round(cast(Individuals_weighted as numeric),2)as float) as Individuals_Weighted,	

	
	T.Trips_unweighted as Trips_UNweighted,
	T.Trips_weighted as Trips_weighted,
	WT.Trips_weighted as TripsMF_weighted,
--	cast(round(Trips_weighted,2)as float) as Trips_Weighted, 
	T.Stages_unweighted as Stages_UNweighted,
	
--round( cast(Trips_unweighted as float)* _weekToYearCorrectionFactor / cast(Individuals_unweighted as float), 3 ) "UNweighted tripRate",	
	
cast( (T.Trips_weighted* _weekToYearCorrectionFactor / total_Individuals_weighted) as float) "weighted tripRate population (0303a)",
cast( (WT.Trips_weighted* _weekToYearCorrectionFactor / total_Individuals_weighted) as float) "weighted tripRateMF population ()",
CASE WHEN Individuals_weighted=0 THEN null ELSE cast( (I.Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted) as float) END "weighted tripRate using mode (unpublished)",

cast(( T.Stages_weighted* _weekToYearCorrectionFactor / total_Individuals_weighted )as float) "weighted stageRate population (0303b)",

cast(( T.Boardings_weighted* _weekToYearCorrectionFactor / total_Individuals_weighted )as float) "weighted boardingRate population (unpublished)",
CASE WHEN Individuals_weighted=0 THEN null ELSE cast(( I.Boardings_weighted* _weekToYearCorrectionFactor / Individuals_weighted )as float) END "weighted boardingRate using mode (unpublished)",

cast(( T.StageDistance_weighted * _weekToYearCorrectionFactor / total_Individuals_weighted )as float) "total stage distance per-person-per-year (miles)(0303c)",

--cast(( T.TripDistance_weighted/T.Trips_weighted )as float) "mean tripDistance (miles)(0303d)",
	
cast(( T.TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / total_Individuals_weighted )as float) "mean tripDuration per-person-per-year (hours)(0303e)",

--cast(( T.TripDuration_weighted/T.Trips_weighted )as float) "mean tripDuration (minutes)(0303f)",

cast(( T.StageTravelTime_weighted * _weekToYearCorrectionFactor / 60.0 / total_Individuals_weighted)as float) "total stg travel tm (in veh) p-pers-p-year (hours)(unpublished)"
	
	
from 
	cteCountryLabels as L
	inner join ctaFromToYears fty
	on L.yearID = fty.toYearId

	left join
	cteXyrsIndividualsAllRegions as I
		on L.yearID = I.yearID
		and L.countryID = I.countryID
		and L.mmID = I.mmID 
		--and L.ageID = I.ageID	
	left join
	cteXyrsAllRegionsAllDays as T
		on L.yearID = T.yearID
		and L.countryID = T.countryID
		and L.mmID = T.mmID 
		--and L.ageID = T.ageID
	left join
	cteXyrsAllRegionsMonFriDays as WT
		on L.yearID = WT.yearID
		and L.countryID = WT.countryID
		and L.mmID = WT.mmID 
		--and L.ageID = WT.ageID

--WHERE
--		(fty.fromyear = 2003 or fty.fromyear=2012)


union


select  
fty.fromyear "start year", 
fty.toyear "end year", 

LaDesc "region",
--AgeDesc "age",
mmDesc "mode",
L.mmID "modeId",
--L.ageID "ageId",
	
	total_Individuals_unweighted as total_Individuals_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
	cast(round(cast(total_Individuals_weighted as numeric),2)as float) as total_Individuals_Weighted,
	cast(round(cast(Individuals_weighted as numeric),2)as float) as Individuals_Weighted,

	T.Trips_unweighted as Trips_UNweighted,
	T.Trips_weighted as Trips_weighted,
	WT.Trips_weighted as TripsMF_weighted,
--	cast(round(Trips_weighted,2)as float) as Trips_Weighted, 
	T.Stages_unweighted as Stages_UNweighted,
	
--round( cast(Trips_unweighted as float)* _weekToYearCorrectionFactor / cast(Individuals_unweighted as float), 3 ) "UNweighted tripRate",	
	
cast(round( cast(T.Trips_weighted* _weekToYearCorrectionFactor / total_Individuals_weighted as numeric), 3 )as float) "weighted tripRate population (0303a)",
cast(round( cast(WT.Trips_weighted* _weekToYearCorrectionFactor / total_Individuals_weighted as numeric), 3 )as float) "weighted tripRateMF population ()",
CASE WHEN Individuals_weighted=0 THEN null ELSE cast(round( cast(I.Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) END "weighted tripRate using mode (unpublished)",

cast(round( cast(T.Stages_weighted* _weekToYearCorrectionFactor / total_Individuals_weighted as numeric), 3 )as float) "weighted stageRate population (0303b)",

cast(round( cast(T.Boardings_weighted* _weekToYearCorrectionFactor / total_Individuals_weighted as numeric), 3 )as float) "weighted boardingRate population (unpublished)",
CASE WHEN Individuals_weighted=0 THEN null ELSE cast(round( cast(I.Boardings_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) END "weighted boardingRate using mode (unpublished)",

cast(round( cast(T.StageDistance_weighted * _weekToYearCorrectionFactor / total_Individuals_weighted as numeric), 3 )as float) "total stage distance per-person-per-year (miles)(0303c)",

--cast(round( cast(T.TripDistance_weighted/T.Trips_weighted as numeric), 3 )as float) "mean tripDistance (miles)(0303d)",

cast(round( cast(T.TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / total_Individuals_weighted as numeric), 3 )as float) "mean tripDuration per-person-per-year (hours)(0303e)",

--cast(round( cast(T.TripDuration_weighted/T.Trips_weighted as numeric), 3 )as float) "mean tripDuration (minutes)(0303f)",

cast(round( cast(T.StageTravelTime_weighted * _weekToYearCorrectionFactor / 60.0 / total_Individuals_weighted as numeric), 3 )as float) "total stg travel tm (in veh) p-pers-p-year (hours)(unpublished)"
	
from 
	cteLaLabels as L
	inner join ctaFromToYears fty
	on L.yearID = fty.toYearId
	
	left join
	cteLaXyrsIndividuals as I
		on L.yearID = I.yearID
		and L.laID = I.laID
		and L.mmID = I.mmID 
		--and L.ageID = I.ageID
	left join
	cteLaXyrsAllDays as T
		on L.yearID = T.yearID
		and L.laID = T.laID
		and L.mmID = T.mmID 
		--and L.ageID = T.ageID
	left join
	cteLaXyrsMonFriDays as WT
		on L.yearID = WT.yearID
		and L.laID = WT.laID
		and L.mmID = WT.mmID 
		--and L.ageID = WT.ageID
	
where 
	(0 != _statsregID)
--	and 	(fty.fromyear = 2003 or fty.fromyear=2012)
order by 1,2,3,5
)


select * from finalQuery;
--select * from cteIndividualWithTrip where statsregid=10 and yearid=28 and mmid=8;
--select * from cteDebugDayOfWeek order by 1,2,3,4,5;

end;
$$;
 
select * from __temp_table;
 
--can't drop the temp table here otherwise I don't get any output from the select statement in the pgadmin window