/*=================================================================================

NTS0601

short walk: MainMode_B02ID = 1 (replaced by MainMode_B11ID<>1)

	Owen O'Neill:	July 2023
	Owen O'Neill:	June 2024: updated to use restricted licence data.
	Owen O'Neill:   June 2024: added WMCA local authorities - watch out for sample size !
	Owen O'Neill:   June 2024: added option to skip covid years (2020+2021)
	Owen O'Neill:   November 2024: created from NTS0303 query	

=================================================================================*/
--use NTS;

DO $$
DECLARE

_numyears constant smallint = 9; --number of years to roll up averages (backwards from date reported in result row)

_statsregID constant  smallint = 0; --set to zero for all regions west midlands=10
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
			mmID, mmDesc,
		  ageID, ageDesc) 
as
(select distinct psu.SurveyYear_B01ID, psu.SurveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
 			 WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psu.PSUStatsReg_B01ID, statsRegLookup.description,
		mm.MainMode_B04ID, mm.description,
 		al.Age_B01ID, al.description
from 
	tfwm_nts_secureschema.psu psu
	left outer join 
	tfwm_nts_securelookups.PSUStatsReg_B01ID as statsRegLookup
	on psu.PSUStatsReg_B01ID = statsRegLookup.PSUStatsReg_B01ID
	cross join
	cteModeLabel mm
 	cross join
	tfwm_nts_securelookups.Age_B01ID al
 WHERE
 	statsRegLookup.part=1 and al.part=1 
),


cteCountryLabels (yearID, yearDesc,
			countryID, countryDesc,
			mmID, mmDesc,
		  ageID, ageDesc) 
as
(select distinct psu.SurveyYear_B01ID, psu.SurveyYear,
		CASE WHEN psu.psucountry_b01id = -10 THEN 1
 			WHEN psu.psucountry_b01id isnull THEN 1
			 ELSE psu.psucountry_b01id
		END,
		countryLookup.description,
		mm.MainMode_B04ID, mm.description,
 		al.Age_B01ID, al.description
from 
	tfwm_nts_secureschema.psu psu
	left outer join 
	tfwm_nts_securelookups.PSUCountry_B01ID as countryLookup
	on CASE WHEN psu.psucountry_b01id = -10 THEN 1
 			WHEN psu.psucountry_b01id isnull THEN 1
			 ELSE psu.psucountry_b01id
		END = countryLookup.PSUCountry_B01ID
	cross join
	cteModeLabel mm
    cross join
	tfwm_nts_securelookups.Age_B01ID al
 WHERE
 	countryLookup.part=1 and al.part=1
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
			mmID, mmDesc,
		  ageID, ageDesc) 
as
(select distinct psu.SurveyYear_B01ID, psu.SurveyYear,
		laLookup.Id,
		laLookup.description description,
		mm.MainMode_B04ID, mm.description,
 		al.Age_B01ID, al.description
from 
	tfwm_nts_secureschema.psu psu
	cross join
	lookup_HHoldOSLAUA_B01ID laLookup
	cross join
	cteModeLabel mm
 	cross join
	tfwm_nts_securelookups.Age_B01ID al
WHERE
 	al.part=1
),


--JJXSC The number of trips to be counted, grossed for short walks and excluding “Series of Calls” trips. 
--JD The distance of the trip (miles), grossed for short walks.
--JTTXSC The total travelling time of the trip (in minutes), grossed for short walks and excluding “Series of Calls” trips. 
--JOTXSC The overall duration of the trip (in minutes), meaning that it includes both the travelling and waiting times between stages, 
--  grossed for short walks and excluding “Series of Calls” trips.
cteTripBase (yearID, surveyYear, countryID, statsregID, laID,
		mmID, MainMode_B11ID, ageID,
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
 		I.Age_B01ID
),


cteTrips (yearID, surveyYear, countryID, statsregID, mmID, ageID, 
		Trips_unweighted , Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted)
as
(select yearID, surveyYear, countryID, statsregID, mmID, ageID, 
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
from cteTripBase
group by yearID, surveyYear, countryID, statsregID, mmID, ageID 

union all

--seperate out 'long' walks
select yearID, surveyYear, countryID, statsregID, _dummyModeIdValue, ageID,
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
from cteTripBase
where MainMode_B11ID=2
/*where t.MainMode_B04ID in (1) -- walking trips >=1 mile
and JJXSC != 0
and (JD/cast(JJXSC as float))>1.0
--AND JD>=1.0*/

group by yearID, surveyYear, countryID, statsregID, ageID
),


cteLaTrips (yearID, surveyYear, laID, mmID, ageID,  
		Trips_unweighted , Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
)
as
(select yearID, surveyYear, laID, mmID, ageID, 
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
from cteTripBase
group by yearID, surveyYear, laID, mmID, ageID 

union all

--seperate out 'long' walks
select yearID, surveyYear, laID, _dummyModeIdValue, ageID, 
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
from cteTripBase
where MainMode_B11ID=2
/*where t.MainMode_B04ID in (1) -- walking trips >=1 mile
and JJXSC != 0
and (JD/cast(JJXSC as float))>1.0
--AND JD>=1.0*/

group by yearID, surveyYear, laID, ageID
),


cteStageBase (yearID, surveyYear, countryID, statsregID, laID,
		smID, StageMode_B11ID, ageID,
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
		I.Age_B01ID
),


cteStages (yearID, surveyYear, countryID, statsregID, smID, ageID, 
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted
)
as
(
select yearID, surveyYear, countryID, statsregID, smID, ageID, 
		sum(Stages_unweighted) , sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted),
		sum(Boardings_weighted)
from cteStageBase
group by yearID, surveyYear, countryID, statsregID, smID, ageID   

union all

--seperate out 'long' walks
select yearID, surveyYear, countryID, statsregID, _dummyModeIdValue, ageID, 
		sum(Stages_unweighted) , sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted),
		sum(Boardings_weighted)
from cteStageBase
where StageMode_B11ID=2
group by yearID, surveyYear, countryID, statsregID, ageID   
),



cteLaStages (yearID, surveyYear, laID, smID, ageID, 
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted
)
as
(
select yearID, surveyYear, laID, smID, ageID, 
		sum(Stages_unweighted) , sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted),
		sum(Boardings_weighted)
from cteStageBase
group by yearID, surveyYear, laID, smID, ageID   

union all

--seperate out 'long' walks
select yearID, surveyYear, laID, _dummyModeIdValue, ageID, 
		sum(Stages_unweighted) , sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted),
		sum(Boardings_weighted)
from cteStageBase
where StageMode_B11ID=2
group by yearID, surveyYear, laID, ageID  
),


cteXyrs (yearID, 
		 fromYear,
		 toYear,
		 countryID, statsregID, mmID, ageID,
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
		on S.yearID = T.yearID and S.countryID = T.countryID and S.statsregID = T.statsregID and S.smID = T.mmID and S.ageID = T.ageID
		
	where
		(_skipCovidYears!=1 OR S.surveyYear< cy.minCovid OR S.surveyYear> cy.maxCovid)
	
group by L.SurveyYear_B01ID, S.countryID, S.statsregID, COALESCE(S.smID, T.mmID), COALESCE(S.ageID, T.ageID) 
),



cteLaXyrs (yearID, 
		fromYear,
		toYear,		   
		laID, mmID, ageID,
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
		on S.yearID = T.yearID and S.laID = T.laID and S.smID = T.mmID and S.ageID = T.ageID

where
	(_skipCovidYears!=1 OR S.surveyYear< cy.minCovid OR S.surveyYear> cy.maxCovid)	
	
group by L.SurveyYear_B01ID, S.laID, COALESCE(S.smID, T.mmID), COALESCE(S.ageID, T.ageID)
),


cteXyrsAllRegions (yearID, 
		fromYear,
		toYear,			   
		countryID, mmID, ageID,
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
 countryID, mmID, ageID,
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
group by yearID, countryID, mmID, ageID
),




--W0	Unweighted interview sample(Household)
--W3	Interview sample household weight (Household)
--W1	Unweighted diary sample(Household)
--W2	Diary sample household weight (Household)
--W5	Trip/Stage weight (Trip)
--W4	LDJ weight (LDJ)
--W6	Attitudes weight(Attitudes)
cteIndividualsBase (yearID, countryID, statsregID, laId, ageID, 
					Individuals_unweighted, Individuals_weighted)
as
(select SurveyYear_B01ID, 
	CASE WHEN psucountry_b01id = -10 THEN 1
 		WHEN psucountry_b01id isnull THEN 1
		 ELSE psucountry_b01id
	END,
	psustatsreg_b01id, 
 	HHoldOSLAUA_B01ID,
 	Age_B01ID,
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
 		HHoldOSLAUA_B01ID,
 		Age_B01ID
),


cteIndividuals (yearID, countryID, statsregID, ageID, Individuals_unweighted, Individuals_weighted)
as
(select yearID, countryID, statsregID, ageID, sum(Individuals_unweighted), sum(Individuals_weighted)
from cteIndividualsBase
group by yearID, countryID, statsregID, ageID
),

cteLaIndividuals (yearID, laID, ageID, Individuals_unweighted, Individuals_weighted)
as
(select yearID, laID, ageID, sum(Individuals_unweighted), sum(Individuals_weighted)
from cteIndividualsBase
group by yearID, laID, ageID
),


cteXyrsIndividuals(yearID, countryID, statsregID, ageID, Individuals_unweighted, Individuals_weighted)
as
(select sy.SurveyYear_B01ID, i.countryID, i.statsregID, i.ageID, sum(I.Individuals_unweighted), sum(I.Individuals_weighted)
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
 
 	inner join ctaFromToYears fty
	on sy.SurveyYear_B01ID = fty.toYearId
	cross join cteCovidYears cy

	left join 
	cteIndividuals as I
 		on I.yearID >= fty.fromYearId and I.yearID <= fty.toYearId
 
where
 sy.SurveyYear_B01ID >=0 AND
	(_skipCovidYears!=1 OR cast(Sy.description as int)< cy.minCovid OR cast(Sy.description as int)> cy.maxCovid)
 
group by sy.SurveyYear_B01ID, countryID, statsregID, ageID
),


cteLaXyrsIndividuals(yearID, laID, ageID, Individuals_unweighted, Individuals_weighted)
as
(select sy.SurveyYear_B01ID, i.laID, i.ageID, sum(I.Individuals_unweighted), sum(I.Individuals_weighted)
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
 
 	inner join ctaFromToYears fty
	on sy.SurveyYear_B01ID = fty.toYearId
	cross join cteCovidYears cy
 
	left join 
	cteLaIndividuals as I
 		on I.yearID >= fty.fromYearId and I.yearID <= fty.toYearId

 where
 sy.SurveyYear_B01ID >=0 AND
	(_skipCovidYears!=1 OR cast(Sy.description as int)< cy.minCovid OR cast(Sy.description as int)> cy.maxCovid)
 
group by sy.SurveyYear_B01ID, laID, ageID
),


cteXyrsIndividualsAllRegions(yearID, countryID, ageID, Individuals_unweighted, Individuals_weighted)
as
(select yearID, countryID, ageID, sum(Individuals_unweighted), sum(Individuals_weighted)
from 
	cteXyrsIndividuals
group by yearID, countryID, ageID
),


finalQuery as
(
-- select query
select  
fty.fromyear "start year", 
fty.toyear "end year", 

StatsRegDesc "region",
AgeDesc "age",
mmDesc "mode",
L.mmID "modeId",
L.ageID "ageId",
	Trips_unweighted as Trips_UNweighted,
--	cast(round(Trips_weighted,2)as float) as Trips_Weighted, 
	Stages_unweighted as Stages_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
	cast(round(cast(Individuals_weighted as numeric),2)as float) as Individuals_Weighted,
	
--round( cast(Trips_unweighted as float)* _weekToYearCorrectionFactor / cast(Individuals_unweighted as float), 3 ) "UNweighted tripRate",	
	
cast(round( cast(Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) "weighted tripRate (0303a)",

cast(round( cast(Stages_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) "weighted stageRate (0303b)",

cast(round( cast(Boardings_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) "weighted boardingRate (unpublished)",

cast(round( cast(StageDistance_weighted * _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) "total stage distance per-person-per-year (miles)(0303c)",

case when Trips_weighted>0 then cast(round( cast(TripDistance_weighted/Trips_weighted as numeric), 3 )as float) else null end "mean tripDistance (miles)(0303d)",

cast(round( cast(TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric), 3 )as float) "mean tripDuration per-person-per-year (hours)(0303e)",

case when Trips_weighted>0 then cast(round( cast(TripDuration_weighted/Trips_weighted as numeric), 3 )as float) else null end "mean tripDuration (minutes)(0303f)",

cast(round( cast(StageTravelTime_weighted * _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric), 3 )as float) "total stg travel tm (in veh) p-pers-p-year (hours)(unpublished)"

from 
	cteLabels as L
	inner join ctaFromToYears fty
	on L.yearID = fty.toYearId
	
	left join
	cteXyrsIndividuals as I
		on L.yearID = I.yearID
		and L.countryID = I.countryID
		and L.StatsRegID = I.statsregID
		and L.ageID = I.ageID
	left join
	cteXyrs as T
		on L.yearID = T.yearID
		and L.countryID = T.countryID
		and L.StatsRegID = T.statsregID
		and L.mmID = T.mmID 
		and L.ageID = T.ageID

WHERE
	(L.statsregID=_statsregID or L.statsregID is null or 0=_statsregID)
	--and 	(fty.fromyear = 2003 or fty.fromyear=2012)


union 

select  
fty.fromyear "start year", 
fty.toyear "end year", 

CountryDesc "country",
AgeDesc "age",
mmDesc "mode",
L.mmID "modeId",
L.ageID "ageId",
	Trips_unweighted as Trips_UNweighted,
--	cast(round(Trips_weighted,2)as float) as Trips_Weighted, 
	Stages_unweighted as Stages_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
	cast(round(cast(Individuals_weighted as numeric),2)as float) as Individuals_Weighted,
	
--round( cast(Trips_unweighted as float)* _weekToYearCorrectionFactor / cast(Individuals_unweighted as float), 3 ) "UNweighted tripRate",	
	
cast( (Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted) as float) "weighted tripRate (0303a)",

cast(( Stages_weighted* _weekToYearCorrectionFactor / Individuals_weighted )as float) "weighted stageRate (0303b)",

cast(( Boardings_weighted* _weekToYearCorrectionFactor / Individuals_weighted )as float) "weighted boardingRate (unpublished)",
	
cast(( StageDistance_weighted * _weekToYearCorrectionFactor / Individuals_weighted )as float) "total stage distance per-person-per-year (miles)(0303c)",

case when Trips_weighted>0 then cast(( TripDistance_weighted/Trips_weighted )as float) else null end "mean tripDistance (miles)(0303d)",

cast(( TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / Individuals_weighted )as float) "mean tripDuration per-person-per-year (hours)(0303e)",

case when Trips_weighted>0 then cast(( TripDuration_weighted/Trips_weighted )as float) else null end "mean tripDuration (minutes)(0303f)",

cast(( StageTravelTime_weighted * _weekToYearCorrectionFactor / 60.0 / Individuals_weighted)as float) "total stg travel tm (in veh) p-pers-p-year (hours)(unpublished)"

from 
	cteCountryLabels as L
	inner join ctaFromToYears fty
	on L.yearID = fty.toYearId

	left join
	cteXyrsIndividualsAllRegions as I
		on L.yearID = I.yearID
		and L.countryID = I.countryID
		and L.ageID = I.ageID	
	left join
	cteXyrsAllRegions as T
		on L.yearID = T.yearID
		and L.countryID = T.countryID
		and L.mmID = T.mmID 
		and L.ageID = T.ageID
--WHERE
--		(fty.fromyear = 2003 or fty.fromyear=2012)


union


select  
fty.fromyear "start year", 
fty.toyear "end year", 

LaDesc "region",
AgeDesc "age",
mmDesc "mode",
L.mmID "modeId",
L.ageID "ageId",	
	Trips_unweighted as Trips_UNweighted,
--	cast(round(Trips_weighted,2)as float) as Trips_Weighted, 
	Stages_unweighted as Stages_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
	cast(round(cast(Individuals_weighted as numeric),2)as float) as Individuals_Weighted,
	
--round( cast(Trips_unweighted as float)* _weekToYearCorrectionFactor / cast(Individuals_unweighted as float), 3 ) "UNweighted tripRate",	
	
cast(round( cast(Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) "weighted tripRate (0303a)",

cast(round( cast(Stages_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) "weighted stageRate (0303b)",

cast(round( cast(Boardings_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) "weighted boardingRate (unpublished)",
	
cast(round( cast(StageDistance_weighted * _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) "total stage distance per-person-per-year (miles)(0303c)",

case when Trips_weighted>0 then cast(round( cast(TripDistance_weighted/Trips_weighted as numeric), 3 )as float) else null end "mean tripDistance (miles)(0303d)",

cast(round( cast(TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric), 3 )as float) "mean tripDuration per-person-per-year (hours)(0303e)",

case when Trips_weighted>0 then cast(round( cast(TripDuration_weighted/Trips_weighted as numeric), 3 )as float) else null end "mean tripDuration (minutes)(0303f)",

cast(round( cast(StageTravelTime_weighted * _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric), 3 )as float) "total stg travel tm (in veh) p-pers-p-year (hours)(unpublished)"

from 
	cteLaLabels as L
	inner join ctaFromToYears fty
	on L.yearID = fty.toYearId
	
	left join
	cteLaXyrsIndividuals as I
		on L.yearID = I.yearID
		and L.laID = I.laID
		and L.ageID = I.ageID
	left join
	cteLaXyrs as T
		on L.yearID = T.yearID
		and L.laID = T.laID
		and L.mmID = T.mmID 
		and L.ageID = T.ageID
where 
	(0 != _statsregID)
--	and 	(fty.fromyear = 2003 or fty.fromyear=2012)

order by 1,2,3,6,7
)

select * from finalQuery;

end;
$$;
 
select * from __temp_table;
 
--can't drop the temp table here otherwise I don't get any output from the select statement in the pgadmin window

