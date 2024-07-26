/*=================================================================================

NTS0409 


	Owen O'Neill:	Jan 2024

=================================================================================*/
--use NTS;

DO $$
DECLARE

_numyears constant smallint = 3; --number of years to roll up averages (backwards from date reported in result row)

_statsregID constant  smallint = 0; --set to zero for all regions west midlands=10

_dummyModeIdValue constant  float = 1.5; --walks are split to 'long' walks and all walks - we use this dummy value for the additional 'long walk' category.

_weekToYearCorrectionFactor constant  float = 52.14; -- ((365.0*4.0)+1.0)/4.0/7.0; 
--diary is for 1 week - need to multiply by a suitable factor to get yearly trip rate
--365/7 appears wrong - to include leap years we should use (365*4+1)/4/7
--documentation further rounds this to 52.14, so to get the closest possible match to published national values use 52.14 (even though it's wrong) 	

_combineLocalBusModes  constant smallint = 1; --captured data segregates london bus and other local buses. We need this to compare with national results 
										 -- but want to combine them for our analysis. Use this to switch it on/off 

_combineUndergroundIntoOther  constant smallint = 1; --captured data segregates london underground. For other regions the tram/metro service goes into the 'other PT' category.
										--We need this to compare with national results but want to combine them for our analysis. Use this to switch it on/off 
BEGIN

DROP TABLE IF EXISTS __temp_table;

CREATE TEMP TABLE __temp_table AS

with 

cteModeLabel(MainMode_B04ID, description)
as
(select MainMode_B04ID, description from tfwm_ntslookups.MainMode_B04ID mm   
where (1!=_combineLocalBusModes or 7!=MainMode_B04ID) --exclude london buses if combining is switched on
	and (1!=_combineUndergroundIntoOther or 10!=MainMode_B04ID) --exclude london underground if combining is switched on
union
select _dummyModeIdValue, 'Walk >=1 mile'
),

cteLabels (yearID, yearDesc,
			countryID, StatsRegID, StatsRegDesc,
		    tpID, tpDesc,
			mmID, mmDesc) 
as
(select distinct psu.SurveyYear_B01ID, psu.SurveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
 			 WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psu.PSUStatsReg_B01ID, statsRegLookup.description,
 		tp.TripPurpose_B04ID, tp.description,
		mm.MainMode_B04ID, mm.description
from 
	tfwm_ntsdata.psu psu
	left outer join 
	tfwm_ntslookups.PSUStatsReg_B01ID as statsRegLookup
	on psu.PSUStatsReg_B01ID = statsRegLookup.PSUStatsReg_B01ID
 	cross join
	tfwm_ntslookups.TripPurpose_B04ID tp
	cross join
	cteModeLabel mm
),


cteCountryLabels (yearID, yearDesc,
			countryID, countryDesc,
			tpID, tpDesc,
			mmID, mmDesc) 
as
(select distinct psu.SurveyYear_B01ID, psu.SurveyYear,
		CASE WHEN psu.psucountry_b01id = -10 THEN 1
 			WHEN psu.psucountry_b01id isnull THEN 1
			 ELSE psu.psucountry_b01id
		END,
		countryLookup.description,
 		tp.TripPurpose_B04ID, tp.description,
		mm.MainMode_B04ID, mm.description
from 
	tfwm_ntsdata.psu psu
	left outer join 
	tfwm_ntslookups.PSUCountry_B01ID as countryLookup
	on CASE WHEN psu.psucountry_b01id = -10 THEN 1
 			WHEN psu.psucountry_b01id isnull THEN 1
			 ELSE psu.psucountry_b01id
		END = countryLookup.PSUCountry_B01ID
 	cross join
	tfwm_ntslookups.TripPurpose_B04ID tp
	cross join
	cteModeLabel mm
),

--JJXSC The number of trips to be counted, grossed for short walks and excluding “Series of Calls” trips. 
--JD The distance of the trip (miles), grossed for short walks.
--JTTXSC The total travelling time of the trip (in minutes), grossed for short walks and excluding “Series of Calls” trips. 
--JOTXSC The overall duration of the trip (in minutes), meaning that it includes both the travelling and waiting times between stages, 
--  grossed for short walks and excluding “Series of Calls” trips.
cteTrips (yearID, countryID, statsregID, tpID, mmID,  
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
 		TripPurpose_B04ID,
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
from 
tfwm_ntsdata.trip T

left join
tfwm_ntsdata.PSU as P
on T.PSUID = P.PSUID

/*left join
nts.stage S
on T.TripID = S.TripID
where S.StageMain_B01ID = 1 --main stage only*/

group by SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id,
 		TripPurpose_B04ID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = MainMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = MainMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE MainMode_B04ID
		END 

union all

--seperate out 'long' walks
select SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id,
 		TripPurpose_B04ID,
 		_dummyModeIdValue, 
		SUM(JJXSC), SUM(W5 * JJXSC),
		SUM(JD), SUM(W5 * JD),
		SUM(JOTXSC), SUM(W5 * JOTXSC),
		SUM(JTTXSC), SUM(W5 * JTTXSC)
		--,SUM(W5 * SD),
		--SUM(W5 * STTXSC)
from 
tfwm_ntsdata.trip T

left join
tfwm_ntsdata.PSU as P
on T.PSUID = P.PSUID

/*left join
nts.stage S
on T.TripID = S.TripID
where S.StageMain_B01ID = 1 --main stage only*/

where t.MainMode_B11ID=2
/*where t.MainMode_B04ID in (1) -- walking trips >=1 mile
and JJXSC != 0
and (JD/cast(JJXSC as float))>1.0
--AND JD>=1.0*/

group by SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id, TripPurpose_B04ID, MainMode_B04ID
),



cteStages (yearID, countryID, statsregID, tpID, smID,  
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted
)
as
(
select SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id, 
		TripPurpose_B04ID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END, 
		SUM(SSXSC), SUM(W5 * SSXSC),
		SUM(W5 * SD),
		SUM(W5 * STTXSC)
from 
tfwm_ntsdata.stage S

left join
tfwm_ntsdata.PSU as P
on S.PSUID = P.PSUID

left join
tfwm_ntsdata.trip T
on s.TripID = t.TripID

group by SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id, 
		TripPurpose_B04ID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END 

union all

--seperate out 'long' walks
select SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id,
		TripPurpose_B04ID,
		_dummyModeIdValue, 
		SUM(SSXSC), SUM(W5 * SSXSC),
		SUM(W5 * SD),
		SUM(W5 * STTXSC)
from 
tfwm_ntsdata.stage S

left join
tfwm_ntsdata.PSU as P
on S.PSUID = P.PSUID

left join
tfwm_ntsdata.trip T
on s.TripID = t.TripID

where s.StageMode_B11ID=2

group by SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id, TripPurpose_B04ID, StageMode_B04ID
),



cteXyrs (yearID, countryID, statsregID, tpID, mmID, 
		Trips_unweighted, Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted,
		--MainStageDistance_weighted, MainStageTravelTime_weighted,
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted
)
as
(
select L.SurveyYear_B01ID, S.countryID, S.statsregID, 
		COALESCE(S.tpID, T.tpID), 
		COALESCE(S.smID, T.mmID), 
		sum(T.Trips_unweighted), sum(T.Trips_weighted),
		sum(T.TripDistance_unweighted), sum(T.TripDistance_weighted),
		sum(T.TripDuration_unweighted), sum(T.TripDuration_weighted),
		sum(T.TripTravelTime_unweighted), sum(T.TripTravelTime_weighted),
		--sum(T.MainStageDistance_weighted), sum(T.MainStageTravelTime_weighted,
		sum(S.Stages_unweighted) , sum(S.Stages_weighted),
		sum(S.StageDistance_weighted),
		sum(S.StageTravelTime_weighted)
from
	tfwm_ntslookups.SurveyYear_B01ID L
	left join 
	cteStages as S
		on S.yearID > L.SurveyYear_B01ID -_numyears and S.yearID <= L.SurveyYear_B01ID
		
	full outer join
	cteTrips as T
		on S.yearID = T.yearID 
			and S.countryID = T.countryID 
			and S.statsregID = T.statsregID 
			and S.tpID = T.tpID
			and S.smID = T.mmID

group by L.SurveyYear_B01ID, S.countryID, S.statsregID, COALESCE(S.tpID, T.tpID), COALESCE(S.smID, T.mmID)
),


cteXyrsAllRegions (yearID, countryID, tpID, mmID, 
		Trips_unweighted, Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted,
		--MainStageDistance_weighted,MainStageTravelTime_weighted,
		Stages_unweighted, Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted
)
as
(select yearID, countryID, tpID, mmID, 
		sum(Trips_unweighted), sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted),
		--sum(MainStageDistance_weighted), sum(MainStageTravelTime_weighted)
		sum(Stages_unweighted) , sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted)
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
cteIndividuals (yearID, countryID, statsregID, Individuals_unweighted, Individuals_weighted)
as
(select SurveyYear_B01ID, 
	CASE WHEN psucountry_b01id = -10 THEN 1
 		WHEN psucountry_b01id isnull THEN 1
		 ELSE psucountry_b01id
	END,
	psustatsreg_b01id, SUM(W1), SUM(W2)
from 
tfwm_ntsdata.individual I
left join
tfwm_ntsdata.PSU as P
on I.PSUID = P.PSUID
left join
tfwm_ntsdata.Household as H
on I.HouseholdID = H.HouseholdID
group by SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		psustatsreg_b01id
),


cteXyrsIndividuals(yearID, countryID, statsregID, Individuals_unweighted, Individuals_weighted)
as
(select sy.SurveyYear_B01ID, i.countryID, i.statsregID, sum(I.Individuals_unweighted), sum(I.Individuals_weighted)
from 
	tfwm_ntslookups.SurveyYear_B01ID sy
	left join 
	cteIndividuals as I
		on sy.SurveyYear_B01ID -_numyears < I.yearID and sy.SurveyYear_B01ID >= I.yearID
group by sy.SurveyYear_B01ID, countryID, statsregID
),


cteXyrsIndividualsAllRegions(yearID, countryID, Individuals_unweighted, Individuals_weighted)
as
(select yearID, countryID, sum(Individuals_unweighted), sum(Individuals_weighted)
from 
	cteXyrsIndividuals
group by yearID, countryID
)



-- select query
select  
yearDesc-_numyears+1 "start year", 
yearDesc "end year", 

StatsRegDesc "region",
tpDesc "trip purpose",
mmDesc "mode",
L.mmID "modeId",
	Trips_unweighted as Trips_UNweighted,
--	cast(round(Trips_weighted,2)as float) as Trips_Weighted, 
	Stages_unweighted as Stages_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
--	cast(round(Individuals_weighted,2)as float) as Individuals_Weighted,
	
--round( cast(Trips_unweighted as float)* _weekToYearCorrectionFactor / cast(Individuals_unweighted as float), 3 ) "UNweighted tripRate",	
	
cast(round( cast(Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) "weighted tripRate (0409a)",

cast(round( cast(Stages_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) "weighted stageRate",

cast(round( cast(StageDistance_weighted * _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) "total stage distance per-person-per-year (miles)",

cast(round( cast(TripDistance_weighted * _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) "total tripDistance per-person-per-year (miles)(0409b)",

cast(round( cast(TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric), 3 )as float) "mean tripDuration per-person-per-year (hours)",

CASE WHEN Trips_weighted != 0 THEN cast(round( cast(TripDuration_weighted/Trips_weighted as numeric), 3 )as float)
ELSE NULL END "mean tripDuration (minutes)",

cast(round( cast(StageTravelTime_weighted * _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric), 3 )as float) "total stg travel tm (in veh) p-pers-p-year (hours)"

from 
	cteLabels as L
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
tpDesc "trip purpose",
mmDesc "mode",
L.mmID "modeId",
	Trips_unweighted as Trips_UNweighted,
--	cast(round(Trips_weighted,2)as float) as Trips_Weighted, 
	Stages_unweighted as Stages_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
--	cast(round(Individuals_weighted,2)as float) as Individuals_Weighted,
	
--round( cast(Trips_unweighted as float)* _weekToYearCorrectionFactor / cast(Individuals_unweighted as float), 3 ) "UNweighted tripRate",	
	
cast( (Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted) as float) "weighted tripRate (0409a)",

cast(( Stages_weighted* _weekToYearCorrectionFactor / Individuals_weighted )as float) "weighted stageRate",

cast(( StageDistance_weighted * _weekToYearCorrectionFactor / Individuals_weighted )as float) "total stage distance per-person-per-year (miles)",

cast(( TripDistance_weighted * _weekToYearCorrectionFactor / Individuals_weighted )as float) "total tripDistance per-person-per-year (miles)(0409b)",

cast(( TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / Individuals_weighted )as float) "mean tripDuration per-person-per-year (hours)",

CASE WHEN Trips_weighted != 0 THEN cast(( TripDuration_weighted/Trips_weighted )as float)
ELSE NULL END "mean tripDuration (minutes)",

cast(( StageTravelTime_weighted * _weekToYearCorrectionFactor / 60.0 / Individuals_weighted)as float) "total stg travel tm (in veh) p-pers-p-year (hours)"

from 
	cteCountryLabels as L
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

	cross join
	(select min(SurveyYear) "year" from tfwm_ntsdata.psu) minYear
where 
	L.yearDesc + 1 >= minYear.year + _numyears

order by 1,2,3,5;

end;
$$;
 
select * from __temp_table;
 
--can't drop the temp table here otherwise I don't get any output from the select statement in the pgadmin window