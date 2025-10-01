/*=================================================================================

!! this query may generate a lot of rows (2 million) - which PG admin won't save to a file - may need to limit the query to fewer geographies !!

Intermediate table used to partially aggregate and weight data. Results MUST be further aggregated to create statistically reliable sample sizes.
NTS recommends that sample size should be at least 300 individuals / 1000 trips to be statistically reliable.

householdVehicleCount is not weighted so should be used for grouping, not added up and used as a vehicle count.

results are split by year, country, region, local authority, age, sex, household vehicle count. These can (and should) be added up in any way desired for further analysis.
Individual counts are for a given combination of (year, country, region, local authority, age, sex, household vehicle count)

'Individuals using mode weighted' is then further split by mode - and tells us how many people use a particular mode (based on modes used at a stage level).
Since an individual may use multiple modes, the sum of this column will be larger than the individual count mentioned above.

The stage and trip data is then further split by trip purpose and trip distance 





	Owen O'Neill:	Jan 2024
	Owen O'Neill:	June 2024 use special access schema - adapt to LA geography
    Owen O'Neill:   June 2024 add functionality to skip covid years while keeping number of active years in rollling average the same.
	Owen O'Neill:   April 2025 merged in a bunch of fixes from nts0303 query, added ability to switch between grouping by different trip purposes
						- validated output against published 2023 data.
	Owen O'Neill:	May 2025 derived from 0403 - split by distance band and age band (to facilitiate school travel analysis) 
	Owen O'Neill:   May 2025: altered region from using PSU (PSUStatsReg_B01ID) field to household field (hholdgor_b01id)
	Owen O'Neill:   Oct 2025: created intermediate query derived from query for NTS0614

=================================================================================*/
--use NTS;

DO $$
DECLARE

_groupByTripPurposeSetting constant smallint = 6;
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

BEGIN

DROP TABLE IF EXISTS __temp_table;

CREATE TEMP TABLE __temp_table AS

with 

cteTripPurpose_B06ID ( TripPurpose_B06ID, description )
AS
(
SELECT * FROM (VALUES
--SELECT -10,'DEAD'  
 --SELECT -8,	'NA'			   
 (1,	'Commuting & escort commuting'),
 (2,	'Business & escort business'),
 (3,	'Education & escort education'),
 (4,	'Shopping & escort shopping / personal business'),
 (5,	'Personal business'),
 (6,	'Leisure'),
 (7,	'Holiday / day trip'),
 (8,	'Other including just walk & escort home (not own) / other')
) as t(TripPurpose_B06ID, description)			   		   
),




cteIndividualJoin (individualID, householdID, 
		yearID, countryID, statsregID, laID, ageId, sexId, ethId,
		householdVehicleCount,
		HouseholdUnweightedFactor,
		HouseholdWeightingFactor
)
as
(select I.individualID,
		I.householdID,
		SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		hholdgor_b01id,
 		HHoldOSLAUA_B01ID,
 		Age_B04ID,
		Sex_B01ID,
 		ethgroup_b02id,
		COALESCE(vehCount.numVehicles,0),

		H.W1,
		H.W2		
		
from tfwm_nts_secureschema.individual I

left join tfwm_nts_secureschema.PSU as P
on I.PSUID = P.PSUID

left join tfwm_nts_secureschema.Household as H
on I.householdid = H.householdid

left join 	
(select householdid, count(*) numVehicles from tfwm_nts_secureschema.vehicle where vehavail_b01id in (1,3) group by householdid) as vehCount
on I.householdid = vehCount.householdid

),




--count of individuals using a given mode. people use multiple modes, so the individual count will be greater than total sample size
cteStageJoinMode (yearID, countryID, statsregID, laID, ageId, sexId, ethId,
		householdVehicleCount,
		smID,
		
		Individuals_using_mode_weighted
)
AS
(SELECT yearID, countryID, statsregID, laID, ageId, sexId, ethId,
		householdVehicleCount,
		smID,
		
		sum(HouseholdWeightingFactor) as Individuals_weighted

FROM cteIndividualJoin

LEFT OUTER JOIN
	
	(SELECT 
		individualID,
		
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END as smID
		
	FROM tfwm_nts_secureschema.stage S
	
	GROUP BY individualID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END
		
	) as t on t.individualID = cteIndividualJoin.individualID

GROUP BY 
	yearID, countryID, statsregID, laID, ageId, sexId, ethId,
	householdVehicleCount,
	smID
),




--JJXSC The number of trips to be counted, grossed for short walks and excluding “Series of Calls” trips. 
--JD The distance of the trip (miles), grossed for short walks.
--JTTXSC The total travelling time of the trip (in minutes), grossed for short walks and excluding “Series of Calls” trips. 
--JOTXSC The overall duration of the trip (in minutes), meaning that it includes both the travelling and waiting times between stages, 
--  grossed for short walks and excluding “Series of Calls” trips.
cteTripJoin (tripID, individualID, householdID, 
		yearID, countryID, statsregID, laID, ageId, sexId, ethId,
		householdVehicleCount,
		HouseholdUnweightedFactor,
		HouseholdWeightingFactor,
		tpID, mmID, tdID, 
		TripWeightingFactor,
		Trips_unweighted,
		TripDistance_unweighted,
		TripDuration_unweighted,
		TripTravelTime_unweighted
)
as
(select T.TripID, I.individualID, I.householdID, 
		yearID, countryID, statsregID, laID, ageId, sexId, ethId,
		householdVehicleCount,
		HouseholdUnweightedFactor,
		HouseholdWeightingFactor,

 		CASE WHEN _groupByTripPurposeSetting = 1 THEN TripPurpose_B01ID
 			 WHEN _groupByTripPurposeSetting = 2 THEN TripPurpose_B02ID
 			 WHEN _groupByTripPurposeSetting = 4 THEN TripPurpose_B04ID
			 
			 --TripPurpose_B06ID
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
			ELSE NULL
		END,
		CASE WHEN 1 = _combineLocalBusModes and 7 = MainMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = MainMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE MainMode_B04ID
		END, 
 		TripDisIncSW_B01ID, 	
		T.W5,
		
		JJXSC,
		JD,
		JOTXSC,
		JTTXSC
		
FROM tfwm_nts_secureschema.trip T
		
LEFT JOIN cteIndividualJoin as I on T.individualID = I.individualID

),





cteTripBase (yearID, countryID, statsregID, laID, tpID, mmID, tdID, ageId, sexId, ethId,
		householdVehicleCount,
		Trips_unweighted,
		Trips_weighted,
		TripDistance_weighted,
		TripDuration_weighted,
		TripTravelTime_weighted
)
as
(select yearID, countryID, statsregID, laID, tpID, mmID, tdID, ageId, sexId, ethId,
		householdVehicleCount,		
		
		sum(Trips_unweighted),
		sum(TripWeightingFactor*Trips_unweighted),
		sum(TripWeightingFactor*TripDistance_unweighted),
		sum(TripWeightingFactor*TripDuration_unweighted),
		sum(TripWeightingFactor*TripTravelTime_unweighted)
		
from cteTripJoin T

group by
yearID, countryID, statsregID, laID, tpID, mmID, tdID, ageId, sexId, ethId, householdVehicleCount

),




cteStagesBase (yearID, surveyYear, countryID, statsregID, laID, tpID, ageId, sexId, ethId, householdVehicleCount,
		smID, tdID, 
		Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted
)
as
(
select yearID, 
		surveyYear,
		countryID, statsregID, laID, tpID, ageId, sexId, ethId, householdVehicleCount, 
		
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END, 
		--map the slightly different stage distance grouping onto the trip distance grouping
		CASE WHEN StageDistance_B01ID=10 OR StageDistance_B01ID=11 THEN 10
		     WHEN StageDistance_B01ID=12 OR StageDistance_B01ID=13 THEN 11
		     WHEN StageDistance_B01ID=14 THEN 12
		ELSE StageDistance_B01ID END,
	
		SUM(TripWeightingFactor*SSXSC),
		SUM(TripWeightingFactor*SD),
		SUM(TripWeightingFactor*STTXSC),
		SUM(TripWeightingFactor*SSXSC * CASE WHEN -8 = numboardings THEN 1 ELSE numboardings END)
	      --assume number of boardings is one if question not answered / not applicable
FROM 
tfwm_nts_secureschema.stage S

LEFT JOIN cteTripJoin T ON s.TripID = t.TripID

GROUP BY
		yearID, 
		surveyYear,
		countryID, statsregID, laID, tpID, ageId, sexId, ethId, householdVehicleCount, 
		
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END, 
		--map the slightly different stage distance grouping onto the trip distance grouping
		CASE WHEN StageDistance_B01ID=10 OR StageDistance_B01ID=11 THEN 10
		     WHEN StageDistance_B01ID=12 OR StageDistance_B01ID=13 THEN 11
		     WHEN StageDistance_B01ID=14 THEN 12
		ELSE StageDistance_B01ID END
),



--W0	Unweighted interview sample(Household)
--W3	Interview sample household weight (Household)
--W1	Unweighted diary sample(Household)
--W2	Diary sample household weight (Household)
--W5	Trip/Stage weight (Trip)
--W4	LDJ weight (LDJ)
--W6	Attitudes weight(Attitudes)
cteIndividualsBase (yearID, countryID, statsregID, laId, ageId, sexId, ethId, householdVehicleCount, 
	Individuals_unweighted, Individuals_weighted)
as
(SELECT yearID, countryID, statsregID, laId, ageId, sexId, ethId, householdVehicleCount, 
 	SUM(HouseholdUnweightedFactor), SUM(HouseholdWeightingFactor)

FROM cteIndividualJoin I 
 
GROUP BY yearID, countryID, statsregID, laId, ageId, sexId, ethId, householdVehicleCount 
),


summaryQuery as (
	select
		s.surveyYear, --COALESCE(s.surveyYear, t.surveyYear, jm.surveyYear) as surveyYear, 
		COALESCE(s.yearID, t.yearID, jm.yearID) as yearID, 
		COALESCE(s.countryID, t.countryID, jm.countryID) as countryID, 		
		COALESCE(s.statsregID, t.statsregID, jm.statsregID) as statsregID, 
		COALESCE(s.laID, t.laID, jm.laID) as laID, 
		COALESCE(S.ageID, T.ageID, jm.ageID) as ageID,
		COALESCE(S.sexID, T.sexID, jm.sexID) as sexID,
		COALESCE(S.ethID, T.ethID, jm.ethID) as ethID,
		COALESCE(S.householdVehicleCount, T.householdVehicleCount, jm.householdVehicleCount) as householdVehicleCount,
		
		COALESCE(S.smID, T.mmID, jm.smID) as mmID,

		COALESCE(s.tpID, T.tpID) as tpID, 
		COALESCE(S.tdID, T.tdID) as tdID,
		
		Individuals_unweighted, Individuals_weighted, Individuals_using_mode_weighted,
			
		Trips_unweighted, 
		Trips_weighted,
		TripDistance_weighted,
		TripDuration_weighted,
		TripTravelTime_weighted,

		--Stages_unweighted, 
		Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted,
		Boardings_weighted

	from cteStagesBase S
	
	full outer join cteTripBase as T
		on s.yearID = t.yearID
		and s.countryID = t.countryID
		and s.StatsRegID = t.statsregID
		and s.laID = t.laID
		and s.ageID = t.ageID
		and s.sexID = t.sexID
		and s.ethID = t.ethID	
		and s.householdVehicleCount = t.householdVehicleCount
		and s.smID = t.mmID
		and s.tpID = t.tpID
		and s.tdID = t.tdID 	
		
	full outer join cteIndividualsBase as I
		on s.yearID = i.yearID
		and s.countryID = i.countryID
		and s.StatsRegID = i.statsregID
		and s.laID = i.laID
		and s.ageID = i.ageID 
		and s.sexID = i.sexID
		and s.ethID = i.ethID
		and s.householdVehicleCount = i.householdVehicleCount
		
	full outer join cteStageJoinMode as jm
		on s.yearID = jm.yearID
		and s.countryID = jm.countryID
		and s.StatsRegID = jm.statsregID
		and s.laID = jm.laID
		and s.ageID = jm.ageID 
		and s.sexID = jm.sexID
		and s.ethID = jm.ethID
		and s.householdVehicleCount = jm.householdVehicleCount
		and s.smID = jm.smID		
)

select * from summaryQuery order by 1,2,3,4,5,6,7,8,9,10,11,12;


end;
$$;
 
select * from __temp_table;
 
--can't drop the temp table here otherwise I don't get any output from the select statement in the pgadmin window

