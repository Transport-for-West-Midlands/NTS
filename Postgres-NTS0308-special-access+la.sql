/*=================================================================================

NTS0308 (Average number of trips and distance travelled by trip length and main mode)

short walk: MainMode_B02ID = 1 (replaced by MainMode_B11ID<>1)

	Owen O'Neill:	July 2023
	Owen O'Neill:	June 2024: updated to use restricted licence data.
	Owen O'Neill:   June 2024: added WMCA local authorities - watch out for sample size !
	Owen O'Neill:   June 2024: added option to skip covid years (2020+2021)
	Owen O'Neill:   Nov 2024: converted from query NTS0303 to NTS0308 by breaking down by trip distance.
	Owen O'Neill:   May 2025: altered region from using PSU (PSUStatsReg_B01ID) field to household field (hholdgor_b01id)

	
								note that some queries along these lines split by stage distance - some do trip distance, which generates different sets of numbers.
								!! really need to watch out for sample size when split by LA, mode and distance !!

=================================================================================*/
--use NTS;

DO $$
DECLARE

_numyears constant smallint = 3; --number of years to roll up averages (backwards from date reported in result row)

_statsregID constant  smallint = 0; --set to zero for all regions west midlands=8

_generateLaResults constant  smallint = 0;	--if 0=no LA results 1=WMCA member LAs, 2=all LAs

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

_minCovid constant smallint = 2020;
_maxCovid constant smallint = 2021;
_minCovidId constant smallint = 26;
_maxCovidId constant smallint = 27;


/*
--there are local authorities that are in multiple StatsRegions
--e.g. E06000057 (Northumberland) has parts of it in StatsRegion 1&2 (Northern, Metropolitan & Northern, Non-metropolitan)
--this means we can't simply aggregate the LA numbers in to stats region numbers.

with las as
(
select distinct h.HHoldOSLAUA_B01ID laCode, h.hholdgor_b01id 
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



--doing this inline get complicated when there are some modes where there is no data for a particular mode in a given years, 
--so generate it seperately and join in later.
ctaFromToYearsId( fromYearId, toYearId, toYear )
as
(
SELECT
L.SurveyYear_B01ID + 1 - 
CASE WHEN _skipCovidYears!=1 THEN _numyears
WHEN cast(L.description as int) -_numyears+1 <= _maxCovid AND cast(L.description as int) >= _minCovid
THEN _numyears + LEAST( cast(L.description as int) - _minCovid+ 1, _maxCovid-_minCovid+ 1 )
	ELSE _numyears
END,
	L.SurveyYear_B01ID, 
	cast (L.description as int)
FROM 
	tfwm_nts_securelookups.SurveyYear_B01ID L
WHERE
	L.SurveyYear_B01ID >= (select min(L2.SurveyYear_B01ID) from tfwm_nts_securelookups.SurveyYear_B01ID L2 WHERE L2.SurveyYear_B01ID>=0) + _numyears -1 
	AND 
	(_skipCovidYears!=1 OR cast (L.description as int) < _minCovid OR cast (L.description as int) > _maxCovid)	
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
 and MainMode_B04ID !=1
 union all select 1, 'All Walks' --now we have the 'long walks' result row, need to make it more obvious that the 'walk' mode is all distances
 union all select _dummyModeIdValue, 'Walk >=1 mile'
),

cteLabels (yearID, yearDesc,
			countryID, StatsRegID, StatsRegDesc,
		   tdID, tdDesc, tdOrder,
			mmID, mmDesc) 
as
(select psu.SurveyYear_B01ID, 
		psu.SurveyYear,
		psu.psucountry_b01id,
		statsRegLookup.hholdgor_b01id, statsRegLookup.description,
 		td.TripDisIncSW_B01ID, td.description, td.order,
		mm.MainMode_B04ID, mm.description
from 
	(select distinct SurveyYear_B01ID, SurveyYear, 
	 	CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END psucountry_b01id
	 from tfwm_nts_secureschema.psu ) as psu
	 
	left outer join tfwm_nts_securelookups.hholdgor_b01id as statsRegLookup
 	on psu.psucountry_b01id = case when statsRegLookup.hholdgor_b01id = 14 then 2 --wales
 									when statsRegLookup.hholdgor_b01id in (15,16) then 3 --scotland
 									else 1 end
									
	cross join tfwm_nts_securelookups.TripDisIncSW_B01ID td
 	cross join cteModeLabel mm
),


cteCountryLabels (yearID, yearDesc,
			countryID, countryCode, countryDesc,
 		    tdID, tdDesc, tdOrder,
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
 		td.TripDisIncSW_B01ID, td.description, td.order,
		mm.MainMode_B04ID, mm.description
from 
	(select distinct SurveyYear_B01ID, SurveyYear, 
	 	CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END psucountry_b01id from tfwm_nts_secureschema.psu ) as psu	
	
	left outer join tfwm_nts_securelookups.PSUCountry_B01ID as countryLookup
	on psu.psucountry_b01id = countryLookup.PSUCountry_B01ID
	cross join tfwm_nts_securelookups.TripDisIncSW_B01ID td 
	cross join cteModeLabel mm
),


--this table is one of the view lookups with a VARCHAR id, that the currently load process doesn't cope with.
lookup_HHoldOSLAUA_B01ID ( ID, description, isWMCA )
as
(
SELECT * FROM (VALUES
('E08000025', 'Birmingham', 1),
('E08000026', 'Coventry', 1),
('E08000027', 'Dudley', 1),
('E08000028', 'Sandwell', 1),
('E08000029', 'Solihull', 1),
('E08000030', 'Walsall', 1),
('E08000031', 'Wolverhampton', 1),
('E06000001', 'Hartlepool', 0),
('E06000002', 'Middlesbrough', 0),
('E06000003', 'Redcar and Cleveland', 0),
('E06000004', 'Stockton-on-Tees', 0),
('E06000005', 'Darlington', 0),
('E06000047', 'County Durham', 0),
('E06000048', 'Northumberland', 0),
('E08000020', 'Gateshead', 0),
('E08000021', 'Newcastle upon Tyne', 0),
('E08000022', 'North Tyneside', 0),
('E08000023', 'South Tyneside', 0),
('E08000024', 'Sunderland', 0),
('E06000006', 'Halton', 0),
('E06000007', 'Warrington', 0),
('E06000008', 'Blackburn with Darwen', 0),
('E06000009', 'Blackpool', 0),
('E06000049', 'Cheshire East', 0),
('E06000050', 'Cheshire West and Chester', 0),
('E07000026', 'Allerdale', 0),
('E07000027', 'Barrow-in-Furness', 0),
('E07000028', 'Carlisle', 0),
('E07000029', 'Copeland', 0),
('E07000030', 'Eden', 0),
('E07000031', 'South Lakeland', 0),
('E07000117', 'Burnley', 0),
('E07000118', 'Chorley', 0),
('E07000119', 'Fylde', 0),
('E07000120', 'Hyndburn', 0),
('E07000121', 'Lancaster', 0),
('E07000122', 'Pendle', 0),
('E07000123', 'Preston', 0),
('E07000124', 'Ribble Valley', 0),
('E07000125', 'Rossendale', 0),
('E07000126', 'South Ribble', 0),
('E07000127', 'West Lancashire', 0),
('E07000128', 'Wyre', 0),
('E08000001', 'Bolton', 0),
('E08000002', 'Bury', 0),
('E08000003', 'Manchester', 0),
('E08000004', 'Oldham', 0),
('E08000005', 'Rochdale', 0),
('E08000006', 'Salford', 0),
('E08000007', 'Stockport', 0),
('E08000008', 'Tameside', 0),
('E08000009', 'Trafford', 0),
('E08000010', 'Wigan', 0),
('E08000011', 'Knowsley', 0),
('E08000012', 'Liverpool', 0),
('E08000013', 'St. Helens', 0),
('E08000014', 'Sefton', 0),
('E08000015', 'Wirral', 0),
('E06000010', 'Kingston upon Hull, City of', 0),
('E06000011', 'East Riding of Yorkshire', 0),
('E06000012', 'North East Lincolnshire', 0),
('E06000013', 'North Lincolnshire', 0),
('E06000014', 'York', 0),
('E07000163', 'Craven', 0),
('E07000164', 'Hambleton', 0),
('E07000165', 'Harrogate', 0),
('E07000166', 'Richmondshire', 0),
('E07000167', 'Ryedale', 0),
('E07000168', 'Scarborough', 0),
('E07000169', 'Selby', 0),
('E08000016', 'Barnsley', 0),
('E08000017', 'Doncaster', 0),
('E08000018', 'Rotherham', 0),
('E08000019', 'Sheffield', 0),
('E08000032', 'Bradford', 0),
('E08000033', 'Calderdale', 0),
('E08000034', 'Kirklees', 0),
('E08000035', 'Leeds', 0),
('E08000036', 'Wakefield', 0),
('E06000015', 'Derby', 0),
('E06000016', 'Leicester', 0),
('E06000017', 'Rutland', 0),
('E06000018', 'Nottingham', 0),
('E07000032', 'Amber Valley', 0),
('E07000033', 'Bolsover', 0),
('E07000034', 'Chesterfield', 0),
('E07000035', 'Derbyshire Dales', 0),
('E07000036', 'Erewash', 0),
('E07000037', 'High Peak', 0),
('E07000038', 'North East Derbyshire', 0),
('E07000039', 'South Derbyshire', 0),
('E07000129', 'Blaby', 0),
('E07000130', 'Charnwood', 0),
('E07000131', 'Harborough', 0),
('E07000132', 'Hinckley and Bosworth', 0),
('E07000133', 'Melton', 0),
('E07000134', 'North West Leicestershire', 0),
('E07000135', 'Oadby and Wigston', 0),
('E07000136', 'Boston', 0),
('E07000137', 'East Lindsey', 0),
('E07000138', 'Lincoln', 0),
('E07000139', 'North Kesteven', 0),
('E07000140', 'South Holland', 0),
('E07000141', 'South Kesteven', 0),
('E07000142', 'West Lindsey', 0),
('E07000150', 'Corby', 0),
('E07000151', 'Daventry', 0),
('E07000152', 'East Northamptonshire', 0),
('E07000153', 'Kettering', 0),
('E07000154', 'Northampton', 0),
('E07000155', 'South Northamptonshire', 0),
('E07000156', 'Wellingborough', 0),
('E07000170', 'Ashfield', 0),
('E07000171', 'Bassetlaw', 0),
('E07000172', 'Broxtowe', 0),
('E07000173', 'Gedling', 0),
('E07000174', 'Mansfield', 0),
('E07000175', 'Newark and Sherwood', 0),
('E07000176', 'Rushcliffe', 0),
('E06000019', 'Herefordshire, County of', 0),
('E06000020', 'Telford and Wrekin', 0),
('E06000021', 'Stoke-on-Trent', 0),
('E06000051', 'Shropshire', 0),
('E07000192', 'Cannock Chase', 0),
('E07000193', 'East Staffordshire', 0),
('E07000194', 'Lichfield', 0),
('E07000195', 'Newcastle-under-Lyme', 0),
('E07000196', 'South Staffordshire', 0),
('E07000197', 'Stafford', 0),
('E07000198', 'Staffordshire Moorlands', 0),
('E07000199', 'Tamworth', 0),
('E07000234', 'Bromsgrove', 0),
('E07000235', 'Malvern Hills', 0),
('E07000236', 'Redditch', 0),
('E07000237', 'Worcester', 0),
('E07000238', 'Wychavon', 0),
('E07000239', 'Wyre Forest', 0),
('E07000218', 'North Warwickshire', 0),
('E07000219', 'Nuneaton and Bedworth', 0),
('E07000220', 'Rugby', 0),
('E07000221', 'Stratford-on-Avon', 0),
('E07000222', 'Warwick', 0),
('E06000031', 'Peterborough', 0),
('E06000032', 'Luton', 0),
('E06000033', 'Southend-on-Sea', 0),
('E06000034', 'Thurrock', 0),
('E06000055', 'Bedford', 0),
('E06000056', 'Central Bedfordshire', 0),
('E07000008', 'Cambridge', 0),
('E07000009', 'East Cambridgeshire', 0),
('E07000010', 'Fenland', 0),
('E07000011', 'Huntingdonshire', 0),
('E07000012', 'South Cambridgeshire', 0),
('E07000066', 'Basildon', 0),
('E07000067', 'Braintree', 0),
('E07000068', 'Brentwood', 0),
('E07000069', 'Castle Point', 0),
('E07000070', 'Chelmsford', 0),
('E07000071', 'Colchester', 0),
('E07000072', 'Epping Forest', 0),
('E07000073', 'Harlow', 0),
('E07000074', 'Maldon', 0),
('E07000075', 'Rochford', 0),
('E07000076', 'Tendring', 0),
('E07000077', 'Uttlesford', 0),
('E07000095', 'Broxbourne', 0),
('E07000096', 'Dacorum', 0),
('E07000097', 'East Hertfordshire', 0),
('E07000098', 'Hertsmere', 0),
('E07000099', 'North Hertfordshire', 0),
('E07000100', 'St Albans (Pre-2013)', 0),
('E07000101', 'Stevenage', 0),
('E07000102', 'Three Rivers', 0),
('E07000103', 'Watford', 0),
('E07000104', 'Welwyn Hatfield (Pre-2013)', 0),
('E07000143', 'Breckland', 0),
('E07000144', 'Broadland', 0),
('E07000145', 'Great Yarmouth', 0),
('E07000147', 'North Norfolk', 0),
('E07000148', 'Norwich', 0),
('E07000149', 'South Norfolk', 0),
('E07000200', 'Babergh', 0),
('E07000201', 'Forest Heath', 0),
('E07000202', 'Ipswich', 0),
('E07000203', 'Mid Suffolk', 0),
('E07000204', 'St Edmundsbury', 0),
('E07000205', 'Suffolk Coastal', 0),
('E07000206', 'Waveney', 0),
('E09000001', 'City of London', 0),
('E09000002', 'Barking and Dagenham', 0),
('E09000003', 'Barnet', 0),
('E09000004', 'Bexley', 0),
('E09000005', 'Brent', 0),
('E09000006', 'Bromley', 0),
('E09000007', 'Camden', 0),
('E09000008', 'Croydon', 0),
('E09000009', 'Ealing', 0),
('E09000010', 'Enfield', 0),
('E09000011', 'Greenwich', 0),
('E09000012', 'Hackney', 0),
('E09000013', 'Hammersmith and Fulham', 0),
('E09000014', 'Haringey', 0),
('E09000015', 'Harrow', 0),
('E09000016', 'Havering', 0),
('E09000017', 'Hillingdon', 0),
('E09000018', 'Hounslow', 0),
('E09000019', 'Islington', 0),
('E09000020', 'Kensington and Chelsea', 0),
('E09000021', 'Kingston upon Thames', 0),
('E09000022', 'Lambeth', 0),
('E09000023', 'Lewisham', 0),
('E09000024', 'Merton', 0),
('E09000025', 'Newham', 0),
('E09000026', 'Redbridge', 0),
('E09000027', 'Richmond upon Thames', 0),
('E09000028', 'Southwark', 0),
('E09000029', 'Sutton', 0),
('E09000030', 'Tower Hamlets', 0),
('E09000031', 'Waltham Forest', 0),
('E09000032', 'Wandsworth', 0),
('E09000033', 'Westminster', 0),
('E06000035', 'Medway', 0),
('E06000036', 'Bracknell Forest', 0),
('E06000037', 'West Berkshire', 0),
('E06000038', 'Reading', 0),
('E06000039', 'Slough', 0),
('E06000040', 'Windsor and Maidenhead', 0),
('E06000041', 'Wokingham', 0),
('E06000042', 'Milton Keynes', 0),
('E06000043', 'Brighton and Hove', 0),
('E06000044', 'Portsmouth', 0),
('E06000045', 'Southampton', 0),
('E06000046', 'Isle of Wight', 0),
('E07000004', 'Aylesbury Vale', 0),
('E07000005', 'Chiltern', 0),
('E07000006', 'South Bucks', 0),
('E07000007', 'Wycombe', 0),
('E07000061', 'Eastbourne', 0),
('E07000062', 'Hastings', 0),
('E07000063', 'Lewes', 0),
('E07000064', 'Rother', 0),
('E07000065', 'Wealden', 0),
('E07000084', 'Basingstoke and Deane', 0),
('E07000085', 'East Hampshire', 0),
('E07000086', 'Eastleigh', 0),
('E07000087', 'Fareham', 0),
('E07000088', 'Gosport', 0),
('E07000089', 'Hart', 0),
('E07000090', 'Havant', 0),
('E07000091', 'New Forest', 0),
('E07000092', 'Rushmoor', 0),
('E07000093', 'Test Valley', 0),
('E07000094', 'Winchester', 0),
('E07000105', 'Ashford', 0),
('E07000106', 'Canterbury', 0),
('E07000107', 'Dartford', 0),
('E07000108', 'Dover', 0),
('E07000109', 'Gravesham', 0),
('E07000110', 'Maidstone', 0),
('E07000111', 'Sevenoaks', 0),
('E07000112', 'Shepway', 0),
('E07000113', 'Swale', 0),
('E07000114', 'Thanet', 0),
('E07000115', 'Tonbridge and Malling', 0),
('E07000116', 'Tunbridge Wells', 0),
('E07000177', 'Cherwell', 0),
('E07000178', 'Oxford', 0),
('E07000179', 'South Oxfordshire', 0),
('E07000180', 'Vale of White Horse', 0),
('E07000181', 'West Oxfordshire', 0),
('E07000207', 'Elmbridge', 0),
('E07000208', 'Epsom and Ewell', 0),
('E07000209', 'Guildford', 0),
('E07000210', 'Mole Valley', 0),
('E07000211', 'Reigate and Banstead', 0),
('E07000212', 'Runnymede', 0),
('E07000213', 'Spelthorne', 0),
('E07000214', 'Surrey Heath', 0),
('E07000215', 'Tandridge', 0),
('E07000216', 'Waverley', 0),
('E07000217', 'Woking', 0),
('E07000223', 'Adur', 0),
('E07000224', 'Arun', 0),
('E07000225', 'Chichester', 0),
('E07000226', 'Crawley', 0),
('E07000227', 'Horsham', 0),
('E07000228', 'Mid Sussex', 0),
('E07000229', 'Worthing', 0),
('E06000022', 'Bath and North East Somerset', 0),
('E06000023', 'Bristol, City of', 0),
('E06000024', 'North Somerset', 0),
('E06000025', 'South Gloucestershire', 0),
('E06000026', 'Plymouth', 0),
('E06000027', 'Torbay', 0),
('E06000028', 'Bournemouth', 0),
('E06000029', 'Poole', 0),
('E06000030', 'Swindon', 0),
('E06000052', 'Cornwall', 0),
('E06000054', 'Wiltshire', 0),
('E07000040', 'East Devon', 0),
('E07000041', 'Exeter', 0),
('E07000042', 'Mid Devon', 0),
('E07000043', 'North Devon', 0),
('E07000044', 'South Hams', 0),
('E07000045', 'Teignbridge', 0),
('E07000046', 'Torridge', 0),
('E07000047', 'West Devon', 0),
('E07000048', 'Christchurch', 0),
('E07000049', 'East Dorset', 0),
('E07000050', 'North Dorset', 0),
('E07000051', 'Purbeck', 0),
('E07000052', 'West Dorset', 0),
('E07000053', 'Weymouth and Portland', 0),
('E07000078', 'Cheltenham', 0),
('E07000079', 'Cotswold', 0),
('E07000080', 'Forest of Dean', 0),
('E07000081', 'Gloucester', 0),
('E07000082', 'Stroud', 0),
('E07000083', 'Tewkesbury', 0),
('E07000187', 'Mendip', 0),
('E07000188', 'Sedgemoor', 0),
('E07000189', 'South Somerset', 0),
('E07000190', 'Taunton Deane', 0),
('E07000191', 'West Somerset', 0),
('S12000005', 'Clackmannanshire', 0),
('S12000006', 'Dumfries and Galloway', 0),
('S12000008', 'East Ayrshire', 0),
('S12000010', 'East Lothian', 0),
('S12000011', 'East Renfrewshire', 0),
('S12000013', 'Eilean Siar', 0),
('S12000014', 'Falkirk', 0),
('S12000015', 'Fife', 0),
('S12000017', 'Highland', 0),
('S12000018', 'Inverclyde', 0),
('S12000019', 'Midlothian', 0),
('S12000020', 'Moray', 0),
('S12000021', 'North Ayrshire', 0),
('S12000023', 'Orkney Islands', 0),
('S12000024', 'Perth and Kinross', 0),
('S12000026', 'Scottish Borders', 0),
('S12000027', 'Shetland Islands', 0),
('S12000028', 'South Ayrshire', 0),
('S12000029', 'South Lanarkshire', 0),
('S12000030', 'Stirling', 0),
('S12000033', 'Aberdeen City', 0),
('S12000034', 'Aberdeenshire', 0),
('S12000035', 'Argyll and Bute', 0),
('S12000036', 'City of Edinburgh', 0),
('S12000038', 'Renfrewshire', 0),
('S12000039', 'West Dunbartonshire', 0),
('S12000040', 'West Lothian', 0),
('S12000041', 'Angus', 0),
('S12000042', 'Dundee City', 0),
('S12000044', 'North Lanarkshire', 0),
('S12000009', 'East Dunbartonshire', 0),
('S12000043', 'Glasgow City', 0),
('W06000001', 'Isle of Anglesey', 0),
('W06000002', 'Gwynedd', 0),
('W06000003', 'Conwy', 0),
('W06000004', 'Denbighshire', 0),
('W06000005', 'Flintshire', 0),
('W06000006', 'Wrexham', 0),
('W06000008', 'Ceredigion', 0),
('W06000009', 'Pembrokeshire', 0),
('W06000010', 'Carmarthenshire', 0),
('W06000011', 'Swansea', 0),
('W06000012', 'Neath Port Talbot', 0),
('W06000013', 'Bridgend', 0),
('W06000014', 'The Vale of Glamorgan', 0),
('W06000015', 'Cardiff', 0),
('W06000016', 'Rhondda Cynon Taf', 0),
('W06000018', 'Caerphilly', 0),
('W06000019', 'Blaenau Gwent', 0),
('W06000020', 'Torfaen', 0),
('W06000021', 'Monmouthshire', 0),
('W06000022', 'Newport', 0),
('W06000023', 'Powys', 0),
('W06000024', 'Merthyr Tydfil', 0),
('E07000240', 'St Albans (Post-2013)', 0),
('E07000241', 'Welwyn Hatfield (Post-2013)', 0),
('E07000242', 'East Hertfordshire (Post-2014)', 0),
('E07000243', 'Stevenage (Post-2014)', 0),
('E06000057', 'Northumberland (Post-2014)', 0),
('E08000037', 'Gateshead (Post-2014)', 0),
('E06000053', 'Isles of Scilly', 0),
('E06000058', 'Bournemouth, Christchurch and Poole', 0),
('E06000059', 'Dorset', 0),
('E06000060', 'Buckinghamshire', 0),
('E06000061', 'North Northamptonshire', 0),
('E06000062', 'West Northamptonshire', 0),
('E07000244', 'East Suffolk', 0),
('E07000245', 'West Suffolk', 0),
('E07000246', 'Somerset West and Taunton', 0),
('S12000045', 'East Dunbartonshire', 0),
('S12000047', 'Fife', 0),
('S12000048', 'Perth and Kinross', 0),
('S12000049', 'Glasgow City', 0),
('S12000050', 'North Lanarkshire', 0)
) AS t(ID, description, isWMCA)	
	
),

 	
cteSelectedLa (LaID, LaDesc, isWMCA)
as
(SELECT Id, description, isWMCA 
 FROM lookup_HHoldOSLAUA_B01ID 
WHERE 
 (2=_generateLaResults)
OR (1=_generateLaResults AND 1=isWMCA)
),


 	
cteLaLabels (yearID, yearDesc,
			LaID, LaDesc, isWMCA,
			tdID, tdDesc, tdOrder,
			mmID, mmDesc) 
as
(select psu.SurveyYear_B01ID, psu.SurveyYear,
		la.LaID, la.LaDesc, la.isWMCA,
 		td.TripDisIncSW_B01ID, td.description, td.order,
		mm.MainMode_B04ID, mm.description
from 
	(select distinct SurveyYear_B01ID, SurveyYear from tfwm_nts_secureschema.psu ) as psu
	cross join cteSelectedLa la
	cross join tfwm_nts_securelookups.TripDisIncSW_B01ID td 
	cross join cteModeLabel mm
),


--JJXSC The number of trips to be counted, grossed for short walks and excluding “Series of Calls” trips. 
--JD The distance of the trip (miles), grossed for short walks.
--JTTXSC The total travelling time of the trip (in minutes), grossed for short walks and excluding “Series of Calls” trips. 
--JOTXSC The overall duration of the trip (in minutes), meaning that it includes both the travelling and waiting times between stages, 
--  grossed for short walks and excluding “Series of Calls” trips.
cteTripBase (yearID, surveyYear, countryID, statsregID, laID, tdID, mmID, MainMode_B11ID,  
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
		hholdgor_b01id,
		HHoldOSLAUA_B01ID,
 		TripDisIncSW_B01ID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = MainMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = MainMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE MainMode_B04ID
		END, 
 		MainMode_B11ID, 
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
		hholdgor_b01id, 
		HHoldOSLAUA_B01ID,
 		TripDisIncSW_B01ID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = MainMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = MainMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE MainMode_B04ID
		END,
 		MainMode_B11ID
),


cteTrips (yearID, surveyYear, countryID, statsregID, tdID, mmID,  
		Trips_unweighted , Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted)
as
(select yearID, surveyYear, countryID, statsregID, tdID, mmID,  
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
from cteTripBase
group by yearID, surveyYear, countryID, statsregID, tdID, mmID 

union all

--seperate out 'long' walks
select yearID, surveyYear, countryID, statsregID, tdID, _dummyModeIdValue,  
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

group by yearID, surveyYear, countryID, statsregID, tdID
),



cteLaTrips (yearID, surveyYear, laID, tdID, mmID,  
		Trips_unweighted , Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
)
as
(select yearID, surveyYear, la.laID, tdID, mmID,  
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
from cteSelectedLa la
 INNER JOIN cteTripBase t
 ON la.laID=t.laID
group by yearID, surveyYear, la.laID, tdID, mmID 

union all

--seperate out 'long' walks
select yearID, surveyYear, la.laID, tdID, _dummyModeIdValue,  
		sum(Trips_unweighted) , sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)
		--,MainStageDistance_weighted,
		--MainStageTravelTime_weighted
from cteSelectedLa la
 INNER JOIN cteTripBase t
 ON la.laID=t.laID
where MainMode_B11ID=2
/*where t.MainMode_B04ID in (1) -- walking trips >=1 mile
and JJXSC != 0
and (JD/cast(JJXSC as float))>1.0
--AND JD>=1.0*/

group by yearID, surveyYear, la.laID, tdID
),



/*
cteStages (yearID, surveyYear, countryID, statsregID, tdID, smID,  
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted
)
as
(
select SurveyYear_B01ID, 
		P.surveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		hholdgor_b01id,  
		TripDisIncSW_B01ID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END, 
		SUM(SSXSC), SUM(W5 * SSXSC),
		SUM(W5 * SD),
		SUM(W5 * STTXSC)
from 
tfwm_nts_secureschema.stage S

left join
tfwm_nts_secureschema.PSU as P
on S.PSUID = P.PSUID

left join
tfwm_nts_secureschema.trip T
on s.TripID = t.TripID

group by SurveyYear_B01ID, 
		P.surveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		hholdgor_b01id, 
		TripDisIncSW_B01ID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END 

union all

--seperate out 'long' walks
select SurveyYear_B01ID, 
		P.surveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		hholdgor_b01id, TripDisIncSW_B01ID, _dummyModeIdValue, 
		SUM(SSXSC), SUM(W5 * SSXSC),
		SUM(W5 * SD),
		SUM(W5 * STTXSC)
from 
tfwm_nts_secureschema.stage S

left join
tfwm_nts_secureschema.PSU as P
on S.PSUID = P.PSUID

left join
tfwm_nts_secureschema.trip T
on s.TripID = t.TripID

where s.StageMode_B11ID=2

group by SurveyYear_B01ID, 
		P.surveyYear,
		CASE WHEN psucountry_b01id = -10 THEN 1
			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		hholdgor_b01id, TripDisIncSW_B01ID, StageMode_B04ID
),



cteLaStages (yearID, surveyYear, laID, tdID, smID,  
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted
)
as
(
select SurveyYear_B01ID, 
		P.surveyYear,
		HHoldOSLAUA_B01ID, 
		TripDisIncSW_B01ID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END, 
		SUM(SSXSC), SUM(W5 * SSXSC),
		SUM(W5 * SD),
		SUM(W5 * STTXSC)
from 
tfwm_nts_secureschema.stage S

left join
tfwm_nts_secureschema.PSU as P
on S.PSUID = P.PSUID

left join
tfwm_nts_secureschema.trip T
on s.TripID = t.TripID
	
left join
tfwm_nts_secureschema.Household as H
on S.householdid = H.householdid

group by SurveyYear_B01ID, 
		P.surveyYear,
		HHoldOSLAUA_B01ID,
		TripDisIncSW_B01ID,
		CASE WHEN 1 = _combineLocalBusModes and 7 = StageMode_B04ID THEN 8 --force 'london bus' to 'local bus'
			WHEN 1 = _combineUndergroundIntoOther and 10 = StageMode_B04ID THEN 13 --force 'london underground' to 'other PT'
			ELSE StageMode_B04ID
		END 

union all

--seperate out 'long' walks
select SurveyYear_B01ID, 
		P.surveyYear,
		HHoldOSLAUA_B01ID, TripDisIncSW_B01ID, _dummyModeIdValue, 
		SUM(SSXSC), SUM(W5 * SSXSC),
		SUM(W5 * SD),
		SUM(W5 * STTXSC)
from 
tfwm_nts_secureschema.stage S

left join
tfwm_nts_secureschema.PSU as P
on S.PSUID = P.PSUID

left join
tfwm_nts_secureschema.trip T
on s.TripID = t.TripID

left join
tfwm_nts_secureschema.Household as H
on S.householdid = H.householdid
	
where s.StageMode_B11ID=2

group by SurveyYear_B01ID, 
		P.surveyYear,
		HHoldOSLAUA_B01ID, TripDisIncSW_B01ID,StageMode_B04ID
),
*/

cteXyrs (yearID, 
		 fromYear,
		 toYear,
		 countryID, statsregID, tdID, mmID, 
		Trips_unweighted, Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted/*,
		--MainStageDistance_weighted, MainStageTravelTime_weighted,
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted*/
)
as
(
select L.SurveyYear_B01ID,
	min(T.surveyYear),
	max(T.surveyYear),
	T.countryID, T.statsregID, 
		tdID,
		T.mmID, 
		sum(T.Trips_unweighted), sum(T.Trips_weighted),
		sum(T.TripDistance_unweighted), sum(T.TripDistance_weighted),
		sum(T.TripDuration_unweighted), sum(T.TripDuration_weighted),
		sum(T.TripTravelTime_unweighted), sum(T.TripTravelTime_weighted)/*,
		--sum(T.MainStageDistance_weighted), sum(T.MainStageTravelTime_weighted,
		sum(S.Stages_unweighted), sum(S.Stages_weighted),
		sum(S.StageDistance_weighted),
		sum(S.StageTravelTime_weighted)*/
from
	tfwm_nts_securelookups.SurveyYear_B01ID L
	
	inner join ctaFromToYears fty
	on L.SurveyYear_B01ID = fty.toYearId
	
	left join cteTrips as T
		on T.yearID >= fty.fromYearId and T.yearID <= fty.toYearId	
	
	/*left join 
	cteStages as S
		on S.yearID >= fty.fromYearId and S.yearID <= fty.toYearId
	
	full outer join
	cteTrips as T
		on S.yearID = T.yearID and S.countryID = T.countryID and S.statsregID = T.statsregID and S.smID = T.mmID and S.tdID = T.tdID*/
		
	where
		(_skipCovidYears!=1 OR T.surveyYear< _minCovid OR T.surveyYear> _maxCovid)
	
group by L.SurveyYear_B01ID, T.countryID, T.statsregID, tdID, T.mmID
),



cteLaXyrs (yearID, 
		fromYear,
		toYear,		   
		laID, tdID, mmID, 
		Trips_unweighted, Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted/*,
		--MainStageDistance_weighted, MainStageTravelTime_weighted,
		Stages_unweighted , Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted*/
)
as
(
select L.SurveyYear_B01ID,
	min(T.surveyYear),
	max(T.surveyYear),
	T.laID, 
	T.tdID,
		T.mmID, 
		sum(T.Trips_unweighted), sum(T.Trips_weighted),
		sum(T.TripDistance_unweighted), sum(T.TripDistance_weighted),
		sum(T.TripDuration_unweighted), sum(T.TripDuration_weighted),
		sum(T.TripTravelTime_unweighted), sum(T.TripTravelTime_weighted)/*,
		--sum(T.MainStageDistance_weighted), sum(T.MainStageTravelTime_weighted,
		sum(S.Stages_unweighted) , sum(S.Stages_weighted),
		sum(S.StageDistance_weighted),
		sum(S.StageTravelTime_weighted)*/
from
	tfwm_nts_securelookups.SurveyYear_B01ID L
	
	inner join ctaFromToYears fty
	on L.SurveyYear_B01ID = fty.toYearId
	
/*	left join 
	cteLaStages as S
		on S.yearID >= fty.fromYearId and S.yearID <= fty.toYearId
		
	full outer join
	cteLaTrips as T
		on S.yearID = T.yearID and S.laID = T.laID and S.smID = T.mmID and S.tdID = T.tdID*/

	left join cteLaTrips as T
		on T.yearID >= fty.fromYearId and T.yearID <= fty.toYearId
	
where
	(_skipCovidYears!=1 OR T.surveyYear< _minCovid OR T.surveyYear> _maxCovid)	
	
group by L.SurveyYear_B01ID, T.laID, T.tdID, T.mmID
),


cteXyrsAllRegions (yearID, 
		fromYear,
		toYear,			   
		countryID, tdID, mmID, 
		Trips_unweighted, Trips_weighted,
		TripDistance_unweighted, TripDistance_weighted,
		TripDuration_unweighted, TripDuration_weighted,
		TripTravelTime_unweighted, TripTravelTime_weighted/*,
		--MainStageDistance_weighted,MainStageTravelTime_weighted,
		Stages_unweighted, Stages_weighted,
		StageDistance_weighted,
		StageTravelTime_weighted*/
)
as
(select yearID, 
		min(fromYear),
		max(toYear),	
 countryID, tdID, mmID, 
		sum(Trips_unweighted), sum(Trips_weighted),
		sum(TripDistance_unweighted), sum(TripDistance_weighted),
		sum(TripDuration_unweighted), sum(TripDuration_weighted),
		sum(TripTravelTime_unweighted), sum(TripTravelTime_weighted)/*,
		--sum(MainStageDistance_weighted), sum(MainStageTravelTime_weighted)
		sum(Stages_unweighted) , sum(Stages_weighted),
		sum(StageDistance_weighted),
		sum(StageTravelTime_weighted)*/
from
cteXyrs
group by yearID, countryID, tdID, mmID
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
	hholdgor_b01id, 
	HHoldOSLAUA_B01ID,
	SUM(W1), SUM(W2)
from 
tfwm_nts_secureschema.individual I
left join
tfwm_nts_secureschema.PSU as P
on I.PSUID = P.PSUID
left join
tfwm_nts_secureschema.Household as H
on I.HouseholdID = H.HouseholdID
group by SurveyYear_B01ID, 
		CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END,
		hholdgor_b01id,
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
(select yearID, la.laID, sum(Individuals_unweighted), sum(Individuals_weighted)
from cteSelectedLa la
 INNER JOIN cteIndividualsBase i
 ON la.laID=i.laID
group by yearID, la.laID
),


cteXyrsIndividuals(yearID, countryID, statsregID, Individuals_unweighted, Individuals_weighted)
as
(select sy.SurveyYear_B01ID, i.countryID, i.statsregID, sum(I.Individuals_unweighted), sum(I.Individuals_weighted)
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
 
 	inner join ctaFromToYears fty
	on sy.SurveyYear_B01ID = fty.toYearId

	left join 
	cteIndividuals as I
 		on I.yearID >= fty.fromYearId and I.yearID <= fty.toYearId
 
where
 sy.SurveyYear_B01ID >=0 
 AND(_skipCovidYears!=1 OR cast(Sy.description as int)< _minCovid OR cast(Sy.description as int)> _maxCovid)
 AND(_skipCovidYears!=1 OR I.yearID< _minCovidId OR I.yearID> _maxCovidId)
 
group by sy.SurveyYear_B01ID, countryID, statsregID
),


cteLaXyrsIndividuals(yearID, laID, Individuals_unweighted, Individuals_weighted)
as
(select sy.SurveyYear_B01ID, i.laID, sum(I.Individuals_unweighted), sum(I.Individuals_weighted)
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
 
 	inner join ctaFromToYears fty
	on sy.SurveyYear_B01ID = fty.toYearId
 
	left join 
	cteLaIndividuals as I
 		on I.yearID >= fty.fromYearId and I.yearID <= fty.toYearId

 where
 sy.SurveyYear_B01ID >=0 
 AND(_skipCovidYears!=1 OR cast(Sy.description as int)< _minCovid OR cast(Sy.description as int)> _maxCovid)
 AND(_skipCovidYears!=1 OR I.yearID< _minCovidId OR I.yearID> _maxCovidId)
 
group by sy.SurveyYear_B01ID, laID
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
fty.fromyear "start year", 
fty.toyear "end year", 

StatsRegDesc "region",
mmDesc "mode",
tdDesc "distance",
L.mmID "modeId",
tdOrder,

	Trips_unweighted as Trips_UNweighted,
	round(cast(Trips_weighted as numeric),2) as Trips_Weighted, 
--	Stages_unweighted as Stages_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
	cast(round(cast(Individuals_weighted as numeric),2)as float) as Individuals_Weighted,
	
CASE WHEN Individuals_weighted>0 THEN cast(round( cast(Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) ELSE NULL END "weighted tripRate (0303a)",

CASE WHEN Individuals_weighted>0 THEN cast(round( cast(TripDistance_weighted* _weekToYearCorrectionFactor/ Individuals_weighted as numeric), 3 )as float) ELSE NULL END "tripDistance per-person-per-year (miles)",

CASE WHEN Trips_weighted>0 THEN cast(round( cast(TripDistance_weighted/Trips_weighted as numeric), 3 )as float) ELSE NULL END "mean tripDistance (miles)(0303d)",

CASE WHEN Individuals_weighted>0 THEN cast(round( cast(TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric), 3 )as float) ELSE NULL END "mean tripDuration per-person-per-year (hours)(0303e)"

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
		and L.tdID = T.tdID 		
		and L.mmID = T.mmID 

WHERE
	(L.statsregID=_statsregID or L.statsregID is null or 0=_statsregID)
	--and 	(fty.fromyear = 2003 or fty.fromyear=2012)
    --and 	(fty.fromyear = 2022)

union all

select  
fty.fromyear "start year", 
fty.toyear "end year", 

CountryDesc "country",
mmDesc "mode",
tdDesc "distance",
L.mmID "modeId",
tdOrder,

	Trips_unweighted as Trips_UNweighted,
	round(cast(Trips_weighted as numeric),2) as Trips_Weighted, 	
--	Stages_unweighted as Stages_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
	cast(round(cast(Individuals_weighted as numeric),2)as float) as Individuals_Weighted,
	
CASE WHEN Individuals_weighted>0 THEN cast( cast(Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric)as float) ELSE NULL END "weighted tripRate (0303a)",

CASE WHEN Individuals_weighted>0 THEN cast( cast(TripDistance_weighted* _weekToYearCorrectionFactor/ Individuals_weighted as numeric)as float) ELSE NULL END "tripDistance per-person-per-year (miles)",

CASE WHEN Trips_weighted>0 THEN cast( cast(TripDistance_weighted/Trips_weighted as numeric)as float) ELSE NULL END "mean tripDistance (miles)(0303d)",

CASE WHEN Individuals_weighted>0 THEN cast( cast(TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric)as float) ELSE NULL END "mean tripDuration per-person-per-year (hours)(0303e)"

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
		and L.tdID = T.tdID 		
		and L.mmID = T.mmID 

--WHERE
		--(fty.fromyear = 2003 or fty.fromyear=2012)
  --    	(fty.fromyear = 2022)

union all


select  
fty.fromyear "start year", 
fty.toyear "end year", 

LaDesc "region",
mmDesc "mode",
tdDesc "distance",
L.mmID "modeId",
tdOrder,

	Trips_unweighted as Trips_UNweighted,
	round(cast(Trips_weighted as numeric),2) as Trips_Weighted, 
--	Stages_unweighted as Stages_UNweighted,
	Individuals_unweighted as Individuals_UNweighted,
	cast(round(cast(Individuals_weighted as numeric),2)as float) as Individuals_Weighted,
	
CASE WHEN Individuals_weighted>0 THEN cast(round( cast(Trips_weighted* _weekToYearCorrectionFactor / Individuals_weighted as numeric), 3 )as float) ELSE NULL END "weighted tripRate (0303a)",

CASE WHEN Individuals_weighted>0 THEN cast(round( cast(TripDistance_weighted* _weekToYearCorrectionFactor/ Individuals_weighted as numeric), 3 )as float) ELSE NULL END "tripDistance per-person-per-year (miles)",

CASE WHEN Trips_weighted>0 THEN cast(round( cast(TripDistance_weighted/Trips_weighted as numeric), 3 )as float) ELSE NULL END "mean tripDistance (miles)(0303d)",

CASE WHEN Individuals_weighted>0 THEN cast(round( cast(TripDuration_weighted* _weekToYearCorrectionFactor / 60.0 / Individuals_weighted as numeric), 3 )as float) ELSE NULL END "mean tripDuration per-person-per-year (hours)(0303e)"

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
		and L.tdID = T.tdID 				
		and L.mmID = T.mmID 

where 
	(0 != _generateLaResults)
  --and 	(fty.fromyear = 2003 or fty.fromyear=2012)
  --  and 	(fty.fromyear = 2022)

order by 1,2,3,6,7;

end;
$$;
 
select * from __temp_table;
 
--can't drop the temp table here otherwise I don't get any output from the select statement in the pgadmin window

