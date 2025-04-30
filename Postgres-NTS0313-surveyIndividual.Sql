/*
	NTS0313  Frequency of use of different transport modes (from self-reported survey, not travel diary)
	(and summarising a bunch of other variables from the individual table, by country, region and LA)
	:usual caution over sample size must be applied, especially when looking at small geographies.
	:all this data is from the self reported interview - not the travel diary

	2024 Dec : Owen O'Neill : re-creating NTS0313, some glue in here to try to recreate the nts tables, especially the notes in the table.
							  some questions change frequently so there is some additional mapping needed to recreate some of the tables over time
							  e.g. the frequency of cycling questions have changed a lot, mainly the size of the bins.
							  -so some mapping over time is needed to completely re-create the results.
*/

DO $$
DECLARE

_numyears constant smallint = 1; --number of years to roll up averages (backwards from date reported in result row)

_statsregID constant  smallint = 0; --set to zero for all regions west midlands=10
									--if non-zero generates LA level results as well.

_weekToYearCorrectionFactor constant  float = 52.14; -- ((365.0*4.0)+1.0)/4.0/7.0; 
--diary is for 1 week - need to multiply by a suitable factor to get yearly trip rate
--365/7 appears wrong - to include leap years we should use (365*4+1)/4/7
--documentation further rounds this to 52.14, so to get the closest possible match to published national values use 52.14 (even though it's wrong) 	

_skipCovidYears constant smallint = 0; --if enabled skips 2020 + 2021 and extends year window to compensate so number of years aggregated remains the same.
                                                              

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


cteLabels (yearID, yearDesc,
			countryID, StatsRegID, StatsRegDesc, sexId, ageId, sexDesc, ageDesc) 
as
(select psu.SurveyYear_B01ID, psu.SurveyYear,
		psu.psucountry_b01id,
		psu.PSUStatsReg_B01ID, statsRegLookup.description, Sex_B01ID, Age_B01ID, sl.description, al.description
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
	cross join tfwm_nts_securelookups.Sex_B01ID sl
	cross join tfwm_nts_securelookups.Age_B01ID al
	
 WHERE
 	statsRegLookup.part=1 
	and sl.part=1
	and al.part=1
),


cteCountryLabels (yearID, yearDesc,
			countryID, countryCode, countryDesc, sexId, ageId, sexDesc, ageDesc) 
as
(select psu.SurveyYear_B01ID, psu.SurveyYear,
		psu.psucountry_b01id,
 		CASE 
		 WHEN 2 = psu.psucountry_b01id THEN 'W92000004'
 		 WHEN 3 = psu.psucountry_b01id THEN 'S92000003'
	     ELSE 'E92000001'
		END, 
		countryLookup.description, Sex_B01ID, Age_B01ID, sl.description, al.description
from 
 	(select distinct SurveyYear_B01ID, SurveyYear, 
	 	CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END psucountry_b01id from tfwm_nts_secureschema.psu ) as psu
 
	left outer join 
	tfwm_nts_securelookups.PSUCountry_B01ID as countryLookup
	on psu.psucountry_b01id = countryLookup.PSUCountry_B01ID
	cross join tfwm_nts_securelookups.Sex_B01ID sl
	cross join tfwm_nts_securelookups.Age_B01ID al
		
 WHERE
 	countryLookup.part=1  
	and sl.part=1
	and al.part=1	
),


--this table is one of the view lookups with a VARCHAR id, that the currently load process doesn't cope with.
lookup_HHoldOSLAUA_B01ID ( ID, description, isWMCA )
as
(
SELECT 'E08000025','Birmingham',1
UNION ALL SELECT 'E08000026','Coventry',1
UNION ALL SELECT 'E08000027','Dudley',1
UNION ALL SELECT 'E08000028','Sandwell',1
UNION ALL SELECT 'E08000029','Solihull',1
UNION ALL SELECT 'E08000030','Walsall',1
UNION ALL SELECT 'E08000031','Wolverhampton',1

UNION ALL SELECT 'E06000001','Hartlepool',0
UNION ALL SELECT 'E06000002','Middlesbrough',0
UNION ALL SELECT 'E06000003','Redcar and Cleveland',0
UNION ALL SELECT 'E06000004','Stockton-on-Tees',0
UNION ALL SELECT 'E06000005','Darlington',0
UNION ALL SELECT 'E06000047','County Durham',0
UNION ALL SELECT 'E06000048','Northumberland',0
UNION ALL SELECT 'E08000020','Gateshead',0
UNION ALL SELECT 'E08000021','Newcastle upon Tyne',0
UNION ALL SELECT 'E08000022','North Tyneside',0
UNION ALL SELECT 'E08000023','South Tyneside',0
UNION ALL SELECT 'E08000024','Sunderland',0
UNION ALL SELECT 'E06000006','Halton',0
UNION ALL SELECT 'E06000007','Warrington',0
UNION ALL SELECT 'E06000008','Blackburn with Darwen',0
UNION ALL SELECT 'E06000009','Blackpool',0
UNION ALL SELECT 'E06000049','Cheshire East',0
UNION ALL SELECT 'E06000050','Cheshire West and Chester',0
UNION ALL SELECT 'E07000026','Allerdale',0
UNION ALL SELECT 'E07000027','Barrow-in-Furness',0
UNION ALL SELECT 'E07000028','Carlisle',0
UNION ALL SELECT 'E07000029','Copeland',0
UNION ALL SELECT 'E07000030','Eden',0
UNION ALL SELECT 'E07000031','South Lakeland',0
UNION ALL SELECT 'E07000117','Burnley',0
UNION ALL SELECT 'E07000118','Chorley',0
UNION ALL SELECT 'E07000119','Fylde',0
UNION ALL SELECT 'E07000120','Hyndburn',0
UNION ALL SELECT 'E07000121','Lancaster',0
UNION ALL SELECT 'E07000122','Pendle',0
UNION ALL SELECT 'E07000123','Preston',0
UNION ALL SELECT 'E07000124','Ribble Valley',0
UNION ALL SELECT 'E07000125','Rossendale',0
UNION ALL SELECT 'E07000126','South Ribble',0
UNION ALL SELECT 'E07000127','West Lancashire',0
UNION ALL SELECT 'E07000128','Wyre',0
UNION ALL SELECT 'E08000001','Bolton',0
UNION ALL SELECT 'E08000002','Bury',0
UNION ALL SELECT 'E08000003','Manchester',0
UNION ALL SELECT 'E08000004','Oldham',0
UNION ALL SELECT 'E08000005','Rochdale',0
UNION ALL SELECT 'E08000006','Salford',0
UNION ALL SELECT 'E08000007','Stockport',0
UNION ALL SELECT 'E08000008','Tameside',0
UNION ALL SELECT 'E08000009','Trafford',0
UNION ALL SELECT 'E08000010','Wigan',0
UNION ALL SELECT 'E08000011','Knowsley',0
UNION ALL SELECT 'E08000012','Liverpool',0
UNION ALL SELECT 'E08000013','St. Helens',0
UNION ALL SELECT 'E08000014','Sefton',0
UNION ALL SELECT 'E08000015','Wirral',0
UNION ALL SELECT 'E06000010','Kingston upon Hull, City of',0
UNION ALL SELECT 'E06000011','East Riding of Yorkshire',0
UNION ALL SELECT 'E06000012','North East Lincolnshire',0
UNION ALL SELECT 'E06000013','North Lincolnshire',0
UNION ALL SELECT 'E06000014','York',0
UNION ALL SELECT 'E07000163','Craven',0
UNION ALL SELECT 'E07000164','Hambleton',0
UNION ALL SELECT 'E07000165','Harrogate',0
UNION ALL SELECT 'E07000166','Richmondshire',0
UNION ALL SELECT 'E07000167','Ryedale',0
UNION ALL SELECT 'E07000168','Scarborough',0
UNION ALL SELECT 'E07000169','Selby',0
UNION ALL SELECT 'E08000016','Barnsley',0
UNION ALL SELECT 'E08000017','Doncaster',0
UNION ALL SELECT 'E08000018','Rotherham',0
UNION ALL SELECT 'E08000019','Sheffield',0
UNION ALL SELECT 'E08000032','Bradford',0
UNION ALL SELECT 'E08000033','Calderdale',0
UNION ALL SELECT 'E08000034','Kirklees',0
UNION ALL SELECT 'E08000035','Leeds',0
UNION ALL SELECT 'E08000036','Wakefield',0
UNION ALL SELECT 'E06000015','Derby',0
UNION ALL SELECT 'E06000016','Leicester',0
UNION ALL SELECT 'E06000017','Rutland',0
UNION ALL SELECT 'E06000018','Nottingham',0
UNION ALL SELECT 'E07000032','Amber Valley',0
UNION ALL SELECT 'E07000033','Bolsover',0
UNION ALL SELECT 'E07000034','Chesterfield',0
UNION ALL SELECT 'E07000035','Derbyshire Dales',0
UNION ALL SELECT 'E07000036','Erewash',0
UNION ALL SELECT 'E07000037','High Peak',0
UNION ALL SELECT 'E07000038','North East Derbyshire',0
UNION ALL SELECT 'E07000039','South Derbyshire',0
UNION ALL SELECT 'E07000129','Blaby',0
UNION ALL SELECT 'E07000130','Charnwood',0
UNION ALL SELECT 'E07000131','Harborough',0
UNION ALL SELECT 'E07000132','Hinckley and Bosworth',0
UNION ALL SELECT 'E07000133','Melton',0
UNION ALL SELECT 'E07000134','North West Leicestershire',0
UNION ALL SELECT 'E07000135','Oadby and Wigston',0
UNION ALL SELECT 'E07000136','Boston',0
UNION ALL SELECT 'E07000137','East Lindsey',0
UNION ALL SELECT 'E07000138','Lincoln',0
UNION ALL SELECT 'E07000139','North Kesteven',0
UNION ALL SELECT 'E07000140','South Holland',0
UNION ALL SELECT 'E07000141','South Kesteven',0
UNION ALL SELECT 'E07000142','West Lindsey',0
UNION ALL SELECT 'E07000150','Corby',0
UNION ALL SELECT 'E07000151','Daventry',0
UNION ALL SELECT 'E07000152','East Northamptonshire',0
UNION ALL SELECT 'E07000153','Kettering',0
UNION ALL SELECT 'E07000154','Northampton',0
UNION ALL SELECT 'E07000155','South Northamptonshire',0
UNION ALL SELECT 'E07000156','Wellingborough',0
UNION ALL SELECT 'E07000170','Ashfield',0
UNION ALL SELECT 'E07000171','Bassetlaw',0
UNION ALL SELECT 'E07000172','Broxtowe',0
UNION ALL SELECT 'E07000173','Gedling',0
UNION ALL SELECT 'E07000174','Mansfield',0
UNION ALL SELECT 'E07000175','Newark and Sherwood',0
UNION ALL SELECT 'E07000176','Rushcliffe',0
UNION ALL SELECT 'E06000019','Herefordshire, County of',0
UNION ALL SELECT 'E06000020','Telford and Wrekin',0
UNION ALL SELECT 'E06000021','Stoke-on-Trent',0
UNION ALL SELECT 'E06000051','Shropshire',0
UNION ALL SELECT 'E07000192','Cannock Chase',0
UNION ALL SELECT 'E07000193','East Staffordshire',0
UNION ALL SELECT 'E07000194','Lichfield',0
UNION ALL SELECT 'E07000195','Newcastle-under-Lyme',0
UNION ALL SELECT 'E07000196','South Staffordshire',0
UNION ALL SELECT 'E07000197','Stafford',0
UNION ALL SELECT 'E07000198','Staffordshire Moorlands',0
UNION ALL SELECT 'E07000199','Tamworth',0
UNION ALL SELECT 'E07000234','Bromsgrove',0
UNION ALL SELECT 'E07000235','Malvern Hills',0
UNION ALL SELECT 'E07000236','Redditch',0
UNION ALL SELECT 'E07000237','Worcester',0
UNION ALL SELECT 'E07000238','Wychavon',0
UNION ALL SELECT 'E07000239','Wyre Forest',0
UNION ALL SELECT 'E07000218','North Warwickshire',0
UNION ALL SELECT 'E07000219','Nuneaton and Bedworth',0
UNION ALL SELECT 'E07000220','Rugby',0
UNION ALL SELECT 'E07000221','Stratford-on-Avon',0
UNION ALL SELECT 'E07000222','Warwick',0
UNION ALL SELECT 'E06000031','Peterborough',0
UNION ALL SELECT 'E06000032','Luton',0
UNION ALL SELECT 'E06000033','Southend-on-Sea',0
UNION ALL SELECT 'E06000034','Thurrock',0
UNION ALL SELECT 'E06000055','Bedford',0
UNION ALL SELECT 'E06000056','Central Bedfordshire',0
UNION ALL SELECT 'E07000008','Cambridge',0
UNION ALL SELECT 'E07000009','East Cambridgeshire',0
UNION ALL SELECT 'E07000010','Fenland',0
UNION ALL SELECT 'E07000011','Huntingdonshire',0
UNION ALL SELECT 'E07000012','South Cambridgeshire',0
UNION ALL SELECT 'E07000066','Basildon',0
UNION ALL SELECT 'E07000067','Braintree',0
UNION ALL SELECT 'E07000068','Brentwood',0
UNION ALL SELECT 'E07000069','Castle Point',0
UNION ALL SELECT 'E07000070','Chelmsford',0
UNION ALL SELECT 'E07000071','Colchester',0
UNION ALL SELECT 'E07000072','Epping Forest',0
UNION ALL SELECT 'E07000073','Harlow',0
UNION ALL SELECT 'E07000074','Maldon',0
UNION ALL SELECT 'E07000075','Rochford',0
UNION ALL SELECT 'E07000076','Tendring',0
UNION ALL SELECT 'E07000077','Uttlesford',0
UNION ALL SELECT 'E07000095','Broxbourne',0
UNION ALL SELECT 'E07000096','Dacorum',0
UNION ALL SELECT 'E07000097','East Hertfordshire',0
UNION ALL SELECT 'E07000098','Hertsmere',0
UNION ALL SELECT 'E07000099','North Hertfordshire',0
UNION ALL SELECT 'E07000100','St Albans (Pre-2013)',0
UNION ALL SELECT 'E07000101','Stevenage',0
UNION ALL SELECT 'E07000102','Three Rivers',0
UNION ALL SELECT 'E07000103','Watford',0
UNION ALL SELECT 'E07000104','Welwyn Hatfield (Pre-2013)',0
UNION ALL SELECT 'E07000143','Breckland',0
UNION ALL SELECT 'E07000144','Broadland',0
UNION ALL SELECT 'E07000145','Great Yarmouth',0
UNION ALL SELECT 'E07000146','King''s Lynn and West Norfolk',0
UNION ALL SELECT 'E07000147','North Norfolk',0
UNION ALL SELECT 'E07000148','Norwich',0
UNION ALL SELECT 'E07000149','South Norfolk',0
UNION ALL SELECT 'E07000200','Babergh',0
UNION ALL SELECT 'E07000201','Forest Heath',0
UNION ALL SELECT 'E07000202','Ipswich',0
UNION ALL SELECT 'E07000203','Mid Suffolk',0
UNION ALL SELECT 'E07000204','St Edmundsbury',0
UNION ALL SELECT 'E07000205','Suffolk Coastal',0
UNION ALL SELECT 'E07000206','Waveney',0
UNION ALL SELECT 'E09000001','City of London',0
UNION ALL SELECT 'E09000002','Barking and Dagenham',0
UNION ALL SELECT 'E09000003','Barnet',0
UNION ALL SELECT 'E09000004','Bexley',0
UNION ALL SELECT 'E09000005','Brent',0
UNION ALL SELECT 'E09000006','Bromley',0
UNION ALL SELECT 'E09000007','Camden',0
UNION ALL SELECT 'E09000008','Croydon',0
UNION ALL SELECT 'E09000009','Ealing',0
UNION ALL SELECT 'E09000010','Enfield',0
UNION ALL SELECT 'E09000011','Greenwich',0
UNION ALL SELECT 'E09000012','Hackney',0
UNION ALL SELECT 'E09000013','Hammersmith and Fulham',0
UNION ALL SELECT 'E09000014','Haringey',0
UNION ALL SELECT 'E09000015','Harrow',0
UNION ALL SELECT 'E09000016','Havering',0
UNION ALL SELECT 'E09000017','Hillingdon',0
UNION ALL SELECT 'E09000018','Hounslow',0
UNION ALL SELECT 'E09000019','Islington',0
UNION ALL SELECT 'E09000020','Kensington and Chelsea',0
UNION ALL SELECT 'E09000021','Kingston upon Thames',0
UNION ALL SELECT 'E09000022','Lambeth',0
UNION ALL SELECT 'E09000023','Lewisham',0
UNION ALL SELECT 'E09000024','Merton',0
UNION ALL SELECT 'E09000025','Newham',0
UNION ALL SELECT 'E09000026','Redbridge',0
UNION ALL SELECT 'E09000027','Richmond upon Thames',0
UNION ALL SELECT 'E09000028','Southwark',0
UNION ALL SELECT 'E09000029','Sutton',0
UNION ALL SELECT 'E09000030','Tower Hamlets',0
UNION ALL SELECT 'E09000031','Waltham Forest',0
UNION ALL SELECT 'E09000032','Wandsworth',0
UNION ALL SELECT 'E09000033','Westminster',0
UNION ALL SELECT 'E06000035','Medway',0
UNION ALL SELECT 'E06000036','Bracknell Forest',0
UNION ALL SELECT 'E06000037','West Berkshire',0
UNION ALL SELECT 'E06000038','Reading',0
UNION ALL SELECT 'E06000039','Slough',0
UNION ALL SELECT 'E06000040','Windsor and Maidenhead',0
UNION ALL SELECT 'E06000041','Wokingham',0
UNION ALL SELECT 'E06000042','Milton Keynes',0
UNION ALL SELECT 'E06000043','Brighton and Hove',0
UNION ALL SELECT 'E06000044','Portsmouth',0
UNION ALL SELECT 'E06000045','Southampton',0
UNION ALL SELECT 'E06000046','Isle of Wight',0
UNION ALL SELECT 'E07000004','Aylesbury Vale',0
UNION ALL SELECT 'E07000005','Chiltern',0
UNION ALL SELECT 'E07000006','South Bucks',0
UNION ALL SELECT 'E07000007','Wycombe',0
UNION ALL SELECT 'E07000061','Eastbourne',0
UNION ALL SELECT 'E07000062','Hastings',0
UNION ALL SELECT 'E07000063','Lewes',0
UNION ALL SELECT 'E07000064','Rother',0
UNION ALL SELECT 'E07000065','Wealden',0
UNION ALL SELECT 'E07000084','Basingstoke and Deane',0
UNION ALL SELECT 'E07000085','East Hampshire',0
UNION ALL SELECT 'E07000086','Eastleigh',0
UNION ALL SELECT 'E07000087','Fareham',0
UNION ALL SELECT 'E07000088','Gosport',0
UNION ALL SELECT 'E07000089','Hart',0
UNION ALL SELECT 'E07000090','Havant',0
UNION ALL SELECT 'E07000091','New Forest',0
UNION ALL SELECT 'E07000092','Rushmoor',0
UNION ALL SELECT 'E07000093','Test Valley',0
UNION ALL SELECT 'E07000094','Winchester',0
UNION ALL SELECT 'E07000105','Ashford',0
UNION ALL SELECT 'E07000106','Canterbury',0
UNION ALL SELECT 'E07000107','Dartford',0
UNION ALL SELECT 'E07000108','Dover',0
UNION ALL SELECT 'E07000109','Gravesham',0
UNION ALL SELECT 'E07000110','Maidstone',0
UNION ALL SELECT 'E07000111','Sevenoaks',0
UNION ALL SELECT 'E07000112','Shepway',0
UNION ALL SELECT 'E07000113','Swale',0
UNION ALL SELECT 'E07000114','Thanet',0
UNION ALL SELECT 'E07000115','Tonbridge and Malling',0
UNION ALL SELECT 'E07000116','Tunbridge Wells',0
UNION ALL SELECT 'E07000177','Cherwell',0
UNION ALL SELECT 'E07000178','Oxford',0
UNION ALL SELECT 'E07000179','South Oxfordshire',0
UNION ALL SELECT 'E07000180','Vale of White Horse',0
UNION ALL SELECT 'E07000181','West Oxfordshire',0
UNION ALL SELECT 'E07000207','Elmbridge',0
UNION ALL SELECT 'E07000208','Epsom and Ewell',0
UNION ALL SELECT 'E07000209','Guildford',0
UNION ALL SELECT 'E07000210','Mole Valley',0
UNION ALL SELECT 'E07000211','Reigate and Banstead',0
UNION ALL SELECT 'E07000212','Runnymede',0
UNION ALL SELECT 'E07000213','Spelthorne',0
UNION ALL SELECT 'E07000214','Surrey Heath',0
UNION ALL SELECT 'E07000215','Tandridge',0
UNION ALL SELECT 'E07000216','Waverley',0
UNION ALL SELECT 'E07000217','Woking',0
UNION ALL SELECT 'E07000223','Adur',0
UNION ALL SELECT 'E07000224','Arun',0
UNION ALL SELECT 'E07000225','Chichester',0
UNION ALL SELECT 'E07000226','Crawley',0
UNION ALL SELECT 'E07000227','Horsham',0
UNION ALL SELECT 'E07000228','Mid Sussex',0
UNION ALL SELECT 'E07000229','Worthing',0
UNION ALL SELECT 'E06000022','Bath and North East Somerset',0
UNION ALL SELECT 'E06000023','Bristol, City of',0
UNION ALL SELECT 'E06000024','North Somerset',0
UNION ALL SELECT 'E06000025','South Gloucestershire',0
UNION ALL SELECT 'E06000026','Plymouth',0
UNION ALL SELECT 'E06000027','Torbay',0
UNION ALL SELECT 'E06000028','Bournemouth',0
UNION ALL SELECT 'E06000029','Poole',0
UNION ALL SELECT 'E06000030','Swindon',0
UNION ALL SELECT 'E06000052','Cornwall',0
UNION ALL SELECT 'E06000054','Wiltshire',0
UNION ALL SELECT 'E07000040','East Devon',0
UNION ALL SELECT 'E07000041','Exeter',0
UNION ALL SELECT 'E07000042','Mid Devon',0
UNION ALL SELECT 'E07000043','North Devon',0
UNION ALL SELECT 'E07000044','South Hams',0
UNION ALL SELECT 'E07000045','Teignbridge',0
UNION ALL SELECT 'E07000046','Torridge',0
UNION ALL SELECT 'E07000047','West Devon',0
UNION ALL SELECT 'E07000048','Christchurch',0
UNION ALL SELECT 'E07000049','East Dorset',0
UNION ALL SELECT 'E07000050','North Dorset',0
UNION ALL SELECT 'E07000051','Purbeck',0
UNION ALL SELECT 'E07000052','West Dorset',0
UNION ALL SELECT 'E07000053','Weymouth and Portland',0
UNION ALL SELECT 'E07000078','Cheltenham',0
UNION ALL SELECT 'E07000079','Cotswold',0
UNION ALL SELECT 'E07000080','Forest of Dean',0
UNION ALL SELECT 'E07000081','Gloucester',0
UNION ALL SELECT 'E07000082','Stroud',0
UNION ALL SELECT 'E07000083','Tewkesbury',0
UNION ALL SELECT 'E07000187','Mendip',0
UNION ALL SELECT 'E07000188','Sedgemoor',0
UNION ALL SELECT 'E07000189','South Somerset',0
UNION ALL SELECT 'E07000190','Taunton Deane',0
UNION ALL SELECT 'E07000191','West Somerset',0
UNION ALL SELECT 'S12000005','Clackmannanshire',0
UNION ALL SELECT 'S12000006','Dumfries and Galloway',0
UNION ALL SELECT 'S12000008','East Ayrshire',0
UNION ALL SELECT 'S12000010','East Lothian',0
UNION ALL SELECT 'S12000011','East Renfrewshire',0
UNION ALL SELECT 'S12000013','Eilean Siar',0
UNION ALL SELECT 'S12000014','Falkirk',0
UNION ALL SELECT 'S12000015','Fife',0
UNION ALL SELECT 'S12000017','Highland',0
UNION ALL SELECT 'S12000018','Inverclyde',0
UNION ALL SELECT 'S12000019','Midlothian',0
UNION ALL SELECT 'S12000020','Moray',0
UNION ALL SELECT 'S12000021','North Ayrshire',0
UNION ALL SELECT 'S12000023','Orkney Islands',0
UNION ALL SELECT 'S12000024','Perth and Kinross',0
UNION ALL SELECT 'S12000026','Scottish Borders',0
UNION ALL SELECT 'S12000027','Shetland Islands',0
UNION ALL SELECT 'S12000028','South Ayrshire',0
UNION ALL SELECT 'S12000029','South Lanarkshire',0
UNION ALL SELECT 'S12000030','Stirling',0
UNION ALL SELECT 'S12000033','Aberdeen City',0
UNION ALL SELECT 'S12000034','Aberdeenshire',0
UNION ALL SELECT 'S12000035','Argyll and Bute',0
UNION ALL SELECT 'S12000036','City of Edinburgh',0
UNION ALL SELECT 'S12000038','Renfrewshire',0
UNION ALL SELECT 'S12000039','West Dunbartonshire',0
UNION ALL SELECT 'S12000040','West Lothian',0
UNION ALL SELECT 'S12000041','Angus',0
UNION ALL SELECT 'S12000042','Dundee City',0
UNION ALL SELECT 'S12000044','North Lanarkshire',0
UNION ALL SELECT 'S12000009','East Dunbartonshire',0
UNION ALL SELECT 'S12000043','Glasgow City',0
UNION ALL SELECT 'W06000001','Isle of Anglesey',0
UNION ALL SELECT 'W06000002','Gwynedd',0
UNION ALL SELECT 'W06000003','Conwy',0
UNION ALL SELECT 'W06000004','Denbighshire',0
UNION ALL SELECT 'W06000005','Flintshire',0
UNION ALL SELECT 'W06000006','Wrexham',0
UNION ALL SELECT 'W06000008','Ceredigion',0
UNION ALL SELECT 'W06000009','Pembrokeshire',0
UNION ALL SELECT 'W06000010','Carmarthenshire',0
UNION ALL SELECT 'W06000011','Swansea',0
UNION ALL SELECT 'W06000012','Neath Port Talbot',0
UNION ALL SELECT 'W06000013','Bridgend',0
UNION ALL SELECT 'W06000014','The Vale of Glamorgan',0
UNION ALL SELECT 'W06000015','Cardiff',0
UNION ALL SELECT 'W06000016','Rhondda Cynon Taf',0
UNION ALL SELECT 'W06000018','Caerphilly',0
UNION ALL SELECT 'W06000019','Blaenau Gwent',0
UNION ALL SELECT 'W06000020','Torfaen',0
UNION ALL SELECT 'W06000021','Monmouthshire',0
UNION ALL SELECT 'W06000022','Newport',0
UNION ALL SELECT 'W06000023','Powys',0
UNION ALL SELECT 'W06000024','Merthyr Tydfil',0
UNION ALL SELECT 'E07000240','St Albans (Post-2013)',0
UNION ALL SELECT 'E07000241','Welwyn Hatfield (Post-2013)',0
UNION ALL SELECT 'E07000242','East Hertfordshire (Post-2014)',0
UNION ALL SELECT 'E07000243','Stevenage (Post-2014)',0
UNION ALL SELECT 'E06000057','Northumberland (Post-2014)',0
UNION ALL SELECT 'E08000037','Gateshead (Post-2014)',0
UNION ALL SELECT 'E06000053','Isles of Scilly',0
UNION ALL SELECT 'E06000058','Bournemouth, Christchurch and Poole',0
UNION ALL SELECT 'E06000059','Dorset',0
UNION ALL SELECT 'E06000060','Buckinghamshire',0
UNION ALL SELECT 'E06000061','North Northamptonshire',0
UNION ALL SELECT 'E06000062','West Northamptonshire',0
UNION ALL SELECT 'E07000244','East Suffolk',0
UNION ALL SELECT 'E07000245','West Suffolk',0
UNION ALL SELECT 'E07000246','Somerset West and Taunton',0
UNION ALL SELECT 'S12000045','East Dunbartonshire',0
UNION ALL SELECT 'S12000047','Fife',0
UNION ALL SELECT 'S12000048','Perth and Kinross',0
UNION ALL SELECT 'S12000049','Glasgow City',0
UNION ALL SELECT 'S12000050','North Lanarkshire',0
),
	
/*
'E47000007', 'West Midlands Combined Authority'

West Midlands E12000005

Herefordshire E06000019
Telford and Wrekin E06000020
Stoke-on-Trent E06000021
Shropshire E06000051

Staffordshire E10000028
Cannock Chase E07000192
East Staffordshire E07000193
Lichfield E07000194
Newcastle-under-Lyme E07000195
South Staffordshire E07000196
Stafford E07000197
Staffordshire Moorlands E07000198
Tamworth E07000199

Warwickshire E10000031
North Warwickshire E07000218
Nuneaton and Bedworth E07000219
Rugby E07000220
Stratford-on-Avon E07000221
Warwick E07000222

Worcestershire E10000034
Bromsgrove E07000234
Malvern Hills E07000235
Redditch E07000236
Worcester E07000237
Wychavon E07000238
Wyre Forest E07000239

East Midlands E12000004
East Midlands Combined County Authority E47000013

Derby E06000015
Leicester E06000016
Rutland E06000017
Nottingham E06000018
North Northamptonshire E06000061
West Northamptonshire E06000062

Derbyshire E10000007
Amber Valley E07000032
Bolsover E07000033
Chesterfield E07000034
Derbyshire Dales E07000035
Erewash E07000036
High Peak E07000037
North East Derbyshire E07000038
South Derbyshire E07000039

Nottinghamshire E10000024
Ashfield E07000170
Bassetlaw E07000171
Broxtowe E07000172
Gedling E07000173
Mansfield E07000174
Newark and Sherwood E07000175
Rushcliffe E07000176

Leicestershire E10000018
Blaby E07000129
Charnwood E07000130
Harborough E07000131
Hinckley and Bosworth E07000132
Melton E07000133
North West Leicestershire E07000134
Oadby and Wigston E07000135

Lincolnshire E10000019
Boston E07000136
East Lindsey E07000137
Lincoln E07000138
North Kesteven E07000139
South Holland E07000140
South Kesteven E07000141
West Lindsey E07000142
*/	
 	
cteLaLabels (yearID, yearDesc,
			LaID, LaDesc, sexId, ageId, sexDesc, ageDesc, isWMCA) 
as
(select psu.SurveyYear_B01ID, psu.SurveyYear,
		laLookup.Id,
		laLookup.description, Sex_B01ID, Age_B01ID, sl.description, al.description, isWMCA
from 
  	(select distinct SurveyYear_B01ID, SurveyYear, 
	 	CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END psucountry_b01id from tfwm_nts_secureschema.psu ) as psu
	cross join lookup_HHoldOSLAUA_B01ID laLookup
	cross join tfwm_nts_securelookups.Sex_B01ID sl
	cross join tfwm_nts_securelookups.Age_B01ID	al
WHERE
	sl.part=1
	and al.part=1
),


cteAllLabels (yearID, yearDesc,
			countryId, GeoID, GeoDesc, sexId, ageId, sexDesc, ageDesc) 
as
(
select distinct yearID, yearDesc, countryId, cast(StatsRegID as varchar), StatsRegDesc, sexId, ageId, sexDesc, ageDesc 
	from cteLabels 
	where 0=_statsregID OR StatsRegID=_statsregID
union all
--select yearID, yearDesc, -1, LaID, LaDesc, sexId, ageId, sexDesc, ageDesc from cteLaLabels where isWMCA=1
--union all
select yearID, yearDesc, countryId, '-1', countryDesc || ':' || countryCode, sexId, ageId, sexDesc, ageDesc from cteCountryLabels
),




--W0	Unweighted interview sample(Household)
--W3	Interview sample household weight (Household)
--W1	Unweighted diary sample(Household)
--W2	Diary sample household weight (Household)
--W5	Trip/Stage weight (Trip)
--W4	LDJ weight (LDJ)
--W6	Attitudes weight(Attitudes)
cteIndividualBase (year, yearID, countryID, statsregID, laId, sexId, ageId, Individuals_unweighted, Individuals_weighted, individualId)
as
(select I.SurveyYear, SurveyYear_B01ID, 
	CASE WHEN psucountry_b01id = -10 THEN 1
 		WHEN psucountry_b01id isnull THEN 1
		 ELSE psucountry_b01id
	END,
	psustatsreg_b01id, 
 	HHoldOSLAUA_B01ID,
	Sex_B01ID,
	Age_B01ID,
 	W0, 
	W3, 
	individualId
from 
tfwm_nts_secureschema.individual I
 
left join tfwm_nts_secureschema.PSU as P
on I.PSUID = P.PSUID
 
left join tfwm_nts_secureschema.Household as H
on I.HouseholdID = H.HouseholdID
 
),


cteIndividualFact( individualId, dimension, val )
as
(
--!!!! very important not to have duplicates in this list - ensure it's sorted alphabetically to reduce risk of duplication.

SELECT individualId, 'Acc12Inj_B01ID', Acc12Inj_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Acc12Mn_B01ID', Acc12Mn_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Acc3Inj_B01ID', Acc3Inj_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Acc3Yr_B01ID', Acc3Yr_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Accident_B01ID', Accident_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Age_B01ID', Age_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Age_B04ID', Age_B04ID FROM tfwm_nts_secureschema.individual I

--2018 only	
UNION ALL SELECT individualId, 'Bicycle2_B01ID', 
	CASE WHEN Bicycle2_B01ID >= 0 OR Bicycle2_B01ID = -10 THEN Bicycle2_B01ID
	WHEN 2=Cycle12_B01ID THEN 7 
	ELSE Bicycle2_B01ID END FROM tfwm_nts_secureschema.individual I	WHERE Age_B04ID!=1

--2018 only
UNION ALL SELECT individualId, 'Bicycle2Freq_B01ID', 
	CASE WHEN Bicycle2Freq_B01ID >= 0 OR Bicycle2Freq_B01ID = -10 THEN Bicycle2Freq_B01ID
	WHEN 2=Cycle12_B01ID THEN 7 
	ELSE Bicycle2Freq_B01ID END FROM tfwm_nts_secureschema.individual I	WHERE Age_B04ID!=1

--New in 2019. Replaces Bicycle2Freq_B01ID	
UNION ALL SELECT individualId, 'Bicycle3Freq_B01ID', 
	CASE WHEN Bicycle3Freq_B01ID >= 0 OR Bicycle3Freq_B01ID = -10 THEN Bicycle3Freq_B01ID
	WHEN 2=Cycle12_B01ID THEN 10 
	ELSE Bicycle3Freq_B01ID END FROM tfwm_nts_secureschema.individual I	WHERE Age_B04ID!=1

--NTS0313: note3,4: Excludes under 5s from 2003.
--For pedal cycles, from 2009 onwards, includes people who answered "no" to the question: "Have you cycled in the last 12 months?". 
--This may have introduced a small break in the series between 2008 and 2009 for the less than once a year or never frequency group.
--last year 2017
UNION ALL SELECT individualId, 'BicycleFreq_B01ID', 
	CASE WHEN BicycleFreq_B01ID >= 0 OR BicycleFreq_B01ID = -10 THEN BicycleFreq_B01ID
	WHEN 2=Cycle12_B01ID THEN 7 
	ELSE BicycleFreq_B01ID END FROM tfwm_nts_secureschema.individual I	WHERE Age_B04ID!=1

UNION ALL SELECT individualId, 'BusOut_B01ID', BusOut_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'CAccIdnt_B01ID', CAccIdnt_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'CarAccess_B02ID', CarAccess_B02ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'CarWM_B01ID', CarWM_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'CarsEas_B01ID', CarsEas_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Coach2Freq_B01ID', Coach2Freq_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'CoachFreq_B01ID', CoachFreq_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Cinjury3_B01ID', Cinjury3_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Cut1_B01ID', Cut1_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Cycle12_B01ID', Cycle12_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'DrivDisable_B01ID', DrivDisable_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'DrivLic_B02ID', DrivLic_B02ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'EcoStat_B02ID', EcoStat_B02ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'EScooter_B01ID', EScooter_B01ID FROM tfwm_nts_secureschema.individual I

--NTS0313: note7: An e-scooter (electric scooter) is a two-wheeled scooter, designed to carry one person in a standing position, 
--which is fitted with an electric motor. Questions on e-scooter use were first introduced in 2021 and includes people who answered "no" to the question: 
--"Have you ridden an e-scooter during the last 12 months?".	
	--don't know if it is 'less than once a year', or 'never' - assume never.
UNION ALL SELECT individualId, 'EScooterFreq_B01ID', 
    CASE WHEN EScooterFreq_B01ID >= 0 OR EScooterFreq_B01ID = -10 THEN EScooterFreq_B01ID
    WHEN 2=EScooter_B01ID THEN 10 
    ELSE EScooterFreq_B01ID END FROM tfwm_nts_secureschema.individual I
	
UNION ALL SELECT individualId, 'EthGroup_B01ID', EthGroup_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'EthGroupTS_B01ID', EthGroupTS_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'EthGrp2_B01ID', EthGrp2_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'FootOut_B01ID', FootOut_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'FrqTrain_B01ID', FrqTrain_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'HowSch_B01ID', HowSch_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Incident_B01ID', Incident_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'IncidentN_B01ID', IncidentN_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'IndIncome2002_B02ID', IndIncome2002_B02ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Injury3_B01ID', Injury3_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'IntPlane_B01ID', IntPlane_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Medical1_B01ID', Medical1_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'NFrqBus_B01ID', NFrqBus_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'NReliBus_B01ID', NReliBus_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'NSatServ_B01ID', NSatServ_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'NTrnsat_B01ID', NTrnsat_B01ID FROM tfwm_nts_secureschema.individual I

--NTS0313: note5: For local buses, less than once a year or never frequency group also includes people who answered "no" to the question "Do you use local buses nowadays?".
UNION ALL SELECT individualId, 'OrdBus2Freq_B01ID', 
    CASE WHEN OrdBus2Freq_B01ID >= 0 OR OrdBus2Freq_B01ID = -10 THEN OrdBus2Freq_B01ID
    WHEN 2=BusOut_B01ID THEN 10 
    ELSE OrdBus2Freq_B01ID END FROM tfwm_nts_secureschema.individual I

UNION ALL SELECT individualId, 'OrdBusFreq_B01ID', 
    CASE WHEN OrdBusFreq_B01ID >= 0 OR OrdBusFreq_B01ID = -10 THEN OrdBusFreq_B01ID
    WHEN 2=BusOut_B01ID THEN 7 
    ELSE OrdBusFreq_B01ID END FROM tfwm_nts_secureschema.individual I

UNION ALL SELECT individualId, 'OthWM_B01ID', OthWM_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'OwnCycle_B01ID', OwnCycle_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'OwnCycleN_B01ID', OwnCycleN_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'OwnPhone_B01ID', OwnPhone_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'ParkWk_B01ID', ParkWk_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'PBlueBdg_B01ID', PBlueBdg_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'PDrivSt_B01ID', PDrivSt_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Plane2Freq_B01ID', Plane2Freq_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'PlaneFreq_B01ID', PlaneFreq_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Police_B01ID', Police_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'PoliceN_B01ID', PoliceN_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'PrbJobN_B01ID', PrbJobN_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'RelTrain_B01ID', RelTrain_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'ReNDNaM_B01ID', ReNDNaM_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'ReNDNbM_B01ID', ReNDNbM_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Report_B01ID', Report_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'ResNDnM_B01ID', ResNDnM_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'SCBus_B01ID', SCBus_B01ID FROM tfwm_nts_secureschema.attitudes A
UNION ALL SELECT individualId, 'SCBusFr_B01ID', SCBusFr_B01ID FROM tfwm_nts_secureschema.attitudes A
UNION ALL SELECT individualId, 'SCBusRe_B01ID', SCBusRe_B01ID FROM tfwm_nts_secureschema.attitudes A
UNION ALL SELECT individualId, 'SchDly_B01ID', SchDly_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'SchTrav_B01ID', SchTrav_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'SCTrain_B01ID', SCTrain_B01ID FROM tfwm_nts_secureschema.attitudes A
UNION ALL SELECT individualId, 'SCTraFr_B01ID', SCTraFr_B01ID FROM tfwm_nts_secureschema.attitudes A
UNION ALL SELECT individualId, 'SCTraRe_B01ID', SCTraRe_B01ID FROM tfwm_nts_secureschema.attitudes A
UNION ALL SELECT individualId, 'Sex_B01ID', Sex_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'SIC2007_B02ID', SIC2007_B02ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'TaxiCab2Freq_B01ID', TaxiCab2Freq_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'TaxiCabFreq_B01ID', TaxiCabFreq_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Train2Freq_B01ID', Train2Freq_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'TrainFreq_B01ID', TrainFreq_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'Walk2Freq_B01ID', Walk2Freq_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'WalkFreq_B01ID', WalkFreq_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'WkMode_B01ID', WkMode_B01ID FROM tfwm_nts_secureschema.individual I
UNION ALL SELECT individualId, 'XSOC2010_B02ID', XSOC2010_B02ID FROM tfwm_nts_secureschema.individual I;


--!!!! very important not to have duplicates in this list - ensure it's sorted alphabetically to reduce risk of duplication. (there are multiple tables selected from)
),
	
	

cteIndividualsBase (year, yearID, countryID, statsregID, laId, sexId, ageId, Individuals_unweighted, Individuals_weighted, dimension, val)
as
(
SELECT year, yearID, countryID, statsregID, laId, sexId, ageId, Individuals_unweighted, Individuals_weighted, dimension, val 
FROM cteIndividualBase
LEFT OUTER JOIN cteIndividualFact
ON cteIndividualBase.individualId = cteIndividualFact.individualId
),


cteIndividuals (year, yearID, countryID, statsregID, sexId, ageId, Individuals_unweighted, Individuals_weighted, dimension, val)
as
(select year, yearID, countryID, statsregID, sexId, ageId, sum(Individuals_unweighted), sum(Individuals_weighted), dimension, val
from cteIndividualsBase
group by year, yearID, countryID, statsregID, sexId, ageId, dimension, val
),

cteLaIndividuals (year, yearID, laID, sexId, ageId, Individuals_unweighted, Individuals_weighted, dimension, val)
as
(select year, yearID, laID, sexId, ageId, sum(Individuals_unweighted), sum(Individuals_weighted), dimension, val
from cteIndividualsBase
group by year, yearID, laID, sexId, ageId, dimension, val
),

cteIndividualsAllRegions (year, yearID, countryID, sexId, ageId, Individuals_unweighted, Individuals_weighted, dimension, val)
as
(select year, yearID, countryID, sexId, ageId, sum(Individuals_unweighted), sum(Individuals_weighted), dimension, val
from cteIndividuals
group by year, yearID, countryID, sexId, ageId, dimension, val
),


cteIndividualFactGeoSummary(year, yearId, countryId, geoId, sexId, ageId, Individuals_unweighted, Individuals_weighted, dimension, val)
as
(select year, yearID, countryID, cast(statsregID as varchar), sexId, ageId, Individuals_unweighted, Individuals_weighted, dimension, val
 from cteIndividuals
union all
select year, yearID, -1, laID, sexId, ageId, Individuals_unweighted, Individuals_weighted, dimension, val
 from cteLaIndividuals
union all 
select year, yearID, countryID, '-1', sexId, ageId, Individuals_unweighted, Individuals_weighted, dimension, val
 from cteIndividualsAllRegions
),




--sum across years.
--not all questions are asked in all years, plus we are at a very detailed level of geography, sex and age.
--this means that it's quite possible there is no data for some years in the window we are interested in.
--hence we can get a difference between fromYear toYear - and the years in the data fromYearData toYearData
cteXyrs (yearID, fromYear, toYear, fromYearData, toYearData,
countryId, geoId,sexId, ageId, dimension, val,
individuals_unweighted, individuals_weighted)
as
(
select L.SurveyYear_B01ID,
	fty.fromYear,
	fty.toYear,
	min(indf.year),
	max(indf.year),
countryId, geoId, sexId,  ageId , dimension, val,
sum(Individuals_unweighted), sum(Individuals_weighted)	
from
	tfwm_nts_securelookups.SurveyYear_B01ID L
	
	inner join ctaFromToYears fty
	on L.SurveyYear_B01ID = fty.toYearId
	cross join cteCovidYears cy
	
	left join cteIndividualFactGeoSummary indf
		on indf.yearId >= fty.fromYearId and indf.yearId <= fty.toYearId
			
	where
		(_skipCovidYears!=1 OR indf.year< cy.minCovid OR indf.year> cy.maxCovid)
	
group by L.SurveyYear_B01ID, fty.fromYear, fty.toYear, countryId, geoId, sexId, ageId, dimension, val
),


--join in all the labels
cteDataAndLabels (fromYear, toYear, fromYearData, toYearData,
				  GeoDesc, sexDesc, ageDesc, variableDesc, "desc", yearID, GeoID, sexId, ageId, variable, "value",
				 individuals_unweighted, individuals_weighted)
as
( select fromYear, toYear, fromYearData, toYearData, GeoDesc, sexDesc, ageDesc, alv.description, alv.desc, 
 L.yearID, L.GeoID, L.sexId, L.ageId, f.dimension, f.val, 
 Individuals_unweighted, Individuals_weighted
from cteAllLabels L
 left outer join cteXyrs f
 on L.yearID = f.yearID and L.countryId = f.countryId and L.GeoID = f.GeoId and L.sexId = f.sexId and L.ageID = f.ageId
 
 left outer join tfwm_nts_securelookups.alllookupvalues alv
 on alv.variable =  f.dimension and alv.id = f.val
 
-- WHERE
-- f.dimension in ('OrdBus2Freq_B01ID', 'OrdBusFreq_B01ID') and 
-- f.val >=0
),



--summarise all ages and sex together
cteDataAndLabelsAllAgeAndSex (fromYear, toYear, --fromYearData, toYearData,
		GeoDesc, variableDesc, "valueDesc", yearID, GeoID, variable, "value", individuals_unweighted, individuals_weighted)
as
( 
select fromYear, toYear, --min(fromYearData), max(toYearData), 
 case when strpos(GeoDesc,':')>0 then split_part(GeoDesc, ':', 1)
	else GeoDesc end, --cosmetic reformatting of ONS country code that was stuffed into description earlier
 variableDesc, 
 "desc", 
 yearID,  
 case when '-1' = GeoId then split_part(GeoDesc, ':', 2)
	else GeoId end, --cosmetic reformatting of ONS country code that was stuffed into description earlier
 variable, 
 "value",
 sum(individuals_unweighted), sum(individuals_weighted)
from cteDataAndLabels
	--where fromyear=2022 --don't understand why applying this criteria causes it to complain about conversion from string to int.
 group by fromYear, toYear, GeoDesc, variableDesc, "desc", yearID, GeoID, variable, "value"
 
 --not all questions are asked in every year, so filter out those which don't have a complete span of data for the number of years requested
 --this is still not perfect becuase some questions are now rotated and asked on a cycle instead of every year
 having fromYear = min(fromYearData) AND toYear = max(toYearData) 
),


cteDataAndLabelsTotal(fromYear, toYear, --fromYearData, toYearData,
		GeoDesc, variableDesc, valueDesc, yearID, GeoID, variable, "value", individuals_unweighted, individuals_weighted)
as
(
	SELECT fromYear, toYear, --fromYearData, toYearData,
		GeoDesc, variableDesc, 'TOTAL', yearID, GeoID, variable, -100, sum(individuals_unweighted), sum(individuals_weighted)
	FROM cteDataAndLabelsAllAgeAndSex
	GROUP BY fromYear, toYear, --fromYearData, toYearData,
		GeoDesc, variableDesc, yearID, GeoID, variable
),
	

cteDataAndLabelsTotalExDna(fromYear, toYear, --fromYearData, toYearData,
		GeoDesc, variableDesc, valueDesc, yearID, GeoID, variable, "value", individuals_unweighted, individuals_weighted)
as
(
	-- -10(DEAD) question not asked in a given year  -9(DNA) respondent did not answer question     -8 (NA) not applicable
	--if there aren't any rows satisfying the where clause don't get a row back.
	SELECT fromYear, toYear, --fromYearData, toYearData,
		GeoDesc, variableDesc, 'TOTAL ex (DEAD,DNA,NA)', yearID, GeoID, variable, -99, sum(individuals_unweighted), sum(individuals_weighted)
	FROM cteDataAndLabelsAllAgeAndSex
	WHERE "value" not in (-10,-9,-8 )
	GROUP BY fromYear, toYear, --fromYearData, toYearData,
		GeoDesc, variableDesc, yearID, GeoID, variable
)


select AnS.*, CASE WHEN Ans.value>=0 THEN AnS.individuals_weighted / tot.individuals_weighted * 100 ELSE NULL END as weightedPercentage from cteDataAndLabelsAllAgeAndSex AnS
left outer join cteDataAndLabelsTotalExDna tot
on ans.fromYear = tot.fromYear and ans.toYear = tot.toYear 
and ans.GeoDesc = tot.GeoDesc and ans.variableDesc = tot.variableDesc
and ans.yearID = tot.yearID and ans.GeoID = tot.GeoID and ans.variable = tot.variable

union all select *, NULL from cteDataAndLabelsTotal
union all select *, 100 from cteDataAndLabelsTotalExDna

order by yearID, GeoID, variable, "value";

--select * from ctaFromToYears; -- where fromyear=2022;

end;
$$;
 
select * from __temp_table;
 
--can't drop the temp table here otherwise I don't get any output from the select statement in the pgadmin window

