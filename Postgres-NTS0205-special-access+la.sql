/*=================================================================================

	NTS0205 ( Household car availability)
					Owen O'Neill:	Jan 2024
					Owen O'Neill:   June 2024 updated to special access schema - added LA
	Owen O'Neill:   May 2025: altered region from using PSU (PSUStatsReg_B01ID) field to household field (hholdgor_b01id)

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

_numyears constant smallint = 10; --number of years to roll up averages (backwards from date reported in result row)

_generateLaResults constant  smallint = 0;	--if 0=no LA results 1=WMCA member LAs, 2=all LAs

_statsregID constant  smallint = 8; --set to zero for all regions west midlands=8

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
(
SELECT psu.SurveyYear_B01ID, 
 		psu.SurveyYear,
		psu.psucountry_b01id,
		statsRegLookup.hholdgor_b01id,  
 		statsRegLookup.description
FROM 
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
),


cteCountryLabels (yearID, yearDesc,
			countryID, countryCode, countryDesc) 
as
(select psu.SurveyYear_B01ID,
 		psu.SurveyYear,
		psu.psucountry_b01id,
 		CASE 
		 WHEN 2 = psu.psucountry_b01id THEN 'W92000004'
 		 WHEN 3 = psu.psucountry_b01id THEN 'S92000003'
	     ELSE 'E92000001'
		END,
		countryLookup.description
from 
	(select distinct SurveyYear_B01ID, SurveyYear, 
	 	CASE WHEN psucountry_b01id = -10 THEN 1
 			WHEN psucountry_b01id isnull THEN 1
			 ELSE psucountry_b01id
		END psucountry_b01id from tfwm_nts_secureschema.psu ) as psu
 
	left outer join 
	tfwm_nts_securelookups.PSUCountry_B01ID as countryLookup
	on psu.psucountry_b01id = countryLookup.PSUCountry_B01ID
 WHERE
 	countryLookup.part=1  
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

 	
cteSelectedLa (LaID, LaDesc, isWMCA)
as
(SELECT Id, description, isWMCA 
 FROM lookup_HHoldOSLAUA_B01ID 
WHERE 
 (2=_generateLaResults)
OR (1=_generateLaResults AND 1=isWMCA)
),

 	
cteLaLabels (yearID, yearDesc,
			LaID, LaDesc, isWMCA) 
as
(select psu.SurveyYear_B01ID, psu.SurveyYear,
		la.LaID, la.LaDesc, la.isWMCA
from 
	(select distinct SurveyYear_B01ID, SurveyYear from tfwm_nts_secureschema.psu ) as psu
	cross join cteSelectedLa la
),


--W0	Unweighted interview sample(Household)
--W3	Interview sample household weight (Household)
--W1	Unweighted diary sample(Household)
--W2	Diary sample household weight (Household)
--W5	Trip/Stage weight (Trip)
--W4	LDJ weight (LDJ)
--W6	Attitudes weight(Attitudes)
cteHouseholdsBase (yearID, countryID, statsregID, laId,
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
	hholdgor_b01id,
	HHoldOSLAUA_B01ID,
 	SUM(W0), 							SUM(W3),
 	sum(W0*H.hholdnumadults), 			sum(W3*H.hholdnumadults), 
 	sum(W0*(case when H.numcarvan='NA' then null else cast( H.numcarvan as int) end)),
    sum(W3*(case when H.numcarvan='NA' then null else cast( H.numcarvan as int) end)), 	
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
tfwm_nts_secureschema.Household as H

left join
tfwm_nts_secureschema.PSU as P
on H.PSUID = P.PSUID

left join
( select HouseholdID, count(*) "adultParticipantCount" from tfwm_nts_secureschema.individual 
 where Age_B04ID >= 4 --17 years old and over
 group by HouseholdID ) I
on I.HouseholdID = H.HouseholdID

left join
( select HouseholdID, count(*) "carOrVanCount" from tfwm_nts_secureschema.vehicle 
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
		hholdgor_b01id,
 		HHoldOSLAUA_B01ID
),



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
 (
 SELECT yearID, countryID, statsregID,
	 sum(households_unweighted), sum(households_weighted),
	sum(hholdnumadults_unweighted), sum(hholdnumadults_weighted), 
	sum(numcarvan_unweighted), sum(numcarvan_weighted), 
	sum(unweighted_adultParticipantCount), sum(weighted_adultParticipantCount),
	sum(unweighted_vehicleCount), sum(weighted_vehicleCount),
 sum(Hcar0),
 sum(Hcar1),
 sum(Hcar2)
 FROM cteHouseholdsBase
 GROUP BY yearID, countryID, statsregID
 ),



cteLaHouseholds (yearID, laId, 
	households_unweighted, households_weighted,
	hholdnumadults_unweighted, hholdnumadults_weighted, 
	numcarvan_unweighted, numcarvan_weighted, 
	unweighted_adultParticipantCount, weighted_adultParticipantCount,
	unweighted_vehicleCount, weighted_vehicleCount,
 Hcar0,
 Hcar1,
 Hcar2)
 as
 (
 SELECT yearID, laId,
	 sum(households_unweighted), sum(households_weighted),
	sum(hholdnumadults_unweighted), sum(hholdnumadults_weighted), 
	sum(numcarvan_unweighted), sum(numcarvan_weighted), 
	sum(unweighted_adultParticipantCount), sum(weighted_adultParticipantCount),
	sum(unweighted_vehicleCount), sum(weighted_vehicleCount),
 sum(Hcar0),
 sum(Hcar1),
 sum(Hcar2)
 FROM cteHouseholdsBase
 GROUP BY yearID, laId
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
	tfwm_nts_securelookups.SurveyYear_B01ID sy
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
),


cteLaXyrsHouseholds(yearID, laID,  
	households_unweighted, households_weighted,
	hholdnumadults_unweighted, hholdnumadults_weighted, 
	numcarvan_unweighted, numcarvan_weighted, 
	unweighted_adultParticipantCount, weighted_adultParticipantCount,
	unweighted_vehicleCount, weighted_vehicleCount,
 Hcar0,
 Hcar1,
 Hcar2)
as
(select sy.SurveyYear_B01ID, h.laID, 

	sum(households_unweighted), sum(households_weighted),
	sum(hholdnumadults_unweighted), sum(hholdnumadults_weighted), 
	sum(numcarvan_unweighted), sum(numcarvan_weighted), 
	sum(unweighted_adultParticipantCount), sum(weighted_adultParticipantCount),
	sum(unweighted_vehicleCount), sum(weighted_vehicleCount),			  
 
 sum(Hcar0),
 sum(Hcar1),
 sum(Hcar2) 
from 
	tfwm_nts_securelookups.SurveyYear_B01ID sy
	left join 
	cteLaHouseholds as H
		on sy.SurveyYear_B01ID -_numyears < H.yearID and sy.SurveyYear_B01ID >= H.yearID
group by sy.SurveyYear_B01ID, laID
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
	(select min(SurveyYear) "year" from tfwm_nts_secureschema.psu) minYear
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
	(select min(SurveyYear) "year" from tfwm_nts_secureschema.psu) minYear
where 
	L.yearDesc + 1 >= minYear.year + _numyears

union 

select  
yearDesc-_numyears+1 "start year", 
yearDesc "end year", 
LaDesc "region",

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
	cteLaLabels as L
	left join
	cteLaXyrsHouseholds as I
		on L.yearID = I.yearID
		and L.laID = I.laID

	cross join
	(select min(SurveyYear) "year" from tfwm_nts_secureschema.psu) minYear
where 
	L.yearDesc + 1 >= minYear.year + _numyears
	and
	(0!=_statsregID)



order by 1,2,3; 

end;
$$;
 
select * from __temp_table;
 
--can't drop the temp table here otherwise I don't get any output from the select statement in the pgadmin window
