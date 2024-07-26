

with

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
)


select h.surveyyear, l.description, h.HHoldOSLAUA_B01ID, SUM(W1) 

from tfwm_nts_secureschema.individual i 

left outer join tfwm_nts_secureschema.household h
on i.householdid = h.householdid

inner join
lookup_HHoldOSLAUA_B01ID l
on 
l.id = h.HHoldOSLAUA_B01ID

where h.surveyyear>=2018

group by 
h.surveyyear,
l.description,
h.HHoldOSLAUA_B01ID

order by 1,2



select h.surveyyear, SUM(W1) 

from tfwm_nts_secureschema.individual i 

left outer join tfwm_nts_secureschema.household h
on i.householdid = h.householdid


where h.surveyyear>=2018

group by 
h.surveyyear

order by 1


