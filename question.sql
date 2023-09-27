with isLTE as (
	select 
		area,
		sum(case when radio IN ('LTE') then 0 else 1 end) isLte
	from 
		OpenCellId_data 
	group by area
	),

cnt_table as (
	select 
		area,
		COUNT(cell) cnt
	from 
		OpenCellId_data
	where 
		mcc = 250
	group by area
	)
	
select 
	area
from (
	select *
	from 
		cnt_table
	where 
		cnt_table.cnt > 2000
	) a
	
inner join (
		 select * 
		 from 
		 	isLTE
		 where 
		 	isLte > 0
		 ) b
	on a.area = b.area
