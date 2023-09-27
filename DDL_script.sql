CREATE table OpenCellId_data (
	radio String,
	mcc UInt32,
	net UInt32,
	area UInt32,
	cell UInt64,
	unit Int64,
	lon Decimal(10,6),
	lat Decimal(10,6),
	range UInt64,
	samples UInt32,
	changeable UInt64,
	created UInt64,
	updated UInt64,
	averageSignal UInt64
) 
Engine = MergeTree()
order by (radio, mcc, area, cell)
partition by mcc;
