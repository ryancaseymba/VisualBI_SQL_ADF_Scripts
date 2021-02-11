CREATE VIEW [model].[DimTime]
	AS 

SELECT 
	[TimeKey],
	[Time24Hr],
	[Time12Hr],
	[Hour24],
	[Hour12],
	[MinuteNumber],
	[SecondNumber]
FROM [dim].[Time]
