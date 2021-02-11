CREATE VIEW [model].[DimDate]
	AS 

SELECT 
	[DateKey],
	[Date],
	[Day],
	[Week],
	[WeekDayName],
	[WeekDayNameShort],
	[DayOfWeek],
	[DayOfYear],
	[Month],
	[MonthName],
	[MonthNameShort],
	[Quarter],
	[QuarterName],
	[Year],
	[FirstOfYear]
FROM [dim].[Date]
