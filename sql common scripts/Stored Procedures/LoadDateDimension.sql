/********************************************************************************************************************************/
--Object Name : [etl].[LoadDateDimension]
--Purpose      : Generic Procedure to load date dimension
--Date Creaed : 8/1/0/2020
--Input Parameters :
--                    StartDate-The start date from which the date dimension should be loaded. The format should be given as yyyy-mm-dd
--                    NumberOfYears-Th number of years to be loaded in date dimension. The format of this parameter is just number. Eg: 2
--Created By : DSAMRAJ
/********************************************************************************************************************************/


CREATE PROCEDURE [etl].[LoadDateDimension]
@StartDate DATE,
@NumberOfYears INT 

AS
BEGIN 

    IF OBJECT_ID('tempdb..#dimdate') IS NOT NULL 
        DROP TABLE #dimdate
        
    CREATE TABLE #dimdate
    (
      [DateKey]   INT,
      [Date]       DATE,  
      [Day]        SMALLINT,    
      [Week]        SMALLINT,  
      [WeekDayName] VARCHAR(20),
      [WeekDayNameShort] VARCHAR(10),    
      [DayOfWeek]  SMALLINT,
      [DayOfYear]  SMALLINT,    
      [Month]      SMALLINT,
      [MonthName]  VARCHAR(12),
      [MonthNameShort] VARCHAR(10),      
      [Quarter]     SMALLINT,
      [QuarterName] VARCHAR(10),    
      [Year]       SMALLINT,
      [FirstOfYear]  DATE
    );    
    
	--Set First Day of the week as 7
    SET DATEFIRST 7;
	
	--Set dateformat to month/day/year
    SET DATEFORMAT mdy;
    
	--Set cut off date based on input parameter StartDate
    DECLARE @CutoffDate DATE = DATEADD(YEAR, @NumberOfYears, @StartDate);    
    
	--Insert date values from start date up until cut off date
    INSERT #dimdate([Date]) 
    SELECT d
    FROM
    (
      SELECT d = DATEADD(DAY, rn - 1, @StartDate)
      FROM 
      (
        SELECT TOP (DATEDIFF(DAY, @StartDate, @CutoffDate)) 
          rn = ROW_NUMBER() OVER (ORDER BY s1.[object_id])
        FROM sys.all_objects AS s1
        CROSS JOIN sys.all_objects AS s2
        ORDER BY s1.[object_id]
      ) AS x
    ) AS y;
    
    --Derivation of other date fields
    UPDATE #DimDate 
    set
      [DateKey]            = CAST(CONVERT(CHAR(8),   [date], 112) AS INT),
      [Day]                    = DATEPART(DAY,      [date]),    
      [Week]                = DATEPART(WEEK,     [date]),
      [WeekDayName]            = DATENAME(dw, [date]),
      [WeekDayNameShort]    = UPPER(LEFT(DATENAME(dw, [date]), 3)),    
      [DayOfWeek]            = DATEPART(WEEKDAY,  [date]),
      [DayOfYear]            = CAST(DATENAME(dy, [date]) AS SMALLINT),    
      [Month]                = DATEPART(MONTH,    [date]),
      [MonthName]            = DATENAME(MONTH,    [date]),
      [MonthNameShort]        = UPPER(LEFT(DATENAME(mm, [date]), 3)),      
      [Quarter]                = DATEPART(QUARTER,  [date]),
      [QuarterName]            = (CASE WHEN DATENAME(qq, [date]) = 1
                                    THEN 'First'
                                WHEN DATENAME(qq, [date]) = 2
                                    THEN 'second'
                                WHEN DATENAME(qq, [date]) = 3
                                    THEN 'third'
                                WHEN DATENAME(qq, [date]) = 4
                                    THEN 'fourth'
                                END),
      [Year]                = DATEPART(YEAR,     [date]),
      [FirstOfYear]            = CONVERT(DATE, DATEADD(YEAR,  DATEDIFF(YEAR,  0, [date]), 0));

 

    --Merges data derived in #dimdate into Date Dimension table
    MERGE INTO [dim].[Date] AS Target1
    USING #DimDate AS Source1
    ON (Target1.DateKey = Source1.DateKey)
    
    WHEN NOT MATCHED BY Target
    THEN
    INSERT (
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
        [FirstOfYear],
        [CreatedDate],
        [ModifiedDate])
    VALUES (
        Source1.DateKey,
        Source1.Date,
        Source1.Day,
        Source1.Week,
        Source1.WeekDayName,
        Source1.WeekDayNameShort,
        Source1.DayOfWeek,
        Source1.DayOfYear,
        Source1.Month,
        Source1.MonthName,
        Source1.MonthNameShort,
        Source1.Quarter,
        Source1.QuarterName,
        Source1.Year,
        Source1.FirstOfYear,
        getdate(),
        getdate());
    
	--drop temporary table
    DROP Table #dimdate;
	 

END