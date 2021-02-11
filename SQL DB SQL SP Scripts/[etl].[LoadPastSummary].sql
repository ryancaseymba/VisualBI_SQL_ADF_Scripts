/****** Object:  StoredProcedure [etl].[LoadPastShiftSummary]    Script Date: 9/16/2020 3:35:28 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



--Truncate table fact.pastshiftsummary

/********************************************************************************************************************************/
--Object Name : [etl].[LoadPastSummary]
--Purpose	  :  Procedure to load data past summary data rolled up for every hour
--Date Created : 8/11/2020
--Input Parameters : none
--Created By : bmayandi
/********************************************************************************************************************************/
CREATE PROCEDURE [etl].[LoadPastShiftSummary] 
AS
BEGIN
	DECLARE @dt2 Datetime,@dt1 Datetime

	SELECT @dt2 =MAX(ShiftStartTime) FROM [stage].[PastShiftSummary]
	SELECT @dt1 =DATEADD(ss,-1,MAX(ShiftEndTime)) FROM [stage].[PastShiftSummary]

	DECLARE @stgSummary TABLE
		(ReportingTime			DATETIME,
		ElementID			UNIQUEIDENTIFIER,
		ElementAttributeID		UNIQUEIDENTIFIER,
		Attribute			NVARCHAR(250),
		HourlyAverage			FLOAT,
		HourlyMinValue			FLOAT,
		HourlyMaxValue			FLOAT,
		HourlyCummulativeTotal		FLOAT,
		RunningHourlyAverage 		FLOAT);

	DECLARE @stgStatusSwitch TABLE
		([ElementAttributeID]		UNIQUEIDENTIFIER,
		[ElementID]			UNIQUEIDENTIFIER,
		[IntialStatus]			VARCHAR(100),
		[IntialStatusTime]		DATETIME,
		[NextStatusTime]		DATETIME,
		[NextStatus]			VARCHAR(100));

	DECLARE @stgSummaryWithStatus TABLE
		([ElementAttributeID]		UNIQUEIDENTIFIER,
		[ElementID]			UNIQUEIDENTIFIER,
		[Attribute]			NVARCHAR(250),
		[Value]				VARCHAR(1000),
		[Time]				DATETIME,
		[StatusFlag]			VARCHAR(1));

	IF OBJECT_ID('tempdb..#tmpPastShiftData') IS NOT NULL
		DROP TABLE #tmpPastShiftData
	
	SELECT 
		ElementAttributeID,
		Time,
		ElementID, 
		ElementPath, 
		ElementHierarchyName, 
		Attribute, 
		Value, 
		CategoryName, 
		PipelineRunTime
	INTO #tmpPastShiftData
	FROM [stage].[PastShiftSummary]
	WHERE TIME<=@dt1
	--select * from #tmpPastShiftData
	--Indentify the start and end time of when the element was OFF
	INSERT INTO @stgStatusSwitch
		([ElementAttributeID] 
		,[ElementID]
		,[IntialStatus] 
		,[IntialStatusTime]
		,[NextStatusTime] 
		,[NextStatus] )
	SELECT 
		ElementAttributeID,
		ElementId,
		[value] AS IntialStatus,
		[time] AS InitialStatustime,
		LEAD([time]) OVER (PARTITION BY ElementAttributeID,ElementId ORDER BY [time]) NextStatusTime,
		LEAD([value],1,0) OVER (PARTITION BY ElementAttributeID,ElementId ORDER BY [time]) NextStatus
	FROM #tmpPastShiftData                        
	WHERE categoryname LIKE'%Run Status%';
	

	--Flag the Shift data based on element availability status
	INSERT INTO @stgSummaryWithStatus 
		([ElementAttributeID] 
		,[ElementID]
		,[Attribute]
		,[Value]
		,[Time]
		,[StatusFlag] )
	SELECT 
		sp.ElementAttributeID
		,sp.ElementID
		,sp.Attribute
		,sp.[Value]
		,sp.[Time]
		,CASE 
			WHEN ss.[IntialStatus]='OFF' AND sp.[Time] > ss.IntialStatusTime AND sp.[Time] <=ss.NextStatusTime 
			THEN 0 ELSE 1 
		END AS StatusFlag
	FROM #tmpPastShiftData sp 
	LEFT JOIN @stgStatusSwitch ss
		ON (sp.ElementAttributeID=ss.ElementAttributeID
		AND sp.ElementID= ss.ElementID)
	WHERE sp.categoryname NOT LIKE'%Run Status%'

	INSERT INTO @stgSummary
		([ReportingTime]
		,[ElementID]
		,[ElementAttributeId]
		,[Attribute]
		,[HourlyAverage]
		,[HourlyMinValue]
		,[HourlyMaxValue]
		,[HourlyCummulativeTotal]
		,[RunningHourlyAverage])
	SELECT 
		ReportingTime,
		ElementID,
		ElementAttributeId,
		Attribute,
		AVG([ValueNumber]) AS HourlyAverage,
		MIN([ValueNumber]) AS HourlyMinValue,
		MAX([ValueNumber]) AS HourlyMaxValue,
		CASE 
			WHEN Attribute IN('Conditioning pH','Level %') OR Attribute LIKE '%level' 
				OR Attribute LIKE '%levels' THEN null
			ELSE SUM([ValueNumber]) 
		END AS HourlyCummulativeTotal,
		AVG(CASE WHEN StatusFlag =1 THEN [ValueNumber] ELSE 0 END) AS RunningHourlyAverage				
	FROM
		(SELECT 
			ABS(FLOOR(DATEDIFF(Minute,@dt2,[TIME])*1.00/60)) as TimeBand,
			DATEADD(minute,ABS(FLOOR(DATEDIFF(Minute,@dt2,[TIME])*1.00/60)+1)*60,@dt2) AS ReportingTime,
			ElementID,
			ElementAttributeId,
			Attribute,
			[Time],
			CASE 
				WHEN [Value]='Calc Failed' THEN NULL 
				WHEN ISNUMERIC([Value])=1 THEN cast([Value] as float)
				ELSE NULL 
			END AS ValueNumber,
			StatusFlag
		FROM @stgSummaryWithStatus
		WHERE  Attribute  NOT LIKE '%mode' 
		AND Attribute  NOT LIKE '%L/H'
		AND Attribute  NOT LIKE '%on/off' 
		) tbl 

	GROUP BY ReportingTime,
	Attribute,
	ElementID,
	ElementAttributeId	
					
	MERGE INTO [fact].[PastShiftSummary] tgt
	USING 
		(SELECT 
			DateKey AS ReportingDateKey,
			TimeKey AS ReportingTimeKey,
			COALESCE(loc.LocationKey,-1) AS LocationKey,
			COALESCE((CASE WHEN LEFT(CAST(CAST((sm.ReportingTime) AS TIME)AS VARCHAR),8) BETWEEN daycrewshift.ShiftStartTime AND daycrewshift.ShiftEndTime THEN daycrewshift.ShiftEndDateKey ELSE nightcrewshift.ShiftEndDateKey END),
					 (CASE WHEN LEFT(CAST(CAST((sm.ReportingTime) AS TIME)AS VARCHAR),8) BETWEEN dayshift.ShiftStartTime AND dayshift.ShiftEndTime THEN da.DateKey ELSE da.NextDateKey END),
					 -1) as ShiftEndDateKey,
			COALESCE((CASE WHEN LEFT(CAST(CAST((sm.ReportingTime) AS TIME)AS VARCHAR),8) BETWEEN dayshift.ShiftStartTime AND dayshift.ShiftEndTime THEN dayshift.ShiftKey ELSE nightshift.ShiftKey END),
					-1) as ShiftTimeKey,
			COALESCE((CASE WHEN LEFT(CAST(CAST((sm.ReportingTime) AS TIME)AS VARCHAR),8) BETWEEN daycrewshift.ShiftStartTime AND daycrewshift.ShiftEndTime THEN daycrew.CrewKey ELSE nightcrew.CrewKey END),-1) as CrewKey,
			COALESCE(AttributeKey,-1) AS AttributeKey,
			HourlyAverage,
			HourlyMinValue,
			HourlyMaxValue,
			HourlyCummulativeTotal,
			RunningHourlyAverage
		FROM @stgsummary sm
		INNER JOIN [dim].[Attribute] Atr
			ON sm.ElementAttributeID= Atr.AttributeID
			AND sm.Attribute=Atr.Name
			AND Atr.IsCurrent=1
		LEFT JOIN  (select lead(Date,1) OVER(ORDER BY Date) AS NextDate,lead(DateKey,1) OVER(ORDER BY Date) AS NextDateKey,* from [dim].[Date]) da
			ON CONVERT(DATE,(sm.ReportingTime))=da.[Date]
		LEFT JOIN  [dim].[Time] t
			ON CAST((sm.ReportingTime) AS TIME)=t.Time24hr		
		LEFT JOIN [dim].[Location] loc
			ON sm.ElementID= loc.ElementID
		LEFT JOIN [dim].[Location] plt
			ON loc.Plant= plt.Name
		LEFT JOIN etl.CrewShiftMapping daycrewshift 
			ON (CONVERT(DATE,(sm.ReportingTime)) = daycrewshift.productiondate 
			AND plt.MineID = daycrewshift.MineID AND daycrewshift.ShiftOrder = 1)
		LEFT JOIN etl.CrewShiftMapping nightcrewshift 
			ON (CONVERT(DATE,(sm.ReportingTime)) = nightcrewshift.productiondate 
			AND plt.MineID = nightcrewshift.MineID AND nightcrewshift.ShiftOrder = 2)
		LEFT JOIN dim.crew daycrew 
			on (daycrewshift.CrewCode = daycrew.CrewCode AND daycrewshift.CrewID = daycrew.CrewID
				AND daycrew.IsCurrent = 1) 
		LEFT JOIN dim.crew nightcrew 
			on (nightcrewshift.CrewCode = nightcrew.CrewCode AND nightcrewshift.CrewID = nightcrew.CrewID
				AND nightcrew.IsCurrent = 1)
		LEFT JOIN dim.Shift dayshift
			on (plt.LocationKey = dayshift.LocationKey AND dayshift.ShiftOrder = 1
				AND dayshift.IsCurrent = 1)
		LEFT JOIN dim.Shift nightshift
			on (plt.LocationKey = nightshift.LocationKey AND nightshift.ShiftOrder = 2
				AND nightshift.IsCurrent = 1))src
	ON src.LocationKey=tgt.LocationKey
	AND src.AttributeKey=tgt.AttributeKey
	AND src.ReportingDateKey = tgt.ReportingDateKey
	AND src.ReportingTimeKey = tgt.ReportingTimeKey

	WHEN MATCHED AND (src.HourlyAverage<>tgt.Averagehourly	
	OR src.HourlyMinValue<>tgt.MinValuehourly
	OR src.HourlyMaxValue<>tgt.MaxValuehourly
	OR src.HourlyCummulativeTotal<>tgt.CumulativeTotalhourly
	OR src.RunningHourlyAverage<>tgt.ActualRunningAverage)	

	THEN UPDATE SET 
		tgt.Averagehourly=src.HourlyAverage	
		,tgt.MinValuehourly=src.HourlyMinValue
		,tgt.MaxValuehourly=src.HourlyMaxValue
		,tgt.CumulativeTotalhourly=src.HourlyCummulativeTotal	
		,tgt.ActualRunningAverage=src.RunningHourlyAverage
		,tgt.ModifiedDate = GETDATE()

	WHEN NOT MATCHED THEN INSERT 
		(ReportingDateKey
		,ReportingTimeKey		
		,[LocationKey]--assertskid
		,[CrewKey]
		,[ShiftEndDateKey]
		,[ShiftTimeKey]
		,[AttributeKey]
		,[Averagehourly]
		,[MinValuehourly]
		,[MaxValuehourly]
		,[CumulativeTotalhourly]
		,[ActualRunningAverage]
		,[CreatedDate]
		,[ModifiedDate])
	VALUES 
		(src.ReportingDateKey
		,src.ReportingTimeKey
		,src.[LocationKey]
		,src.[CrewKey]
		,src.[ShiftEndDateKey]
		,src.[ShiftTimeKey]
		,src.[AttributeKey]
		,src.[HourlyAverage]
		,src.[HourlyMinValue]
		,src.[HourlyMaxValue]
		,src.[HourlyCummulativeTotal]
		,src.[RunningHourlyAverage]
		,GETDATE()
		,GETDATE());

END

GO


