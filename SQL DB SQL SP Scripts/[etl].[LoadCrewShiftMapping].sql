/****** Object:  StoredProcedure [etl].[LoadCrewShiftMapping]    Script Date: 9/16/2020 3:32:46 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/********************************************************************************************************************************/
--Object Name : [etl].[LoadCrewShiftMapping]
--Purpose	  : Procedure to load CrewShiftMapping table. Lookup is done on dim.Shift to get location information.
--Date Creaed : 9/4/2020
--Created By : DSAMRAJ
/********************************************************************************************************************************/

CREATE PROCEDURE [etl].[LoadCrewShiftMapping]


 AS     
 BEGIN      
 
	SET NOCOUNT ON 

		TRUNCATE TABLE etl.crewShiftMapping

		INSERT INTO etl.CrewShiftMapping 
		SELECT 
			src.ShiftID,
			src.ShiftDescription,
			src.ShiftOrder,
			lkplocation.LocationID,
			lkplocation.MineID,
			src.CrewID,
			src.CrewCode,
			src.ScheduleID,
			src.ShiftStartDate,
			src.ShiftEndDate,
			src.ShiftStartTime,
			src.ShiftEndTime,
			lkpdate.DateKey as ShiftEndDateKey,
			lkptime.TimeKey as ShiftStartTimeKey,
			src.ProductionDate, 
			GetDate() as CreatedDate,
			GetDate() as ModifiedDate
		FROM stage.CrewShiftMapping src
		LEFT JOIN dim.Shift lkpshift ON (src.ScheduleID = lkpshift.ScheduleID and src.ShiftOrder = lkpshift.ShiftOrder AND lkpshift.IsCurrent = 1)
		LEFT JOIN dim.Date lkpdate ON (src.ShiftEndDate = lkpdate.Date)
		LEFT JOIN dim.Time lkptime ON (src.ShiftStartTime = lkptime.Time24HrTimeType)
		LEFT JOIN dim.Location lkplocation ON (lkpshift.LocationKey = lkplocation.LocationKey) 


END
GO


