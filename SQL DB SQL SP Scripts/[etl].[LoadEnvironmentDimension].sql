/****** Object:  StoredProcedure [etl].[LoadEnvironmentDimension]    Script Date: 9/16/2020 3:34:05 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/********************************************************************************************************************************/
--Object Name : [etl].[LoadEnvironmentDimension]
--Purpose	  : Procedure to load data into dim.Environment table
--Date Creaed : 8/28/2020
--Input Parameters : None
--Created By : HSANKAR
/********************************************************************************************************************************/
 
CREATE PROCEDURE [etl].[LoadEnvironmentDimension]

AS
BEGIN

SET NOCOUNT ON


--MERGE statement for SCD 2 insert

	INSERT INTO [dim].[Environment] 
	(
		EnvironmentID,
		EnvironmentType,
		EnvironmentName,
		EnvironmentParentKey,
		Level,
		Enterprise,
		Class,
		MeasureGroup,
		Measure,
		IsCurrent,
		EffectiveStartTime,
		EffectiveEndTime,
		IsDeleted,
		CreatedDate,
		ModifiedDate	
	)
	SELECT 
		EnvironmentID,
		EnvironmentType,
		EnvironmentName,
		EnvironmentParentKey,
		Level,
		Enterprise,
		Class,
		MeasureGroup,
		Measure,
		IsCurrent,
		EffectiveStartTime,
		EffectiveEndTime,
		IsDeleted,
		CreatedDate,
		ModifiedDate
	FROM 
	(
		MERGE INTO [dim].[Environment] AS Target1 
		USING 
		(
			SELECT 
				S.EnvironmentID   AS EnvironmentID,
				S.EnvironmentType AS EnvironmentType,
				S.EnvironmentName AS EnvironmentName,
				(
					SELECT 
						D.EnvironmentKey 
					FROM 
						[dim].[Environment] AS D 
					WHERE 
						D.EnvironmentID = S.EnvironmentParentID AND
						D.IsCurrent     = 1
				) AS EnvironmentParentKey,
				S.Level,
				CASE 
					WHEN Level1Type = 'Enterprise' THEN
						Level1Name
					ELSE
						NULL
				END AS Enterprise,
				CASE 
					WHEN Level2Type = 'Class' THEN
						Level2Name
					ELSE
						NULL
				END AS Class,
				CASE 
					WHEN Level3Type = 'Measure Group' THEN
						Level3Name
					ELSE
						NULL
				END AS MeasureGroup,
				CASE 
					WHEN EnvironmentType = 'Measure' THEN
						EnvironmentName
					ELSE
						NULL
				END AS Measure
			FROM 
				[stage].[Environment] AS S
		) AS Source1 

		ON (Target1.EnvironmentID = Source1.EnvironmentID) 

		WHEN MATCHED AND 
			(EffectiveEndTime = '9999-12-31') AND (IsCurrent = 1) AND 
			(Target1.EnvironmentType != Source1.EnvironmentType) AND 
			(Target1.EnvironmentName != Source1.EnvironmentName) AND
			(Target1.EnvironmentParentKey != Source1.EnvironmentParentKey) AND
			(Target1.Enterprise != Source1.Enterprise) AND
			(Target1.Class != Source1.Class) AND
			(Target1.MeasureGroup != Source1.MeasureGroup) AND
			(Target1.Measure != Source1.Measure) AND
			(Target1.Level != Source1.Level)
		  THEN 
			UPDATE SET Target1.EffectiveEndTime = GetDate(), IsCurrent = 0, ModifiedDate = GETDATE()      

		  WHEN NOT MATCHED BY TARGET THEN 
			  INSERT 
			  (
				EnvironmentID,
				EnvironmentType,
				EnvironmentName,
				EnvironmentParentKey,
				Level,
				Enterprise,
				Class,
				MeasureGroup,
				Measure,
				IsCurrent,
				EffectiveStartTime,
				EffectiveEndTime,
				IsDeleted,
				CreatedDate,
				ModifiedDate
			  ) 
			  VALUES 
			  (
				Source1.EnvironmentID,
				Source1.EnvironmentType,
				Source1.EnvironmentName,
				NULL,
				Source1.Level,
				Source1.Enterprise,
				Source1.Class,
				Source1.MeasureGroup,
				Source1.Measure,
				1,
				GETDATE(),
				'9999-12-31',
				0,
				GETDATE(),
				NULL
			  ) 

		  WHEN NOT MATCHED BY SOURCE THEN 
			UPDATE SET Target1.IsDeleted = 1    

		  OUTPUT 
			$action AS action,
			Source1.EnvironmentID,
			Source1.EnvironmentType,
			Source1.EnvironmentName,
			Source1.EnvironmentParentKey,
			Source1.Level,
			Source1.Enterprise,
			Source1.Class,
			Source1.MeasureGroup,
			Source1.Measure,
			1 AS IsCurrent,
			GETDATE() AS EffectiveStartTime,
			'9999-12-31' AS EffectiveEndTime,
			0 AS IsDeleted,
			GETDATE() AS CreatedDate,
			NULL AS ModifiedDate

	) AS CHANGES     

	WHERE action='UPDATE' AND EnvironmentID IS NOT NULL;

	UPDATE D SET
		D.EnvironmentParentKey = P.EnvironmentKey
	FROM 
		[dim].[Environment] AS D
	LEFT OUTER JOIN
		[stage].[Environment] AS S	
	ON 
		D.EnvironmentID = S.EnvironmentID
	LEFT OUTER JOIN 
		[dim].[Environment] AS P
	ON 
		S.EnvironmentParentID = P.EnvironmentID 
	WHERE
		D.IsCurrent = 1 AND
		P.IsCurrent = 1 AND
		D.EnvironmentParentKey IS NULL; 

END


GO


