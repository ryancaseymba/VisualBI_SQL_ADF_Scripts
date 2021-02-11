/****** Object:  StoredProcedure [etl].[LoadOpsMeasureDimension]    Script Date: 9/16/2020 3:35:06 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/********************************************************************************************************************************/
--Object Name : [etl].[LoadOpsMeasureDimension]
--Purpose	  : Procedure to load data into dim.OpsMeasure table
--Date Creaed : 8/28/2020
--Input Parameters : None
--Created By : HSANKAR
/********************************************************************************************************************************/
 
CREATE PROCEDURE [etl].[LoadOpsMeasureDimension]

AS
BEGIN

SET NOCOUNT ON


--MERGE statement for SCD 2 insert

	INSERT INTO [dim].[OpsMeasure] 
	(
		OpsMeasureID,
		OpsMeasureType,
		OpsMeasureName,
		OpsMeasureParentKey,
		Level,
		Enterprise,
		Division,
		Plant,
		Department,
		EquipmentAndFunctionalArea,
		SubProcessArea,
		Measure,
		IsCurrent,
		EffectiveStartTime,
		EffectiveEndTime,
		IsDeleted,
		CreatedDate,
		ModifiedDate	
	)
	SELECT 
		OpsMeasureID,
		OpsMeasureType,
		OpsMeasureName,
		OpsMeasureParentKey,
		Level,
		Enterprise,
		Division,
		Plant,
		Department,
		EquipmentAndFunctionalArea,
		SubProcessArea,
		Measure,
		IsCurrent,
		EffectiveStartTime,
		EffectiveEndTime,
		IsDeleted,
		CreatedDate,
		ModifiedDate
	FROM 
	(
		MERGE INTO [dim].[OpsMeasure] AS Target1 
		USING 
		(
			SELECT 
				S.OpsMeasureID   AS OpsMeasureID,
				S.OpsMeasureType AS OpsMeasureType,
				S.OpsMeasureName AS OpsMeasureName,
				(
					SELECT 
						D.OpsMeasureKey 
					FROM 
						[dim].[OpsMeasure] AS D 
					WHERE 
						D.OpsMeasureID = S.OpsMeasureParentID AND
						D.IsCurrent     = 1
				) AS OpsMeasureParentKey,
				S.Level,
				CASE 
				WHEN Level1Type = 'Enterprise' THEN
					Level1Name
				ELSE
					NULL
			  END AS Enterprise,
			  CASE 
				WHEN Level2Type = 'Division' THEN
					Level2Name
				ELSE
					NULL
			  END AS Division,
			  CASE 
				WHEN Level3Type = 'Plant' THEN
					Level3Name
				ELSE
					NULL
			  END AS Plant,
			  CASE 
				WHEN Level4Type = 'Department' THEN
					Level4Name
				ELSE
					NULL
			  END AS Department,
			  CASE 
				WHEN Level5Type = 'Equipment' OR Level5Type = 'Functional Area' THEN
					Level5Name
				ELSE
					NULL
			  END AS "EquipmentAndFunctionalArea",
			  CASE 
				WHEN Level6Type = 'Sub Process Area' THEN
					Level6Name
				ELSE
					NULL
			  END AS SubProcessArea,
			  CASE 
				WHEN OpsMeasureType = 'Measure' THEN
					OpsMeasureName
				ELSE
					NULL
			  END AS Measure
			FROM 
				[stage].[OpsMeasure] AS S
		) AS Source1 

		ON (Target1.OpsMeasureID = Source1.OpsMeasureID) 

		WHEN MATCHED AND 
			(EffectiveEndTime = '9999-12-31') AND (IsCurrent = 1) AND 
			(Target1.OpsMeasureType != Source1.OpsMeasureType) AND 
			(Target1.OpsMeasureName != Source1.OpsMeasureName) AND
			(Target1.OpsMeasureParentKey != Source1.OpsMeasureParentKey) AND
			(Target1.Enterprise != Source1.Enterprise) AND
			(Target1.Division != Source1.Division) AND
			(Target1.Plant != Source1.Plant) AND
			(Target1.Department != Source1.Department) AND
			(Target1.EquipmentAndFunctionalArea != Source1.EquipmentAndFunctionalArea) AND
			(Target1.SubProcessArea != Source1.SubProcessArea) AND
			(Target1.Measure != Source1.Measure) AND
			(Target1.Level != Source1.Level)
		  THEN 
			UPDATE SET Target1.EffectiveEndTime = GetDate(), IsCurrent = 0, ModifiedDate = GETDATE()      

		  WHEN NOT MATCHED BY TARGET THEN 
			  INSERT 
			  (
				OpsMeasureID,
				OpsMeasureType,
				OpsMeasureName,
				OpsMeasureParentKey,
				Level,
				Enterprise,
				Division,
				Plant,
				Department,
				EquipmentAndFunctionalArea,
				SubProcessArea,
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
				Source1.OpsMeasureID,
				Source1.OpsMeasureType,
				Source1.OpsMeasureName,
				NULL,
				Source1.Level,
				Source1.Enterprise,
				Source1.Division,
				Source1.Plant,
				Source1.Department,
				Source1.EquipmentAndFunctionalArea,
				Source1.SubProcessArea,
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
			Source1.OpsMeasureID,
			Source1.OpsMeasureType,
			Source1.OpsMeasureName,
			Source1.OpsMeasureParentKey,
			Source1.Level,
			Source1.Enterprise,
			Source1.Division,
			Source1.Plant,
			Source1.Department,
			Source1.EquipmentAndFunctionalArea,
			Source1.SubProcessArea,
			Source1.Measure,
			1 AS IsCurrent,
			GETDATE() AS EffectiveStartTime,
			'9999-12-31' AS EffectiveEndTime,
			0 AS IsDeleted,
			GETDATE() AS CreatedDate,
			NULL AS ModifiedDate

	) AS CHANGES     

	WHERE action='UPDATE' AND OpsMeasureID IS NOT NULL;

	UPDATE D SET
		D.OpsMeasureParentKey = P.OpsMeasureKey
	FROM 
		[dim].[OpsMeasure] AS D
	LEFT OUTER JOIN
		[stage].[OpsMeasure] AS S	
	ON 
		D.OpsMeasureID = S.OpsMeasureID
	LEFT OUTER JOIN 
		[dim].[OpsMeasure] AS P
	ON 
		S.OpsMeasureParentID = P.OpsMeasureID 
	WHERE
		D.IsCurrent = 1 AND
		P.IsCurrent = 1 AND
		D.OpsMeasureParentKey IS NULL; 

END


GO


