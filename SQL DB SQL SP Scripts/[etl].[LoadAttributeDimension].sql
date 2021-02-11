/****** Object:  StoredProcedure [etl].[LoadAttributeDimension]    Script Date: 9/16/2020 3:31:35 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/********************************************************************************************************************************/
--Object Name : [etl].[LoadAttributeDimension]
--Purpose	  : Procedure to load data into dim.Attribute table
--Date Creaed : 8/17/2020
--Input Parameters : None
--Created By : DSAMRAJ
/********************************************************************************************************************************/
 
CREATE PROCEDURE [etl].[LoadAttributeDimension]

AS
BEGIN

SET NOCOUNT ON


--MERGE statement for SCD 2 insert

INSERT INTO [dim].[Attribute] 
(
	AttributeID,
	UnitOfMeasureKey,
	ElementKey,
	EnumerationSetName,
	IsConfigurationItem,
	IsHidden,
	IsManualDataEntry,
	Level,
	Name,
	Path,
	TraitType,
	IsCurrent,
	EffectiveStartTime,
	EffectiveEndTime,
	IsDeleted,
	CreatedDate,
	ModifiedDate
)
SELECT 
	AttributeID,
	UnitOfMeasureKey,
	ElementKey,
	EnumerationSetName,
	IsConfigurationItem,
	IsHidden,
	IsManualDataEntry,
	Level,
	Name,
	Path,
	TraitType,
	IsCurrent,
	EffectiveStartTime,
	EffectiveEndTime,
	IsDeleted,
	CreatedDate,
	ModifiedDate
FROM 
(
    MERGE INTO [dim].[Attribute] AS Target1 
    USING 
	(
		SELECT 
			ID as AttributeID, 
			EnumerationSetName, 
			IsConfigurationItem, 
			IsHidden, 
			IsManualDataEntry, 
			Level, 
			Name, 
			Path, 
			TraitType,
			COALESCE(UnitOfMeasureKey,-1) AS UnitOfMeasureKey
		FROM [stage].[Attribute] Atr
		LEFT JOIN 
			(SELECT UnitOfMeasureKey,COALESCE(Map.PIUOMName,dim.UnitOfMeasureName) AS UomName
			FROM [dim].[UnitOfMeasure] dim
			LEFT JOIN  [config].[UOMMapping] Map
				ON dim.UnitOfMeasureName = Map.UomName) dimUom
			ON Atr.UomName=dimUom.UomName
	) AS Source1 

	ON (Target1.AttributeID = Source1.AttributeID) 

	WHEN MATCHED AND 
		(EffectiveEndTime = '9999-12-31') AND (IsCurrent = 1) AND 
		(Target1.EnumerationSetName != Source1.EnumerationSetName) AND
		(Target1.IsConfigurationItem != Source1.IsConfigurationItem) AND
		(Target1.IsHidden != Source1.IsHidden) AND
		(Target1.IsManualDataEntry != Source1.IsManualDataEntry) AND
		(Target1.Level != Source1.Level) AND
		(Target1.Name != Source1.Name) AND
		(Target1.Path != Source1.Path) AND
		(Target1.TraitType != Source1.TraitType) AND
		(Target1.UnitOfMeasureKey != Source1.UnitOfMeasureKey)
      THEN 
      UPDATE SET Target1.EffectiveEndTime = GetDate(), IsCurrent = 0, ModifiedDate = GETDATE()      

      WHEN NOT MATCHED BY Target
      THEN 
      INSERT 
	  (
		AttributeID,
		UnitOfMeasureKey,
		ElementKey,
		EnumerationSetName,
		IsConfigurationItem,
		IsHidden,
		IsManualDataEntry,
		Level,
		Name,
		Path,
		TraitType,
		IsCurrent,
		EffectiveStartTime,
		EffectiveEndTime,
		IsDeleted,
		CreatedDate,
		ModifiedDate
	  ) 
      VALUES 
	  (
		Source1.AttributeID,
		Source1.UnitOfMeasureKey,
		-1,
		Source1.EnumerationSetName,
		Source1.IsConfigurationItem,
		Source1.IsHidden,
		Source1.IsManualDataEntry,
		Source1.Level,
		Source1.Name,
		Source1.Path,
		Source1.TraitType,
		1,
		'1963-01-01',
		'9999-12-31',
		0,
		GETDATE(),
		NULL
	  ) 

      WHEN NOT MATCHED BY Source
      THEN 
      UPDATE SET Target1.IsDeleted = 1    

      OUTPUT 
		$action AS action,
		Source1.AttributeID,
		Source1.UnitOfMeasureKey,
		-1 AS ElementKey,
		Source1.EnumerationSetName,
		Source1.IsConfigurationItem,
		Source1.IsHidden,
		Source1.IsManualDataEntry,
		Source1.Level,
		Source1.Name,
		Source1.Path,
		Source1.TraitType,
		1 AS IsCurrent,
		GETDATE() AS EffectiveStartTime,
		'9999-12-31' AS EffectiveEndTime,
		0 AS IsDeleted,
		GETDATE() AS CreatedDate,
		NULL AS ModifiedDate

) as Changes     

WHERE action='UPDATE' AND AttributeID IS NOT NULL;

END


GO


