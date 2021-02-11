/****** Object:  StoredProcedure [etl].[LoadUnitOfMeasureDimension]    Script Date: 9/16/2020 3:36:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/********************************************************************************************************************************/
--Object Name : [etl].[LoadUnitOfMeasureDimension]
--Purpose	  : Procedure to load data into dim.UnitOfMeasure table
--Date Creaed : 9/17/2020
--Input Parameters : None
--Created By : BMAYANDI
/********************************************************************************************************************************/
 
CREATE PROCEDURE [etl].[LoadUnitOfMeasureDimension]

AS
BEGIN
	SET NOCOUNT ON
	MERGE INTO [dim].[UnitOfMeasure] AS tgt
	USING (
		SELECT 
			UOMID,
			COALESCE(OIS.UomName,PIUom.UomName,fpa.UomName) AS UOMName
		FROM [stage].[UnitOfMeasure] OIS
		FULL OUTER JOIN 
			(SELECT DISTINCT COALESCE(Map.UomName,PIStg.UomName) AS UomName
			FROM [stage].[Attribute] PIStg
			LEFT JOIN  [config].[UOMMapping] Map
				ON PIStg.UomName = Map.[PIUOMName]) PIUom
			ON OIS.UomName=PIUom.UomName
		FULL OUTER JOIN 
			(SELECT DISTINCT UnitOfMeasure  AS UomName
			FROM [stage].[FPAUnitOfMeasure]
			WHERE [UnitOfMeasure] IS NOT NULL
				AND [UnitOfMeasure] NOT LIKE '%blank%') fpa
			ON OIS.UomName=fpa.UomName) src
		ON  (tgt.[UnitOfMeasureName] = src.UOMName)
	WHEN MATCHED AND (tgt.UnitOfMeasureID != src.UOMID)
	THEN UPDATE
		SET tgt.UnitOfMeasureID = src.UOMID
		,tgt.ModifiedDate = GETDATE()
	WHEN NOT MATCHED BY TARGET
	THEN INSERT
		(UnitOfMeasureID
		,UnitOfMeasureName
		,CreatedDate
		,ModifiedDate)
	VALUES
		(src.UOMID
		,src.UOMName
		,GETDATE()
		,GETDATE());
END
GO


