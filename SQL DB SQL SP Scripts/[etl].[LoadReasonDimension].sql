/****** Object:  StoredProcedure [etl].[LoadReasonDimension]    Script Date: 9/16/2020 3:35:49 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/********************************************************************************************************************************/
--Object Name : [etl].[LoadReasonDimension]
--Purpose	  : Procedure to load data into dim.Reason table
--Date Creaed : 8/28/2020
--Input Parameters : None
--Created By : HSANKAR
/********************************************************************************************************************************/
 
CREATE PROCEDURE [etl].[LoadReasonDimension]

AS
BEGIN

SET NOCOUNT ON
 
	CREATE TABLE #Temp_Reason_Parent(
		ReasonID        INT PRIMARY KEY,
		ReasonCode      VARCHAR(25),
		ReasonHierarchy VARCHAR(25),
		ReasonName      VARCHAR(255),
		ReasonParentID  INT,
		OeeCategory     VARCHAR(100),
		LeveL           INT
	);

	Create Table #Temp_Stage_Reason(
		ReasonID         INT PRIMARY KEY,
		ReasonHierarchy  VARCHAR(25),
		ReasonName       VARCHAR(255),
		ReasonParentID   INT,
		OeeCategory      VARCHAR(100),
		Level1Name       VARCHAR(255),
		Level2Name       VARCHAR(255),
		Level3Name       VARCHAR(255),
		Level4Name       VARCHAR(255),
		Level5Name       VARCHAR(255),
		Level6Name       VARCHAR(255),
		Level7Name       VARCHAR(255),
		Level8Name       VARCHAR(255),
		Level9Name       VARCHAR(255),
		Level10Name      VARCHAR(255),
		Level            INT
	);

	WITH lv_stage_Reason AS
	(
		SELECT 
			ReasonID,	
			ReasonCode,
			ReasonCodeParent AS ReasonParentCode,
			ReasonHierarchy,
			ReasonName,
			OeeCategory,
			(LEN(ReasonPath)-LEN(REPLACE(ReasonPath, '\', '')))+1 AS Level
		FROM 
			stage.Reason
	),

	lv_Reason_Parent AS 
	(
		SELECT 
			C.ReasonID        AS ReasonID,
			C.ReasonCode      AS ReasonCode,
			C.ReasonHierarchy AS ReasonHierarchy,
			C.ReasonName      AS ReasonName,
			C.OeeCategory     AS OeeCategory,
			P.ReasonID        AS ReasonParentID,
			C.Level AS Level
		FROM 
			lv_Stage_Reason AS C 
		LEFT OUTER JOIN Stage.Reason AS P 
			ON C.ReasonParentCode = P.ReasonCode AND 
			   C.ReasonHierarchy  = P.ReasonHierarchy
	)

	INSERT INTO #Temp_Reason_Parent
										SELECT
											ReasonID,
											ReasonCode,
											ReasonHierarchy,
											ReasonName,
											ReasonParentID,
											OeeCategory,
											Level
										 FROM
											lv_Reason_Parent
							

	CREATE INDEX Parent_Index ON #Temp_Reason_Parent (ReasonParentID); 

	WITH lv_Reason_Hierarchy AS
	(
		SELECT 
			ReasonID AS CurID,
			ReasonID,
			ReasonHierarchy,
			ReasonName AS ReasonName,
			ReasonParentID,
			OeeCategory,
			ReasonName AS Level1Name,
			CAST(NULL AS VARCHAR(255)) AS Level2Name,
			CAST(NULL AS VARCHAR(255)) AS Level3Name,
			CAST(NULL AS VARCHAR(255)) AS Level4Name,
			CAST(NULL AS VARCHAR(255)) AS Level5Name,
			CAST(NULL AS VARCHAR(255)) AS Level6Name,
			CAST(NULL AS VARCHAR(255)) AS Level7Name,
			CAST(NULL AS VARCHAR(255)) AS Level8Name,
			CAST(NULL AS VARCHAR(255)) AS Level9Name,
			CAST(NULL AS VARCHAR(255)) AS Level10Name,
			Level
		FROM 
			#Temp_Reason_Parent
		WHERE Level = 1
		UNION ALL
		SELECT 
			H.CurID           AS CurID,
			P.ReasonID        AS ReasonID,
			P.ReasonHierarchy AS ReasonHierarchy,
			P.ReasonName      AS ReasonName,
			P.ReasonParentID  AS ReasonParentID,
			P.OeeCategory     AS OeeCategory,
			H.Level1Name,
			CASE WHEN P.Level = 2  THEN P.ReasonName ELSE H.Level2Name END AS Level2Name,
			CASE WHEN P.Level = 3  THEN P.ReasonName ELSE H.Level3Name END AS Level3Name,
			CASE WHEN P.Level = 4  THEN P.ReasonName ELSE H.Level4Name END AS Level4Name,
			CASE WHEN P.Level = 5  THEN P.ReasonName ELSE H.Level5Name END AS Level5Name,
			CASE WHEN P.Level = 6  THEN P.ReasonName ELSE H.Level6Name END AS Level6Name,
			CASE WHEN P.Level = 7  THEN P.ReasonName ELSE H.Level7Name END AS Level7Name,
			CASE WHEN P.Level = 8  THEN P.ReasonName ELSE H.Level8Name END AS Level8Name,
			CASE WHEN P.Level = 9  THEN P.ReasonName ELSE H.Level9Name END AS Level9Name,
			CASE WHEN P.Level = 10 THEN P.ReasonName ELSE H.Level10Name END AS Level10Name,
			P.Level AS Level
		FROM
			#Temp_Reason_Parent AS P
		JOIN
			lv_Reason_Hierarchy AS H
			ON H.ReasonID = P.ReasonParentID
	)

	INSERT INTO #Temp_Stage_Reason
									SELECT
										ReasonID,
										ReasonHierarchy,
										ReasonName,
										ReasonParentID,
										OeeCategory,
										Level1Name,
										Level2Name,
										Level3Name,
										Level4Name,
										Level5Name,
										Level6Name,
										Level7Name,
										Level8Name,
										Level9Name,
										Level10Name,
										Level
									 FROM
										lv_Reason_Hierarchy
	
	
	--MERGE statement for SCD 2 insert

	INSERT INTO [dim].[Reason] 
	(
		ReasonID,
		ReasonHierarchy,
		ReasonName,
		ReasonParentKey,
		OeeCategory,
		Level1Name,
		Level2Name,
		Level3Name,
		Level4Name,
		Level5Name,
		Level6Name,
		Level7Name,
		Level8Name,
		Level9Name,
		Level10Name,
		Level,
		IsCurrent,
		EffectiveStartTime,
		EffectiveEndTime,
		IsDeleted,
		CreatedDate,
		ModifiedDate	
	)
	SELECT 
		ReasonID,
		ReasonHierarchy,
		ReasonName,
		ReasonParentKey,
		OeeCategory,
		Level1Name,
		Level2Name,
		Level3Name,
		Level4Name,
		Level5Name,
		Level6Name,
		Level7Name,
		Level8Name,
		Level9Name,
		Level10Name,
		Level,
		IsCurrent,
		EffectiveStartTime,
		EffectiveEndTime,
		IsDeleted,
		CreatedDate,
		ModifiedDate
	FROM 
	(
		MERGE INTO [dim].[Reason] AS Target1 
		USING 
		(
			SELECT 
				S.ReasonID   AS ReasonID,
				S.ReasonHierarchy AS ReasonHierarchy,
				S.ReasonName AS ReasonName,
				(
					SELECT 
						D.ReasonKey 
					FROM 
						[dim].[Reason] AS D 
					WHERE 
						D.ReasonID = S.ReasonParentID AND
						D.IsCurrent     = 1
				) AS ReasonParentKey,
				S.OeeCategory AS OeeCategory,
				S.Level1Name  AS Level1Name,
				S.Level2Name  AS Level2Name,
				S.Level3Name  AS Level3Name,
				S.Level4Name  AS Level4Name,
				S.Level5Name  AS Level5Name,
				S.Level6Name  AS Level6Name,
				S.Level7Name  AS Level7Name,
				S.Level8Name  AS Level8Name,
				S.Level9Name  AS Level9Name,
				S.Level10Name AS Level10Name,
				S.Level
			FROM 
				#Temp_Stage_Reason AS S
		) AS Source1 

		ON (Target1.ReasonID = Source1.ReasonID) 

		WHEN MATCHED AND 
			(EffectiveEndTime = '9999-12-31') AND (IsCurrent = 1) AND 
			(Target1.ReasonHierarchy != Source1.ReasonHierarchy) AND 
			(Target1.ReasonName != Source1.ReasonName) AND
			(Target1.ReasonParentKey != Source1.ReasonParentKey) AND
			(Target1.OeeCategory != Source1.OeeCategory) AND
			(Target1.Level1Name != Source1.Level1Name) AND
			(Target1.Level2Name != Source1.Level2Name) AND
			(Target1.Level3Name != Source1.Level3Name) AND
			(Target1.Level4Name != Source1.Level4Name) AND
			(Target1.Level5Name != Source1.Level5Name) AND
			(Target1.Level6Name != Source1.Level6Name) AND
			(Target1.Level7Name != Source1.Level7Name) AND
			(Target1.Level8Name != Source1.Level8Name) AND
			(Target1.Level9Name != Source1.Level9Name) AND
			(Target1.Level10Name != Source1.Level10Name) AND
			(Target1.Level != Source1.Level)
		  THEN 
			UPDATE SET Target1.EffectiveEndTime = GetDate(), IsCurrent = 0, ModifiedDate = GETDATE()      

		  WHEN NOT MATCHED BY TARGET THEN 
			  INSERT 
			  (
				ReasonID,
				ReasonHierarchy,
				ReasonName,
				ReasonParentKey,
				OeeCategory,
				Level1Name,
				Level2Name,
				Level3Name,
				Level4Name,
				Level5Name,
				Level6Name,
				Level7Name,
				Level8Name,
				Level9Name,
				Level10Name,
				Level,
				IsCurrent,
				EffectiveStartTime,
				EffectiveEndTime,
				IsDeleted,
				CreatedDate,
				ModifiedDate
			  ) 
			  VALUES 
			  (
				Source1.ReasonID,
				Source1.ReasonHierarchy,
				Source1.ReasonName,
				NULL,
				Source1.OeeCategory,
				Source1.Level1Name,
				Source1.Level2Name,
				Source1.Level3Name,
				Source1.Level4Name,
				Source1.Level5Name,
				Source1.Level6Name,
				Source1.Level7Name,
				Source1.Level8Name,
				Source1.Level9Name,
				Source1.Level10Name,
				Source1.Level,
				1,
				'1963-01-01',
				'9999-12-31',
				0,
				GETDATE(),
				NULL
			  ) 

		  WHEN NOT MATCHED BY SOURCE THEN 
			UPDATE SET Target1.IsDeleted = 1    

		  OUTPUT 
			$action AS action,
			Source1.ReasonID,
			Source1.ReasonHierarchy,
			Source1.ReasonName,
			Source1.ReasonParentKey,
			Source1.OeeCategory,
			Source1.Level1Name,
			Source1.Level2Name,
			Source1.Level3Name,
			Source1.Level4Name,
			Source1.Level5Name,
			Source1.Level6Name,
			Source1.Level7Name,
			Source1.Level8Name,
			Source1.Level9Name,
			Source1.Level10Name,
			Source1.Level,
			1 AS IsCurrent,
			GETDATE() AS EffectiveStartTime,
			'9999-12-31' AS EffectiveEndTime,
			0 AS IsDeleted,
			GETDATE() AS CreatedDate,
			NULL AS ModifiedDate

	) AS CHANGES     

	WHERE action='UPDATE' AND ReasonID IS NOT NULL;

	UPDATE D SET
		D.ReasonParentKey = P.ReasonKey
	FROM 
		[dim].[Reason] AS D
	LEFT OUTER JOIN
		#Temp_Stage_Reason AS S
	ON 
		D.ReasonID = S.ReasonID
	LEFT OUTER JOIN 
		[dim].[Reason] AS P
	ON 
		S.ReasonParentID = P.ReasonID 
	WHERE
		D.IsCurrent = 1 AND
		P.IsCurrent = 1 AND
		D.ReasonParentKey IS NULL; 

	DROP TABLE #Temp_Reason_Parent;
	DROP TABLE #Temp_Stage_Reason;
	
END


GO


