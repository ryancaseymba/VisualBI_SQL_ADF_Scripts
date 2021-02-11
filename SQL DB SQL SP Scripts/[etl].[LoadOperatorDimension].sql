/****** Object:  StoredProcedure [etl].[LoadOperatorDimension]    Script Date: 9/16/2020 3:34:42 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/********************************************************************************************************************************/
--Object Name : [etl].[LoadOperatorDimension]
--Purpose	  : Procedure to load data into dim.Operator table
--Date Creaed : 9/11/2020
--Input Parameters : None
--Created By : PADMA
/********************************************************************************************************************************/

CREATE PROCEDURE  [etl].[LoadOperatorDimension]
AS
BEGIN

	IF OBJECT_ID('tempdb..#Operator') IS NOT NULL
		DROP TABLE #Operator

	SELECT 
		PersonUID
		,PersonID
		,OperatorID
		,OISOperatorAlias
		,FirstName
		,LastName
		,DisplayName
		,Department
		,Title
		,Supervisor
		,Status INTO #Operator
	FROM (
		SELECT 
			PersonUID
			,PersonID
			,OperatorID
			,NULL AS OISOperatorAlias
			,dms.FirstName
			,dms.LastName
			,hana.DisplayName
			,hana.Department
			,hana.Title
			,hana.Supervisor		
			,RANK() OVER(PARTITION BY OperatorID 
						ORDER BY CASE WHEN IsActive=1 THEN 'ACTIVE' 
									ELSE 'INACTIVE' END ASC,DateAdded,PersonUID DESC) StatusRank
			,CASE WHEN IsActive=1 THEN 'ACTIVE' 
										ELSE 'INACTIVE' END Status
		FROM stage.Operator dms
		LEFT JOIN stage.maximoperson hana
			ON dms.firstname=hana.firstname
			AND dms.lastname=hana.lastname
		)a
	WHERE a.StatusRank=1

	UNION
	SELECT 
		PersonUID,
		ois.PersonID,
		Null AS OperatorID,
		OISOperatorAlias,
		hana.FirstName,
		hana.Lastname,
		hana.DisplayName,
		hana.Department,
		hana.Title,
		hana.Supervisor,
		COALESCE(hana.Status,'ACTIVE') AS Status
	FROM 
		(SELECT 
			RIGHT(Operator,LEN(Operator)-charindex('\',Operator)) AS PersonID
			,Operator AS OISOperatorAlias 
		FROM [stage].[OISOperator]) ois
	LEFT JOIN [stage].[MaximoPerson] hana
		ON ois.PersonID=hana.PersonID

	

	SET NOCOUNT ON
	MERGE INTO [dim].[Operator] AS tgt
	USING  #Operator src
		ON  (tgt.[FirstName] = src.[FirstName] and tgt.[LastName] = src.[LastName])
	WHEN MATCHED AND (
		tgt.PersonUID != src.PersonUID or
		tgt.PersonID != src.PersonID or 
		tgt.OperatorID != src.OperatorID or
		tgt.OISOperatorAlias != src.OISOperatorAlias or
		tgt.DisplayName != src.DisplayName or
		tgt.Department != src.Department or
		tgt.Title != src.Title or
		tgt.Supervisor != src.Supervisor or
		tgt.Status != src.Status
	)
	THEN UPDATE
		SET tgt.PersonUID = src.PersonUID
		,tgt.PersonID = src.PersonID
		,tgt.OperatorID = src.OperatorID
		,tgt.OISOperatorAlias = src.OISOperatorAlias
		,tgt.DisplayName = src.DisplayName
		,tgt.Department = src.Department
		,tgt.Title = src.Title
		,tgt.Supervisor = src.Supervisor
		,tgt.Status = src.Status
		,tgt.ModifiedDate = GETDATE()
	WHEN NOT MATCHED BY TARGET
	THEN INSERT
		(PersonUID
		 ,PersonID
		 ,OperatorID
		,OISOperatorAlias
		,DisplayName
		,FirstName
		,LastName
		,Department
		,Title
		,Supervisor
		,Status
		,CreatedDate
		,ModifiedDate)
	VALUES
		(src.PersonUID
		,src.PersonID
		,src.OperatorID
		,src.OISOperatorAlias
		,src.DisplayName
		,src.FirstName
		,src.LastName
		,src.Department
		,src.Title
		,src.Supervisor
		,src.Status
		,GETDATE()
		,GETDATE());


END
GO


