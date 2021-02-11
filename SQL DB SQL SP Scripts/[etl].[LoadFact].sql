/****** Object:  StoredProcedure [etl].[LoadFact]    Script Date: 9/16/2020 3:34:22 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/********************************************************************************************************************************/
--Object Name : [etl].[LoadFact]
--Purpose	  : Generic Procedure to load data into fact table
--Date Creaed : 8/11/2020
--Input Parameters :
--					SchemaName-The schema name where the fact table is present
--					TableName-fact table name
--					StartDateFieldName-This is the name of the date field present in the fact table. 
--									   This is applicable only for fact that needs versioning of data
--					EndDateFieldName-This is the name of the date field present in the fact table. 
--									 This is applicable only for fact that needs versioning of data
--					VersioningFieldName-This is the name of the field present in the fact table based on which versioning is done. 
--										This is applicable only for fact that needs versioning of data
--					IsVersioningRequired-This field indicates if the fact table needs to maintain version. 
--										 Accepted values: 0, 1. Default value: 0
--Created By : DSAMRAJ
/********************************************************************************************************************************/
  
  CREATE PROCEDURE [etl].[LoadFact] 
	@SchemaName NVARCHAR(100),    
	@TableName NVARCHAR(100),
	@IsVersioned INT = 0,
	@VersionEndDate NVARCHAR(100) = '',    
	@VersionStartDate NVARCHAR(100) = '',    
	@VersionFieldName NVARCHAR(100) = ''

  AS     
  BEGIN      
  
	SET NOCOUNT ON      
  
  --Variable Declaration
	DECLARE @columnlist NVARCHAR(MAX)
	,@Mergeon NVARCHAR(MAX)
	,@MergeInsert NVARCHAR(MAX)
	,@MergeOutput NVARCHAR(MAX)
	,@MergeDeleteChk NVARCHAR(MAX)
	,@VersionMergeSql NVARCHAR(MAX)
	,@FactMergeSql NVARCHAR(MAX)
	,@SrcSelect NVARCHAR(MAX)
	,@SrcLocationSelect NVARCHAR(MAX)
	,@SrcShiftSelect NVARCHAR(MAX)
	,@SrcLookup NVARCHAR(MAX)
	,@SrcLocationLookup NVARCHAR(MAX)
	,@SrcCrewShiftLookup NVARCHAR(MAX)
	,@SrcCrewLookup NVARCHAR(MAX)
	,@SrcShiftLookup NVARCHAR(MAX)
	,@SrcSelectSql NVARCHAR(MAX)
	,@TruncateStgTableSql NVARCHAR(MAX)
	,@LocationLevel VARCHAR(200)

	
	--Dynamic Truncate table statement. This is to clear the stage table after processing
	SET @TruncateStgTableSql = 'TRUNCATE TABLE stage.'+@TableName+';'

	--Truncate stage table
	--EXEC sp_executesql @TruncateStgTableSql

	--Temp table to store metadata for building dynamic MERGE statement
	IF OBJECT_ID('tempdb..#tmpMerge') IS NOT NULL    
	DROP TABLE #tmpMerge     
	
	CREATE TABLE #tmpMerge (    
		DatabaseName NVARCHAR(100)    
		,SchemaName NVARCHAR(500)    
		,TableName NVARCHAR(500)    
		,ColumnName NVARCHAR(500)    
		,MergeOn NVARCHAR(1000)    
		,MergeChk NVARCHAR(1000)    
		,UpdateOn NVARCHAR(1000)    
		,InsertOn NVARCHAR(1000)    
		,IsPK BIT DEFAULT 0
	)        
	
	--Retrieve the input table structure and load into temp table
	INSERT INTO #tmpMerge 
		(DatabaseName,SchemaName,TableName,ColumnName,MergeOn,UpdateOn,InsertOn)   
	SELECT     
		Table_Catalog AS DatabaseName     
		,Table_Schema AS SchemaName     
		,Table_Name AS TableName     
		,Column_Name AS ColumnName     
		,'Target1.' + Column_Name + ' = Source1.' + Column_Name as MergeOn     
		,'Target1.' + Column_Name + ' = Source1.' + Column_Name as UpdateOn     
		,'Source1.' + Column_Name as InsertOn    
	FROM Information_Schema.Columns    
	WHERE Table_Schema = @SchemaName     
		  AND Table_Name = @TableName             
	
	--Update input table primary key information in temp table
	UPDATE dt SET IsPK = 1 
	FROM #tmpMerge dt    
	INNER JOIN Information_Schema.Key_Column_Usage ku ON dt.ColumnName =ku.Column_Name    
	INNER JOIN Information_Schema.Table_Constraints tc ON tc.Constraint_Type ='PRIMARY KEY'  AND tc.Constraint_Name = ku.Constraint_Name AND ku.Table_Name = dt.TableName;  

	SET @LocationLevel = (SELECT LocationLevel FROM etl.FactLookupDetails WHERE FactTableName = @TableName AND LookupTableName = 'Location')
	
	--Build dynamic Source query to be used in MERGE statement - Step 1 - build select clause for source query
	SET @SrcSelect = (Select STRING_AGG((CASE WHEN COLUMN_NAME='-1' THEN '-1' + ' AS ' + FactColumnName ELSE 'COALESCE(' + COLUMN_NAME + ' ,-1) AS ' + FactColumnName END),',') + ',' as SelectClause
						FROM
						(	SELECT distinct 
							(case when dt.TABLE_NAME is null then '-1' else ld.LookupTableAliasName+'.'+ku.COLUMN_NAME end) as COLUMN_NAME,ld.FactColumnName
							FROM etl.FactLookupDetails ld
							LEFT JOIN INFORMATION_SCHEMA.Columns dt ON (ld.LookupSchemaName = dt.TABLE_SCHEMA And ld.LookupTableName = dt.Table_name)
							LEFT JOIN Information_Schema.Key_Column_Usage ku ON (dt.Table_Schema = ku.Table_Schema AND dt.Table_Name = ku.Table_Name AND dt.Column_Name =ku.Column_Name)
							LEFT JOIN Information_Schema.Table_Constraints tc ON tc.Constraint_Type ='PRIMARY KEY'  AND ku.Constraint_Name = tc.Constraint_Name AND ku.Table_Name = tc.Table_Name AND ku.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA 
							WHERE ld.FactTableName = @TableName AND ld.LookupTableName NOT IN ('Location','Shift','Crew')
						) a
						WHERE COLUMN_NAME IS NOT NULL
					 )

	SET @SrcLocationSelect = (SELECT  
								(CASE WHEN @LocationLevel = 'Element' THEN 'COALESCE(lkploc.LocationKey,-1) AS ' + FactColumnName ELSE 'COALESCE(' + LookupTableAliasName + '.' + FactColumnName + ',-1) AS ' + FactColumnName END) + ', ' as LocationSelectClause
								FROM etl.FactLookupDetails ld
								WHERE ld.FactTableName = @TableName AND ld.LookupTableName = 'Location')

	SET @SrcShiftSelect = (SELECT  
							'COALESCE((CASE WHEN src.'+StgColumnName+' BETWEEN lkpdaycrewshift.ShiftStartTime AND lkpdaycrewshift.ShiftEndTime THEN lkpdaycrewshift.ShiftEndDateKey ELSE lkpnightcrewshift.ShiftEndDateKey END), ' + CHAR(13) +
							 '(CASE WHEN src.'+StgColumnName+' BETWEEN lkpdayshift.ShiftStartTime AND lkpdayshift.ShiftEndTime THEN ' + LookupTableAliasName + '.DateKey ELSE ' + LookupTableAliasName + '.NextDateKey END), ' + CHAR(13) +
							 '-1)  as ShiftEndDateKey, '+CHAR(13) +
							'COALESCE((CASE WHEN src.'+StgColumnName+' BETWEEN lkpdayshift.ShiftStartTime AND lkpdayshift.ShiftEndTime THEN lkpdayshift.ShiftKey ELSE lkpnightshift.ShiftKey END), ' + CHAR(13) +
							'-1) as ShiftTimeKey, '+CHAR(13) +
							'COALESCE((CASE WHEN src.'+StgColumnName+' BETWEEN lkpdaycrewshift.ShiftStartTime AND lkpdaycrewshift.ShiftEndTime THEN lkpdaycrew.CrewKey ELSE lkpnightcrew.CrewKey END),-1) as CrewKey, '+CHAR(13)
							FROM etl.FactLookupDetails ld
							WHERE ld.FactTableName = @TableName AND ld.LookupTableName = 'CrewShiftMapping' AND ld.StgColumnName LIKE '%Time%'
						  )

	--Build dynamic Source query to be used in MERGE statement - Step 2 - build from clause for source query with joins. The lookup table information is stored in LookupDetails table
	SET @SrcLookup = (SELECT STRING_AGG(JoinTbl+'('+SUBSTRING(JoinCondition,5,Len(JoinCondition))+SCDCondition,CHAR(13))
					  FROM
					  (
					  	SELECT DISTINCT 
					  		 (' LEFT JOIN ' + 
							   (CASE WHEN LookupTableName = 'Date' THEN '(SELECT lead(Date,1) OVER(ORDER BY Date) AS NextDate,lead(DateKey,1) OVER(ORDER BY Date) AS NextDateKey,* from [dim].[Date])'
									 ELSE LookupSchemaName + '.' + LookupTableName END) + ' ' + LookupTableAliasName + ' ON ') AS JoinTbl,
							 (SELECT STRING_AGG(' AND  ' + 'src.'+ StgColumnName+ '= ' + LookupTableAliasName + '.' + LookupColumnName,CHAR(13))
					  			FROM etl.FactLookupDetails JC
					  			WHERE JC.StgTableName = LD.StgTableName
					  			AND JC.LookupTableName=LD.LookupTableName 
								AND JC.LookupTableAliasName=LD.LookupTableAliasName) as JoinCondition,
							 (CASE WHEN LookupIsSCD = 'Yes' 
							  THEN ' AND src.' + SCDDateField + ' BETWEEN ' + LookupTableAliasName + '.EffectiveStartTime AND ' + LookupTableAliasName + '.EffectiveEndTime) '
							  ELSE ') ' END) AS SCDCondition,
							  dt.TABLE_NAME
					  	FROM etl.FactLookupDetails LD
						LEFT JOIN INFORMATION_SCHEMA.Columns dt ON (LD.LookupSchemaName = dt.TABLE_SCHEMA And LD.LookupTableName = dt.Table_name AND LD.LookupColumnName = dt.Column_Name)
					  	WHERE LD.FactTableName = @TableName AND LD.LookupTableName NOT IN ('Location','CrewShiftMapping','Shift','Crew')
					  ) a
					  WHERE TABLE_NAME is not null
					)

	
	SET @SrcLocationLookup = (SELECT 
								(CASE WHEN LocationLevel = 'Plant' AND LookupIsSCD = 'Yes' THEN 'LEFT JOIN ' + LookupSchemaName + '.' + LookupTableName + ' ' + LookupTableAliasName + ' ON ( src.' + StgColumnName + ' = ' + LookupTableAliasName + '.' + LookupColumnName + ' AND src.' + SCDDateField + ' BETWEEN ' + LookupTableAliasName + '.EffectiveStartTime AND ' + LookupTableAliasName + '.EffectiveEndTime )' + CHAR(13) 
									  WHEN LocationLevel = 'Plant' AND LookupIsSCD = 'No' THEN 'LEFT JOIN ' + LookupSchemaName + '.' + LookupTableName + ' ' + LookupTableAliasName + ' ON ( src.' + StgColumnName + ' = ' + LookupTableAliasName + '.' + LookupColumnName + ' )' + CHAR(13)
									  WHEN LocationLevel = 'Element' AND LookupIsSCD = 'Yes' THEN  'LEFT JOIN ' + LookupSchemaName + '.' + LookupTableName + ' lkploc ON (src.' + StgColumnName + ' = lkploc.' + LookupColumnName + ' AND src.' + SCDDateField + ' BETWEEN lkploc.EffectiveStartTime AND lkploc.EffectiveEndTime ) '+ CHAR(13) + ' LEFT JOIN ' + LookupSchemaName + '.' + LookupTableName + ' lkpplant ON (lkploc.Plant = lkpplant.Name )' + CHAR(13)
									  WHEN LocationLevel = 'Element' AND LookupIsSCD = 'No' THEN  'LEFT JOIN ' + LookupSchemaName + '.' + LookupTableName + ' lkploc ON (src.' + StgColumnName + ' = lkploc.' + LookupColumnName + ' )' + CHAR(13) + ' LEFT JOIN ' + LookupSchemaName + '.' + LookupTableName + ' lkpplant ON (lkploc.Plant = lkpplant.Name )' + CHAR(13)
								ELSE '' END)
							 FROM etl.FactLookupDetails LD
							 WHERE LD.FactTableName = @TableName AND LD.LookupTableName ='Location')

	

	SET @SrcCrewShiftLookup = (SELECT STRING_AGG(crewshiftjoin, ' ') 
								FROM
								(SELECT   
									('LEFT JOIN ' + a.LookupSchemaName + '.' + a.LookupTableName + ' ' + a.LookupTableAliasName + ' ON ' + 
									'(' + STUFF(STRING_AGG('AND ' + (CASE WHEN @LocationLevel = 'Element' AND a.StgColName NOT LIKE '%date%' THEN 'lkpplant.' ELSE 'src.' END) + a.StgColName + ' = ' + a.LookupTableAliasName + '.' + b.LookupColName + ' ' + CHAR(13) ,' ' ),1,4,'') +  
									(CASE WHEN a.LookupTableAliasName = 'lkpdaycrewshift' THEN ' AND ' + a.LookupTableAliasName + '.ShiftOrder = 1)' ELSE ' AND ' + a.LookupTableAliasName + '.ShiftOrder = 2)' END) + CHAR(13) ) AS crewshiftjoin
									FROM
									(SELECT row_number() OVER(ORDER BY (select null)) as ID, LookupTableName, LookupSchemaName, LookupTableAliasName, FactColumnName, Value as StgColName FROM etl.FactLookupDetails CROSS APPLY STRING_SPLIT(StgColumnName,',') WHERE FactTableName = @TableName AND LookupTableName ='CrewShiftMapping' AND StgColumnName NOT LIKE '%Time%') a
									INNER JOIN 
									(SELECT row_number() OVER(ORDER BY (select null)) as ID,StgTableName, StgSchemaName, StgTableAliasName, Value as LookupColName FROM etl.FactLookupDetails CROSS APPLY STRING_SPLIT(LookupColumnName,',') WHERE FactTableName = @TableName AND LookupTableName ='CrewShiftMapping' AND StgColumnName NOT LIKE '%Time%') b
											ON a.ID = b.ID
									GROUP BY a.LookupSchemaName, a.LookupTableName, a.LookupTableAliasName
								) a)

	SET @SrcCrewLookup = (SELECT STRING_AGG(CrewJoin,' ')
							FROM (SELECT 
							('LEFT JOIN ' + b.LookupSchemaName + '.' + b.LookupTableName + ' ' + b.LookupTableAliasName + ' ON ' + 
							'(' + STUFF(STRING_AGG('AND ' + a.StgTableAliasName + '.' + a.StgColName + ' = ' + b.LookupTableAliasName + '.' + b.LookupColName + ' ' + CHAR(13) ,' ' ),1,4,'') + ' AND ' + b.LookupTableAliasName + '.IsCurrent = 1)' + CHAR(13)) AS CrewJoin
							FROM
							(SELECT row_number() OVER(ORDER BY (select null)) as ID, StgTableName, StgSchemaName, StgTableAliasName, Value as StgColName FROM etl.FactLookupDetails CROSS APPLY STRING_SPLIT(StgColumnName,',') WHERE FactTableName = @TableName AND LookupTableName ='Crew') a
							INNER JOIN 
							(SELECT row_number() OVER(ORDER BY (select null)) as ID,LookupTableName, LookupSchemaName, LookupTableAliasName, Value as LookupColName FROM etl.FactLookupDetails CROSS APPLY STRING_SPLIT(LookupColumnName,',') WHERE FactTableName = @TableName AND LookupTableName ='Crew') b
									ON a.ID = b.ID
							GROUP BY b.LookupSchemaName, b.LookupTableName, b.LookupTableAliasName
						) a)

	SET @SrcShiftLookup = (SELECT STRING_AGG(ShiftJoin,' ')
							 FROM (SELECT
								('LEFT JOIN ' + LookupSchemaName + '.' + LookupTableName + ' ' + LookupTableAliasName + ' ON (' +
								(CASE WHEN @LocationLevel = 'Element' THEN 'lkpplant.' ELSE StgTableAliasName + '.' END) + StgColumnName +
								' = ' + LookupTableAliasName + '.' + LookupColumnName + ' AND ' +
								(CASE WHEN LookupTableAliasName = 'lkpdayshift' THEN LookupTableAliasName + '.ShiftOrder = 1 AND ' + LookupTableAliasName + '.IsCurrent = 1)' ELSE LookupTableAliasName + '.ShiftOrder = 2 AND ' + LookupTableAliasName + '.IsCurrent = 1)' END) + ' ' + CHAR(13)) AS ShiftJoin
								FROM etl.FactLookupDetails  
								WHERE FactTableName = @TableName AND LookupTableName ='Shift'
							) a )



	--Build dynamic Source query to be used in MERGE statement - Step 3 - Combine select clause and from clause to build the source query
	SET @SrcSelectSql = 'SELECT DISTINCT '+@SrcSelect+CHAR(13)+' '+COALESCE(@SrcLocationSelect,'')+CHAR(13)+' '+COALESCE(@SrcShiftSelect,'')+' src.*'+CHAR(13)+' FROM stage.'+@TableName+' src '+CHAR(13)
							+@SrcLookup+CHAR(13)+' '+COALESCE(@SrcLocationLookup,'')+CHAR(13)+' '+COALESCE(@SrcCrewShiftLookup,'')+CHAR(13)+' '+COALESCE(@SrcCrewLookup,'')+CHAR(13)+' '+COALESCE(@SrcShiftLookup,'')

	
	
	--Build target table column list. This will be used in building MERGE statement
	SET @ColumnList = (SELECT STUFF((SELECT ',' + ColumnName + CHAR(10)        
					   FROM #tmpMerge x1        
					   WHERE x1.TableName = x2.TableName        
					   FOR XML PATH ('')), 1, 1,'') AS MergeOnColumns     
					   FROM #tmpMerge x2     GROUP BY TableName);            
	
	--Build the Merge join condition between source and target
	SET @MergeOn = (SELECT STUFF((SELECT 'AND ' + MergeOn + CHAR(10)        
					FROM #tmpMerge x1        
					WHERE x1.TableName = x2.TableName AND x1.IsPK = 1 AND x1.ColumnName!=@VersionStartDate        
					FOR XML PATH ('')), 1, 4,'')     
					FROM #tmpMerge x2     
					WHERE IsPK = 1 AND ColumnName!=@VersionStartDate     
					GROUP BY TableName);        
	
	--Build MERGE Insert statement for the target table
	SET @MergeInsert = (SELECT STUFF(
							(SELECT ',' + 
								(CASE WHEN InsertOn = 'Source1.StartDate' AND @IsVersioned=1 THEN 'GETDATE()'
								      WHEN InsertOn = 'Source1.EndDate' AND @IsVersioned=1 THEN 'NULL'
									  WHEN InsertOn = 'Source1.CreatedDate' THEN 'GETDATE()'
									  WHEN InsertOn = 'Source1.ModifiedDate' THEN 'GETDATE()' ELSE InsertOn END) + CHAR(10)        
							FROM #tmpMerge x1        
							WHERE x1.TableName = x2.TableName       
							FOR XML PATH ('')), 1, 1,'')      
						FROM #tmpMerge x2  
						GROUP BY TableName);         
	
	--Build MERGE Output statement
	SET @MergeOutput = '$action as action,'+
						(SELECT STUFF(
							(SELECT ',' + 
								(CASE WHEN InsertOn = 'Source1.StartDate' AND @IsVersioned=1 THEN 'GETDATE() AS StartDate'
									  WHEN InsertOn = 'Source1.EndDate' AND @IsVersioned=1 THEN 'NULL AS EndDate'
									  WHEN InsertOn = 'Source1.CreatedDate' THEN 'GETDATE() AS CreatedDate'
									  WHEN InsertOn = 'Source1.ModifiedDate' THEN 'NULL AS ModifiedDate' ELSE InsertOn END)
								+ CHAR(10)        
							 FROM #tmpMerge x1        
							 WHERE x1.TableName = x2.TableName       
							 FOR XML PATH ('')), 1, 1,'')      
						FROM #tmpMerge x2    
						GROUP BY TableName)         
	
	--Build Deletion condition to check if a row has been deleted in source
	SET @MergeDeleteChk = (SELECT STUFF((SELECT 'AND ' + ColumnName + ' IS NOT NULL '+ CHAR(10)        
							FROM #tmpMerge x1        
							WHERE x1.TableName = x2.TableName AND x1.IsPK = 1 AND x1.ColumnName!=@VersionStartDate       
							FOR XML PATH ('')), 1, 4,'')     
							FROM #tmpMerge x2     
							WHERE IsPK = 1 AND ColumnName!=@VersionStartDate     
							GROUP BY TableName)

	--Build dynamic MERGE Statement. This MERGE statement is used for loading fact tables that require versioning of data
	SET @VersionMergeSql = 
	'INSERT INTO ['+@SchemaName+'].['+@TableName+'] ('+@columnlist+') '+CHAR(10)+
	'SELECT '+@columnlist+CHAR(10)+        
	' FROM '+CHAR(10)+        
	'('+CHAR(10)+         
		'MERGE INTO ['+@SchemaName+'].['+@TableName+'] AS Target1 '+CHAR(10)+         
		'USING ( '+ CHAR(10) + @SrcSelectSql + 
		 ') AS Source1 '+CHAR(10)+         
		'ON ('+@MergeOn+') '+ CHAR(10)+           
		
		'WHEN MATCHED AND (Target1.' + @VersionEndDate +' Is Null) AND (IsNull(Target1.'+@VersionFieldName+',-1) != IsNull(Source1.'+@VersionFieldName+',-1))'+CHAR(10)+         
		'THEN '+CHAR(10)+         
		'UPDATE SET Target1.'+@VersionEndDate+' = GetDate(), Target1.ModifiedDate = GetDate() '+CHAR(10)+                  
		
		'WHEN NOT MATCHED BY Target AND Source1.'+@VersionFieldName+' is not null '+CHAR(10)+         
		'THEN '+CHAR(10)+         
		'INSERT ('+@columnlist+') '+CHAR(10)+         
		'VALUES ('+@MergeInsert+') '+CHAR(10)+                  

		'WHEN NOT MATCHED BY Source'+CHAR(10)+         
		'THEN '+CHAR(10)+         
		'UPDATE SET Target1.'+@VersionEndDate+' = GetDate(), Target1.ModifiedDate = GetDate() '+CHAR(10)+      
		
		'OUTPUT '+@MergeOutput+        ') as Changes 
		WHERE action=''UPDATE'' AND '+ @MergeDeleteChk +';'        

	--Build dynamic MERGE Statement. This MERGE statement is used for loading fact tables that do not require versioning of data
	SET @FactMergeSql = 
		'MERGE INTO ['+@SchemaName+'].['+@TableName+'] AS Target1 '+CHAR(10)+         
		'USING ( '+ CHAR(10) + @SrcSelectSql + 
		 ') AS Source1 '+CHAR(10)+         
		'ON ('+@MergeOn+') '+ CHAR(10)+           
		
		'WHEN NOT MATCHED BY Target '+CHAR(10)+         
		'THEN '+CHAR(10)+         
		'INSERT ('+@columnlist+') '+CHAR(10)+         
		'VALUES ('+@MergeInsert+') '+CHAR(10)+';'   

	--Check if Versioning required and execute the appropriate MERGE statement 
	IF @IsVersioned=1
		EXEC sp_executesql @VersionMergeSql
	ELSE
		EXEC sp_executesql @FactMergeSql

	--Truncate stage table
	--EXEC sp_executesql @TruncateStgTableSql

END
GO


