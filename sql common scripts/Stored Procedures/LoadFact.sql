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
	@StartDateFieldName NVARCHAR(100) = '',    
	@EndDateFieldName NVARCHAR(100) = '',    
	@VersioningFieldName NVARCHAR(100) = '',
	@IsVersioningRequired INT = 0

  AS     
  BEGIN      
  
  SET NOCOUNT ON      
  
  --Variable Declaration
	DECLARE @columnlist NVARCHAR(MAX)
	,@Mergeon NVARCHAR(MAX)
	,@MergeInsert NVARCHAR(MAX)
	,@MergeOutput NVARCHAR(MAX)
	,@MergeDeleteChk NVARCHAR(MAX)
	,@VersioningMergeSql NVARCHAR(MAX)
	,@FactMergeSql NVARCHAR(MAX)
	,@SrcSelect NVARCHAR(MAX)
	,@SrcLookup NVARCHAR(MAX)
	,@SrcSelectSql NVARCHAR(MAX)
	,@TruncateStgTableSql NVARCHAR(MAX)

	
	--Dynamic Truncate table statement. This is to clear the stage table after processing
	SET @TruncateStgTableSql = 'TRUNCATE TABLE stage.'+@TableName+';'

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

	--Build dynamic Source query to be used in MERGE statement - Step 1 - build select clause for source query
	SET @SrcSelect = (Select STRING_AGG(TABLE_SCHEMA+'.'+TABLE_NAME+'.'+COLUMN_NAME,',')+',src.*' as SelectClause
						FROM
						(	SELECT distinct ku.TABLE_SCHEMA,ku.TABLE_NAME,ku.COLUMN_NAME 
							FROM config.LookupDetails ld
							INNER JOIN INFORMATION_SCHEMA.Columns dt ON (ld.LookupDatabaseName = dt.Table_Catalog AND ld.LookupSchemaName = dt.TABLE_SCHEMA And ld.LookupTableName = dt.Table_name)
							INNER JOIN Information_Schema.Key_Column_Usage ku ON dt.Column_Name =ku.Column_Name    
							INNER JOIN Information_Schema.Table_Constraints tc ON tc.Constraint_Type ='PRIMARY KEY'  AND tc.Constraint_Name = ku.Constraint_Name AND ku.Table_Name = dt.Table_Name
							WHERE ld.StgTableName = @TableName
						) a
					 )

	--Build dynamic Source query to be used in MERGE statement - Step 2 - build from clause for source query with joins. The lookup table information is stored in LookupDetails table
	SET @SrcLookup = (SELECT STRING_AGG(JoinTbl+'('+SUBSTRING(JoinCondition,5,Len(JoinCondition)),CHAR(13))
					  FROM
					  (
					  	SELECT DISTINCT 
					  		(' LEFT JOIN ' + LookupSchemaName + '.' + LookupTableName + ' ON ') AS JoinTbl,
					  		(
					  			SELECT STRING_AGG(' AND  ' + 'src.'+ StgColumnName+ '= ' + LookupSchemaName + '.' + LookupTableName + '.' + LookupColumnName,CHAR(13))
					  			FROM config.LookupDetails JC
					  			WHERE JC.StgTableName = LD.StgTableName
					  			AND JC.LookupTableName=LD.LookupTableName
					          )+') ' AS JoinCondition
					  	FROM config.LookupDetails LD
					  	WHERE LD.StgTableName = @TableName
					  ) a
					)
	--Build dynamic Source query to be used in MERGE statement - Step 3 - Combine select clause and from clause to build the source query
	SET @SrcSelectSql = 'SELECT '+@SrcSelect+' FROM stage.'+@TableName+' src '+CHAR(13)+@SrcLookup	

	
	--Build target table column list. This will be used in building MERGE statement
	SET @ColumnList = (SELECT STUFF((SELECT ',' + ColumnName + CHAR(10)        
					   FROM #tmpMerge x1        
					   WHERE x1.TableName = x2.TableName        
					   FOR XML PATH ('')), 1, 1,'') AS MergeOnColumns     
					   FROM #tmpMerge x2     GROUP BY TableName);            
	
	--Build the Merge join condition between source and target
	SET @MergeOn = (SELECT STUFF((SELECT 'AND ' + MergeOn + CHAR(10)        
					FROM #tmpMerge x1        
					WHERE x1.TableName = x2.TableName AND x1.IsPK = 1 AND x1.ColumnName!=@StartDateFieldName        
					FOR XML PATH ('')), 1, 4,'')     
					FROM #tmpMerge x2     
					WHERE IsPK = 1 AND ColumnName!=@StartDateFieldName     
					GROUP BY TableName);        
	
	--Build MERGE Insert statement for the target table
	SET @MergeInsert = (SELECT STUFF(
							(SELECT ',' + 
								(CASE WHEN InsertOn = 'Source1.StartDate' AND @IsVersioningRequired=1 THEN 'GETDATE()'
								      WHEN InsertOn = 'Source1.EndDate' AND @IsVersioningRequired=1 THEN 'NULL'
									  WHEN InsertOn = 'Source1.CreatedDate' THEN 'GETDATE()'
									  WHEN InsertOn = 'Source1.ModifiedDate' THEN 'NULL' ELSE InsertOn END) + CHAR(10)        
							FROM #tmpMerge x1        
							WHERE x1.TableName = x2.TableName       
							FOR XML PATH ('')), 1, 1,'')      
						FROM #tmpMerge x2  
						GROUP BY TableName);         
	
	--Build MERGE Output statement
	SET @MergeOutput = '$action as action,'+
						(SELECT STUFF(
							(SELECT ',' + 
								(CASE WHEN InsertOn = 'Source1.StartDate' AND @IsVersioningRequired=1 THEN 'GETDATE() AS StartDate'
									  WHEN InsertOn = 'Source1.EndDate' AND @IsVersioningRequired=1 THEN 'NULL AS EndDate'
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
							WHERE x1.TableName = x2.TableName AND x1.IsPK = 1 AND x1.ColumnName!=@StartDateFieldName       
							FOR XML PATH ('')), 1, 4,'')     
							FROM #tmpMerge x2     
							WHERE IsPK = 1 AND ColumnName!=@StartDateFieldName     
							GROUP BY TableName)

	--Build dynamic MERGE Statement. This MERGE statement is used for loading fact tables that require versioning of data
	SET @VersioningMergeSql = 
	'INSERT INTO ['+@SchemaName+'].['+@TableName+'] ('+@columnlist+') '+CHAR(10)+
	'SELECT '+@columnlist+CHAR(10)+        
	' FROM '+CHAR(10)+        
	'('+CHAR(10)+         
		'MERGE INTO ['+@SchemaName+'].['+@TableName+'] AS Target1 '+CHAR(10)+         
		'USING ( '+ CHAR(10) + @SrcSelectSql + 
		 ') AS Source1 '+CHAR(10)+         
		'ON ('+@MergeOn+') '+ CHAR(10)+           
		
		'WHEN MATCHED AND (Target1.' + @EndDateFieldName +' Is Null) AND (IsNull(Target1.'+@VersioningFieldName+',-1) != IsNull(Source1.'+@VersioningFieldName+',-1))'+CHAR(10)+         
		'THEN '+CHAR(10)+         
		'UPDATE SET Target1.'+@EndDateFieldName+' = GetDate(), Target1.ModifiedDate = GetDate() '+CHAR(10)+                  
		
		'WHEN NOT MATCHED BY Target AND Source1.'+@VersioningFieldName+' is not null '+CHAR(10)+         
		'THEN '+CHAR(10)+         
		'INSERT ('+@columnlist+') '+CHAR(10)+         
		'VALUES ('+@MergeInsert+') '+CHAR(10)+                  

		'WHEN NOT MATCHED BY Source'+CHAR(10)+         
		'THEN '+CHAR(10)+         
		'UPDATE SET Target1.'+@EndDateFieldName+' = GetDate(), Target1.ModifiedDate = GetDate() '+CHAR(10)+      
		
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
	IF @IsVersioningRequired=1
		EXEC sp_executesql @VersioningMergeSql
	ELSE
		EXEC sp_executesql @FactMergeSql

	EXEC sp_executesql @TruncateStgTableSql

  END  
GO