/****** Object:  StoredProcedure [etl].[LoadDimension]    Script Date: 9/16/2020 3:33:48 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/********************************************************************************************************************************/
--Object Name : [etl].[LoadDimension]
--Purpose	  : Generic Procedure to load dimension tables
--Date Creaed : 9/1/2020
--Input Parameters :
--					SchemaName-The schema name where the dimension table is present
--					TableName-dimension table name
--Created By : DSAMRAJ
/********************************************************************************************************************************/

CREATE PROCEDURE [etl].[LoadDimension]
	@SchemaName VARCHAR(100),    
	@TableName VARCHAR(255)

 AS     
 BEGIN      
 
	SET NOCOUNT ON 

	
  --Variable Declaration
	DECLARE @columnlist NVARCHAR(MAX)
	,@Mergeon NVARCHAR(MAX)
	,@Updateon NVARCHAR(MAX)
	,@MergeInsert NVARCHAR(MAX)
	,@MergeOutput NVARCHAR(MAX)
	,@MergeDeleteChk NVARCHAR(MAX)
	,@DimMergeSql NVARCHAR(MAX)
	,@DimSCD2MergeSql NVARCHAR(MAX)
	,@SrcSelect NVARCHAR(MAX)
	,@SrcLookup NVARCHAR(MAX)
	,@SrcSelectSql NVARCHAR(MAX)
	,@SrcSchemaName NVARCHAR(100)
	,@SrcTableName NVARCHAR(255)
	,@IsDimSCD NVARCHAR(20)
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
		,IsNaturalKey BIT DEFAULT 0
		,IsSurrogateKey BIT DEFAULT 0
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
	UPDATE dt SET IsSurrogateKey = 1 
	FROM #tmpMerge dt    
	INNER JOIN Information_Schema.Key_Column_Usage ku ON dt.SchemaName = ku.Table_Schema AND dt.TableName = ku.Table_Name AND dt.ColumnName =ku.Column_Name    
	INNER JOIN Information_Schema.Table_Constraints tc ON tc.Constraint_Type ='PRIMARY KEY'  AND tc.Constraint_Name = ku.Constraint_Name AND tc.Table_Name = ku.Table_Name;  
	
	--Update natural key information in temp table
	Update dt SET IsNaturalKey = 1
	FROM #tmpMerge dt
	JOIN (SELECT distinct DimTableName,value AS NaturalKeys FROM etl.DimensionLookupDetails CROSS APPLY STRING_SPLIT(DimNaturalKeys,',') WHERE DimTableName = @TableName And DimSchemaName = @SchemaName) as NaturalKeyInfo
		ON (dt.TableName = NaturalKeyInfo.DimTableName and dt.ColumnName = NaturalKeyInfo.NaturalKeys)


	--Build dynamic Source query to be used in MERGE statement - Step 1 - build select clause for source query
	SET @SrcSelect = (Select STRING_AGG((CASE WHEN COLUMN_NAME='-1' THEN '-1'+' AS '+DimColumnName ELSE '(CASE WHEN '+ 
										 COLUMN_NAME +
										 ' IS NULL THEN -1 ELSE '+
										 COLUMN_NAME+
										 ' END) AS '+DimColumnName END),',')+',src.*' as SelectClause
						FROM
						(	SELECT distinct 
							(case when dt.TABLE_NAME is null then '-1' else ld.LookupTableAliasName+'.'+ku.COLUMN_NAME end) as COLUMN_NAME,ld.DimColumnName
							FROM etl.DimensionLookupDetails ld
							LEFT JOIN INFORMATION_SCHEMA.Columns dt ON (ld.LookupSchemaName = dt.TABLE_SCHEMA And ld.LookupTableName = dt.Table_name)
							LEFT JOIN Information_Schema.Key_Column_Usage ku ON dt.Column_Name =ku.Column_Name    
							LEFT JOIN Information_Schema.Table_Constraints tc ON tc.Constraint_Type ='PRIMARY KEY'  AND tc.Constraint_Name = ku.Constraint_Name AND ku.Table_Name = dt.Table_Name
							WHERE ld.DimTableName = @TableName AND ld.DimSchemaName = @SchemaName
						) a
						WHERE COLUMN_NAME IS NOT NULL
					 )

	--Build dynamic Source query to be used in MERGE statement - Step 2 - build from clause for source query with joins. The lookup table information is stored in LookupDetails table
	SET @SrcLookup = (SELECT STRING_AGG(JoinTbl+'('+SUBSTRING(JoinCondition,5,Len(JoinCondition))+SCDCondition,CHAR(13))
					  FROM
					  (
					  	SELECT DISTINCT 
					  		(' LEFT JOIN ' + LookupSchemaName + '.' + LookupTableName + ' ' + LookupTableAliasName + ' ON ') AS JoinTbl,
					  		(
					  			SELECT STRING_AGG(' AND  ' + 'src.'+ StgColumnName+ '= ' + LookupTableAliasName + '.' + LookupColumnName,CHAR(13))
					  			FROM etl.DimensionLookupDetails JC
					  			WHERE JC.StgTableName = LD.StgTableName
					  			AND JC.LookupTableName=LD.LookupTableName 
								AND JC.LookupTableAliasName=LD.LookupTableAliasName
					          ) AS JoinCondition,
							(CASE WHEN LookupIsSCD = 'Yes' 
							  THEN ' AND ' + LookupTableAliasName + '.IsCurrent = 1)'
							  ELSE ') ' END) AS SCDCondition,
							  dt.TABLE_NAME
					  	FROM etl.DimensionLookupDetails LD
						LEFT JOIN INFORMATION_SCHEMA.Columns dt ON (LD.LookupSchemaName = dt.TABLE_SCHEMA And LD.LookupTableName = dt.Table_name)
					  	WHERE LD.DimTableName = @TableName AND ld.DimSchemaName = @SchemaName
					  ) a
					  WHERE TABLE_NAME is not null
					)
			
	--Build dynamic Source query to be used in MERGE statement - Step 3 - Combine select clause and from clause to build the source query
	SET @SrcSelectSql = 'SELECT '+@SrcSelect+' FROM stage.'+@TableName+' src '+CHAR(13)+@SrcLookup

	--Get Stage Schema Name
	SET @SrcSchemaName = (SELECT DISTINCT  StgSchemaName FROM etl.DimensionLookupDetails WHERE DimTableName = @TableName AND DimSchemaName = @SchemaName)

	--Get Stage table Name
	SET @SrcTableName = (SELECT DISTINCT  StgTableName FROM etl.DimensionLookupDetails WHERE DimTableName = @TableName AND DimSchemaName = @SchemaName)

	--Build the target table column list
	SET @ColumnList = (SELECT STUFF((SELECT ',' + ColumnName + CHAR(10)        
					   FROM #tmpMerge x1        
					   WHERE x1.TableName = x2.TableName  AND x1.IsSurrogateKey <> 1      
					   FOR XML PATH ('')), 1, 1,'') AS MergeOnColumns     
					   FROM #tmpMerge x2     
					   WHERE x2.IsSurrogateKey <> 1 
					   GROUP BY TableName);            
	
	--Build the Merge join condition between source and target
	SET @MergeOn = (SELECT STUFF((SELECT 'AND ' + MergeOn + CHAR(10)        
					FROM #tmpMerge x1        
					WHERE x1.TableName = x2.TableName AND x1.IsNaturalKey = 1        
					FOR XML PATH ('')), 1, 4,'')     
					FROM #tmpMerge x2     
					WHERE IsNaturalKey = 1     
					GROUP BY TableName);        
	
	--Build the Merge Update column list
	SET @Updateon = (select STRING_AGG(UpdateOn,',') from #tmpMerge	where IsNaturalKey=0 and IsSurrogateKey=0 and ColumnName NOT IN ('CreatedDate','ModifiedDate'))

	--Build MERGE Insert statement for the target table
	SET @MergeInsert = (SELECT STUFF(
							(SELECT ',' +  InsertOn + CHAR(10)        
							FROM #tmpMerge x1        
							WHERE x1.TableName = x2.TableName  AND x1.IsSurrogateKey <> 1   AND x1.ColumnName NOT IN ('IsCurrent','EffectiveStartTime','EffectiveEndTime','IsDeleted','CreatedDate','ModifiedDate')  
							FOR XML PATH ('')), 1, 1,'')      
						FROM #tmpMerge x2 
						WHERE x2.IsSurrogateKey <> 1  AND x2.ColumnName NOT IN ('IsCurrent','EffectiveStartTime','EffectiveEndTime','IsDeleted','CreatedDate','ModifiedDate')  
						GROUP BY TableName);         

	SET @MergeOutput = '$action as action,'+
						(SELECT STUFF(
							(SELECT ',' + InsertOn + CHAR(10)        
							 FROM #tmpMerge x1        
							 WHERE x1.TableName = x2.TableName AND x1.IsSurrogateKey <> 1  AND x1.ColumnName NOT IN ('IsCurrent','EffectiveStartTime','EffectiveEndTime','IsDeleted','CreatedDate','ModifiedDate')       
							 FOR XML PATH ('')), 1, 1,'')      
						FROM #tmpMerge x2
						WHERE  x2.IsSurrogateKey <> 1  AND x2.ColumnName NOT IN ('IsCurrent','EffectiveStartTime','EffectiveEndTime','IsDeleted','CreatedDate','ModifiedDate') 
						GROUP BY TableName)         
	
	--Build Deletion condition to check if a row has been deleted in source
	SET @MergeDeleteChk = (SELECT STUFF((SELECT 'AND ' + ColumnName + ' IS NOT NULL '+ CHAR(10)        
							FROM #tmpMerge x1        
							WHERE x1.TableName = x2.TableName AND x1.IsNaturalKey = 1   
							FOR XML PATH ('')), 1, 4,'')     
							FROM #tmpMerge x2     
							WHERE IsNaturalKey = 1    
							GROUP BY TableName)
	
	--Get SCD info about dim table
	SET @IsDimSCD = (SELECT DISTINCT  DimIsSCD FROM etl.DimensionLookupDetails WHERE DimTableName = @TableName AND DimSchemaName = @SchemaName)
									
	--Build dynamic MERGE Statement. This MERGE statement is used for loading SCD2 dim tables
	SET @DimSCD2MergeSql = 
	'INSERT INTO ['+@SchemaName+'].['+@TableName+'] ('+@columnlist+') '+CHAR(10)+
	'SELECT '+@columnlist+CHAR(10)+        
	' FROM '+CHAR(10)+        
	'('+CHAR(10)+         
		'MERGE INTO ['+@SchemaName+'].['+@TableName+'] AS Target1 '+CHAR(10)+         
		'USING ' + 
		(CASE WHEN @SrcSelectSql IS NULL THEN '['+@SrcSchemaName+'].['+@SrcTableName+'] AS Source1 '
			ELSE
			'( '+ CHAR(10) + @SrcSelectSql + 
			 ') AS Source1 ' END) +CHAR(10)+         
		
		'ON ('+@MergeOn+') '+ CHAR(10)+           
		
		'WHEN MATCHED '+CHAR(10)+         
		'THEN '+CHAR(10)+         
		'UPDATE SET Target1.EffectiveEndTime = GetDate(), IsCurrent = 0, ModifiedDate = GETDATE() ' +CHAR(10)+                  
		
		'WHEN NOT MATCHED BY Target '+CHAR(10)+         
		'THEN '+CHAR(10)+         
		'INSERT ('+@columnlist+') '+CHAR(10)+         
		'VALUES ('+@MergeInsert+', 1, GetDate(), ''9999-12-31'', 0, GetDate(), GetDate()) '+CHAR(10)+                  

		'WHEN NOT MATCHED BY Source'+CHAR(10)+         
		'THEN '+CHAR(10)+         
		'UPDATE SET Target1.IsDeleted = 1, Target1.ModifiedDate = GetDate() '+CHAR(10)+      
		
		'OUTPUT '+@MergeOutput+ ', 1 AS IsCurrent, GetDate() AS EffectiveStartTime, ''9999-12-31'' AS EffectiveEndTime, 0 AS IsDeleted, GetDate() AS CreatedDate, GetDate() AS ModifiedDate ' + CHAR(10) + 
	') as Changes 
	WHERE action=''UPDATE'' AND '+ @MergeDeleteChk +';'        


	--Build dynamic MERGE Statement. This MERGE statement is used for loading SCD1 dim tables
	SET @DimMergeSql = 
		'MERGE INTO ['+@SchemaName+'].['+@TableName+'] AS Target1 '+CHAR(10)+         
		'USING ' + 
		(CASE WHEN @SrcSelectSql IS NULL THEN '['+@SrcSchemaName+'].['+@SrcTableName+'] AS Source1 '
			ELSE
			'( '+ CHAR(10) + @SrcSelectSql + 
			 ') AS Source1 ' END) +CHAR(10)+         
		
		'ON ('+@MergeOn+') '+ CHAR(10)+           
		
		(CASE WHEN @Updateon IS NULL THEN ''
			ELSE
			'WHEN MATCHED '+CHAR(10)+         
			'THEN '+CHAR(10)+         
			'UPDATE SET ' + @Updateon + ',Target1.ModifiedDate = GetDate() ' END) +CHAR(10)+                  
		
		'WHEN NOT MATCHED BY Target '+CHAR(10)+         
		'THEN '+CHAR(10)+         
		'INSERT ('+@columnlist+') '+CHAR(10)+         
		'VALUES ('+@MergeInsert+', GetDate(), GetDate()) ;'+CHAR(10)   

	--Execute the appropriate Merge SQL based on SCD2 check
	IF @IsDimSCD = 'Yes'
		EXEC sp_executesql @DimSCD2MergeSql
	ELSE 
		EXEC sp_executesql @DimMergeSql

	--Truncate stage table
	EXEC sp_executesql @TruncateStgTableSql


END
GO


