CREATE TABLE [config].[LookupDetails]
(
	[StgTableName] NVARCHAR(255) NULL,
	[StgDatabaseName] NVARCHAR(255) NULL,
	[StgSchemaName] NVARCHAR(100) NULL,
	[StgColumnName] NVARCHAR(255) NULL,
	[LookupTableName] NVARCHAR(255) NULL,
	[LookupDatabaseName] NVARCHAR(255) NULL,
	[LookupSchemaName] NVARCHAR(100) NULL,
	[LookupColumnName] NVARCHAR(255) NULL,
	[CreatedDate] DATETIME NULL,
	[ModifiedDate] DATETIME NULL
);
GO