INSERT INTO config.LookupDetails (StgTableName, StgDatabaseName, StgSchemaName, StgColumnName, LookupTableName, LookupDatabaseName, LookupSchemaName, LookupColumnName, CreatedDate, ModifiedDate)

/*--------FPA Lookup Details -----------------*/
SELECT 'Forecast','sqldb-mic-d-01','stage','Channel','ForecastDetail','sqldb-mic-d-01','dim','Channel',GETDATE(),GETDATE()
UNION ALL
SELECT 'Forecast','sqldb-mic-d-01','stage','FileName','ForecastDetail','sqldb-mic-d-01','dim','FileName',GETDATE(),GETDATE()
UNION ALL
SELECT 'Forecast','sqldb-mic-d-01','stage','Version','ForecastDetail','sqldb-mic-d-01','dim','Version',GETDATE(),GETDATE()
UNION ALL
SELECT 'Forecast','sqldb-mic-d-01','stage','Segment','ForecastDetail','sqldb-mic-d-01','dim','Segment',GETDATE(),GETDATE()
UNION ALL
SELECT 'Forecast','sqldb-mic-d-01','stage','BusinessUnit','ForecastDetail','sqldb-mic-d-01','dim','BusinessUnit',GETDATE(),GETDATE()
UNION ALL
SELECT 'Forecast','sqldb-mic-d-01','stage','ForecastMonth','ForecastDetail','sqldb-mic-d-01','dim','ForecastMonth',GETDATE(),GETDATE()
UNION ALL
SELECT 'Forecast','sqldb-mic-d-01','stage','ForecastYear','ForecastDetail','sqldb-mic-d-01','dim','ForecastYear',GETDATE(),GETDATE()
UNION ALL
SELECT 'Forecast','sqldb-mic-d-01','stage','Type','ForecastDetail','sqldb-mic-d-01','dim','Type',GETDATE(),GETDATE()
UNION ALL
SELECT 'Forecast','sqldb-mic-d-01','stage','ChangedBy','ForecastDetail','sqldb-mic-d-01','dim','ChangedBy',GETDATE(),GETDATE()
UNION ALL
SELECT 'Forecast','sqldb-mic-d-01','stage','BusinessMetric','BusinessMetric','sqldb-mic-d-01','dim','BusinessMetric',GETDATE(),GETDATE()
UNION ALL
SELECT 'Forecast','sqldb-mic-d-01','stage','UnitOfMeasure','UnitOfMeasure','sqldb-mic-d-01','dim','UnitOfMeasure',GETDATE(),GETDATE()

GO