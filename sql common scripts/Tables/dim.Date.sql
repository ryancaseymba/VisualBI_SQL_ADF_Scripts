CREATE TABLE [dim].[Date] (
    [DateKey]         INT          NOT NULL,
    [Date]             DATE         NOT NULL,
    [Day]              SMALLINT     NULL,
    [Week]             SMALLINT     NULL,
    [WeekDayName]      VARCHAR (20) NULL,
    [WeekDayNameShort] VARCHAR (10) NULL,
    [DayOfWeek]        SMALLINT     NULL,
    [DayOfYear]        SMALLINT     NULL,
    [Month]            SMALLINT     NULL,
    [MonthName]        VARCHAR (12) NULL,
    [MonthNameShort]   VARCHAR (10) NULL,
    [Quarter]          SMALLINT     NULL,
    [QuarterName]      VARCHAR (10) NULL,
    [Year]             SMALLINT     NULL,
    [FirstOfYear]      DATE         NULL,
    [CreatedDate]      DATETIME         NULL,
    [ModifiedDate]     DATETIME         NULL,
    CONSTRAINT [PkDate] PRIMARY KEY CLUSTERED ([DateKey] ASC)
);

