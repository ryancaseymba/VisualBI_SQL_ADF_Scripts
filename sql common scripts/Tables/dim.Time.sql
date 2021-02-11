CREATE TABLE [dim].[Time] (
    [TimeKey]     INT          NOT NULL,
    [Time24Hr]     VARCHAR (20) ,
    [Time12Hr]     VARCHAR(20)  ,
    [Hour24]       TINYINT      ,
    [Hour12]       TINYINT      ,
    [MinuteNumber] TINYINT      ,
    [SecondNumber] TINYINT      ,
    [CreatedDate]  DATETIME     ,
    [ModifiedDate] DATETIME     ,
    CONSTRAINT [PkTime] PRIMARY KEY CLUSTERED ([TimeKey] ASC)
);

