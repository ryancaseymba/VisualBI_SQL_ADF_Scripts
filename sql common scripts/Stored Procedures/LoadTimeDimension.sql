/********************************************************************************************************************************/
--Object Name : [etl].[LoadTimeDimension]
--Purpose	  : Generic Procedure to load time dimension
--Date Creaed : 8/13/2020
--Created By : DSAMRAJ
/********************************************************************************************************************************/


CREATE PROCEDURE [etl].[LoadTimeDimension]
AS 
BEGIN 
	
	--Variable Declaration
	DECLARE @hour INTEGER 
	,@minute INTEGER 
	,@second INTEGER 
	,@k INTEGER 
	,@Time24 VARCHAR(25) 
	,@Time12 VARCHAR(25)
	,@Hour24 VARCHAR(4) 
	,@Minute30 VARCHAR(4) 
	,@Second30 VARCHAR(4)  

	--Set Initial Values
	SET @hour = 0 
	SET @minute = 0 
	SET @second = 0 
	SET @k = 1 
	
	--Begin Hour loop. Loop ends when hour reaches 23
	WHILE(@hour <= 23 ) 
	BEGIN 
	
		IF (@hour <10 )
			SET @Hour24 = '0' + CAST( @hour AS VARCHAR(10))
		ELSE  
			SET @Hour24 = @hour
		
		--Begin Minute loop. Loop ends when minute reaches 59
		WHILE(@minute <= 59) 
		BEGIN 
			
			--Begin Second loop. Loop ends when second reaches 59
			WHILE(@second <= 59) 
			BEGIN 
	
				IF @minute <10 
					SET @Minute30 = '0' + CAST( @minute AS VARCHAR(10) ) 
				ELSE  
					SET @Minute30 = @minute 
	
				IF @second <10
					SET @Second30 = '0' + CAST( @second AS VARCHAR(10) ) 
				ELSE 
					SET @Second30 = @second
	
				--Derivation of 24 Hour Time and 12 Hour Time formats
				SET @Time24 = @Hour24 +':'+@Minute30 +':'+@Second30 
				SELECT @Time12 = FORMAT(CAST(@Time24 AS DATETIME),'hh:mm:ss tt')
	
				--Insert into time dimension table 
				INSERT INTO dim.Time (
					[TimeKey],
					[Time24Hr],
					[Time12Hr],
					[Hour24],
					[Hour12],
					[MinuteNumber],
					[SecondNumber],
					[CreatedDate],
					[ModifiedDate]
				) 
				VALUES (
					@k,
					@Time24,
					@Time12,
					@hour,
					Left(@Time12,2),
					@minute,
					@Second,
					getdate(),
					getdate()
				) 
				
				--Increment second by 1 and Id by 1 for next looping
				SET @second = @second + 1 
				SET @k = @k + 1 
	
			END 

			--Increment minute by 1 for next looping
			SET @minute = @minute + 1 
			SET @second = 0 
	
		END 

		--Increment hour by 1 for next looping
		SET @hour = @hour + 1 
		SET @minute =0 
	
	END 

END