/*
********************************************************************************************
Cellular Automation - SQL 2005 & Azure compatible workload simulation v3.3 (2017-10-02)
(C) 2017, Paul Brewer
                         
Feedback: paulbrewer@yahoo.co.uk
Description - https://paulbrewer.wordpress.com/2015/07/19/sql-server-performance-synthetic-transaction-baseline/
Synopsis: Creates 'Game of Life' Solution solution in SQL Server for load simulation.
********************************************************************************************
*/

-- Create Tables and views
-- A load control table used by Data Factory for incremental loads 
IF OBJECT_ID('load_control') IS NOT NULL
	DROP TABLE dbo.load_control;
GO

CREATE TABLE dbo.load_control 
(
	id INT IDENTITY(1,1) PRIMARY KEY, 
	load_start DATETIME, 
	load_end DATETIME, 
	row_count INT, 
	load_status varchar(10), 
	updated_at TIMESTAMP
);
GO

-- Game of Life patterns and views
IF EXISTS (SELECT * FROM sys.tables WHERE [name] = 'Merkle')
	DROP TABLE dbo.Merkle;
GO

CREATE TABLE dbo.Merkle
(
    ID INT IDENTITY(1,1),
    Session_ID INT NOT NULL,
    Pattern_ID INT NOT NULL, 
    x INT NOT NULL, 
    y INT NOT NULL,
	updated_at TIMESTAMP,
    CONSTRAINT PK_Merkle PRIMARY KEY NONCLUSTERED
    (ID) ON [PRIMARY]
) ON [PRIMARY]
GO
  
CREATE CLUSTERED INDEX CIX_Merkle_Session_ID ON dbo.Merkle(Session_ID, Pattern_ID, x, y, ID); 
GO

-- A view of changed Game of Life patterns
IF EXISTS (SELECT * FROM sys.views WHERE [name] = 'vw_transform_merkle')
	DROP VIEW dbo.vw_transform_merkle;
GO

CREATE VIEW [dbo].[vw_transform_merkle] AS
SELECT m.ID, m.Pattern_ID, m.Session_ID, m.x, m.y, m.updated_at
FROM dbo.merkle m
WHERE m.updated_at > (SELECT ISNULL(MAX(updated_at),0) FROM dbo.load_control WHERE load_status NOT IN ('Failed','Running'));
GO

-- An application work table            
IF EXISTS (SELECT * FROM sys.tables WHERE [name] = 'GridReference')
	DROP TABLE dbo.GridReference;
GO

CREATE TABLE dbo.GridReference 
(
    ID INT IDENTITY(1,1), 
    Session_ID INT,
    Pattern_ID INT NOT NULL, 
    x INT NOT NULL,
    y INT NOT NULL, 
    merkle_exists BIT NULL DEFAULT 0,
    neighbours INT NULL DEFAULT 0,
    CONSTRAINT PK_GridReference PRIMARY KEY NONCLUSTERED
    (ID) ON [PRIMARY]
) ON [PRIMARY]
GO
  
CREATE CLUSTERED INDEX CIX_GridReference_Session_ID ON dbo.GridReference(Session_ID, Pattern_ID, x, y, ID); 
GO
  
CREATE NONCLUSTERED INDEX CIX_GridReference_MerkelExists ON dbo.GridReference(Session_ID, Pattern_ID, merkle_exists) INCLUDE(x,y); 
GO

  ---------------------------------------------------------------------------------------------------------------
-- Error handling
-- http://www.sommarskog.se/error_handling/Part1.html 

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'error_handler_sp')
	EXEC ('CREATE PROC dbo.error_handler_sp AS SELECT ''stub version, to be replaced''')
GO 

ALTER PROCEDURE error_handler_sp AS
    
DECLARE @errmsg   nvarchar(2048),
        @severity tinyint,
        @state    tinyint,
        @errno    int,
        @proc     sysname,
        @lineno   int
              
SELECT @errmsg = error_message(), @severity = error_severity(),
        @state  = error_state(), @errno = error_number(),
        @proc   = error_procedure(), @lineno = error_line()
          
IF @errmsg NOT LIKE '***%'
BEGIN
    SELECT @errmsg = '*** ' + coalesce(quotename(@proc), '<dynamic SQL>') + 
                    ', Line ' + ltrim(str(@lineno)) + '. Errno ' + 
                    ltrim(str(@errno)) + ': ' + @errmsg
END
RAISERROR('%s', @severity, @state, @errmsg);
   
GO
   
        
---------------------------------------------------------------------------------------------------------------
-- Pattern lifecycle birth
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'CA_InitPatterns')
	EXEC ('CREATE PROC dbo.CA_InitPatterns AS SELECT ''stub version, to be replaced''')
GO 
                   
ALTER PROCEDURE dbo.CA_InitPatterns @StressLevel INT = 2
AS
BEGIN
       
    SET XACT_ABORT, NOCOUNT ON;
    BEGIN TRY
   
        DECLARE @Session_ID INT;
        SELECT @Session_ID = @@SPID;
            
        IF @StressLevel IS NULL
            SET @StressLevel = 2;
            
        IF @StressLevel NOT IN (1,2,3)
        BEGIN;
            RAISERROR('Input parameters can only be 1 (gentle), 2(moderate), 3 (severe)', 16, 1);
            RETURN;
        END;
   
        BEGIN TRANSACTION;
            
            DELETE FROM dbo.Merkle WHERE Session_ID = @Session_ID;
            DELETE FROM dbo.GridReference WHERE Session_ID = @Session_ID;
            
            IF @StressLevel >= 1
            BEGIN
            
                -- Blinker
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,5,8);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,6,8);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,7,8);    
            END
            
            IF @StressLevel >= 2
            BEGIN
            
                -- Toad
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-8,7);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-7,7);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-6,7);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-9,6);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-8,6);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-7,6);
                       
                -- Beacon
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-9,-4);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-8,-4);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-9,-5);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-8,-5);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-7,-6);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-6,-6);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-7,-7);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,-6,-7);
        
                ----Pulsar
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,7,-3);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,7,-4);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,7,-5);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,8,-5);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,13,-3);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,13,-4);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,13,-5);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,12,-5);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,3,-13);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,4,-13);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,5,-13);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,5,-12);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,3,-7);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,4,-7);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,5,-7);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,5,-8);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,8,-15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,7,-15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,7,-16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,7,-17);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,12,-15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,13,-15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,13,-16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,13,-17);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,15,-12);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,15,-13);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,16,-13);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,17,-13);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,15,-8);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,15,-7);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,16,-7);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,17,-7);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,7,-8);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,7,-9);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,8,-9);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,8,-7);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,9,-7);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,9,-8);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,11,-8);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,11,-7);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,12,-7);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,12,-9);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,13,-9);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,13,-8);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,7,-12);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,7,-11);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,8,-11);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,8,-13);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,9,-13);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,9,-12);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,12,-11);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,13,-11);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,13,-12);
                   
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,11,-12);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,11,-13);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,12,-13);
        
            END;
            
            IF @StressLevel = 3
            BEGIN
                -- 2 Gosper Glider Guns
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,2,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,2,16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,3,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,3,16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,14,12);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,15,12);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,13,13);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,12,14);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,12,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,12,16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,13,17);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,14,18);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,15,18);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,16,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,17,13);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,17,17);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,18,14);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,18,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,18,16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,19,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,22,16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,22,17);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,22,18);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,23,16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,23,17);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,23,18);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,24,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,24,19);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,26,14);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,26,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,26,19);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,26,20);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,36,17);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,36,18);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,37,17);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,37,18);
            
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,52,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,52,16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,53,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,53,16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,64,12);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,65,12);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,63,13);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,62,14);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,62,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,62,16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,63,17);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,64,18);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,65,18);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,66,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,67,13);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,67,17);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,68,14);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,68,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,68,16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,69,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,72,16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,72,17);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,72,18);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,73,16);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,73,17);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,73,18);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,74,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,74,19);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,76,14);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,76,15);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,76,19);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,76,20);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,86,17);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,86,18);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,87,17);
                INSERT INTO dbo.Merkle(Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, 1,87,18);
        
            END;
        COMMIT
    END TRY
    BEGIN CATCH
        IF @@trancount > 0 ROLLBACK TRANSACTION
        EXEC error_handler_sp
        RETURN 55555
    END CATCH         
END; -- Create Procedure    
GO
        
---------------------------------------------------------------------------------------------------------------
-- Display Patterns 
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'CA_DspPatterns_SQL')
	EXEC ('CREATE PROC dbo.CA_DspPatterns_SQL AS SELECT ''stub version, to be replaced''')
GO 
                
ALTER PROCEDURE dbo.CA_DspPatterns_SQL @Pattern_ID INT = 0, @Session_ID INT = @@SPID
AS
BEGIN
    SET XACT_ABORT, NOCOUNT ON;
    BEGIN TRY    
       
       DECLARE @RowCount INT;
        
        IF ISNULL(@Pattern_ID,0) = 0
            SELECT @Pattern_ID = (SELECT MAX(Pattern_ID) FROM dbo.Merkle WHERE Session_ID = @Session_ID)
        
        IF ISNULL(@Session_ID,0) = 0
            SET @Session_ID = @@SPID
              
        IF NOT EXISTS (SELECT TOP 1 1 FROM dbo.Merkle WHERE Pattern_ID = @Pattern_ID AND Session_ID = @Session_ID)
        BEGIN
            RAISERROR('No patterns exist for this session, run the initialization procedure first.', 16, 1);
            RETURN
        END;
              
        DECLARE @x_upper INT, @x_lower INT, @y_upper INT, @y_lower INT;
              
        SELECT @x_upper = (SELECT MAX(x) FROM dbo.Merkle WHERE Pattern_ID = @Pattern_ID AND Session_ID = @Session_ID);
        SET @x_upper = @x_upper + 1;
                
        SELECT @x_lower = (SELECT MIN(x) FROM dbo.Merkle WHERE Pattern_ID = @Pattern_ID AND Session_ID = @Session_ID);
        SET @x_lower = @x_lower - 1;
                
        SELECT @y_upper = (SELECT MAX(y) FROM dbo.Merkle WHERE Pattern_ID = @Pattern_ID AND Session_ID = @Session_ID);
        SET @y_upper = @y_upper + 1;
                
        SELECT @y_lower = (SELECT MIN(y) FROM dbo.Merkle WHERE Pattern_ID = @Pattern_ID AND Session_ID = @Session_ID);
        SET @y_lower = @y_lower - 1;
              
        WITH x_axis (x_coordinate) AS
        (
            SELECT @x_lower AS x_coordinate
            UNION ALL
            SELECT x_coordinate + 1
            FROM x_axis
            WHERE x_coordinate <= @x_upper
        ),
        y_axis (y_coordinate) AS
        (
            SELECT @y_lower AS y_coordinate
            UNION ALL
            SELECT y_coordinate + 1
            FROM y_axis
            WHERE y_coordinate <= @y_upper
        ) ,
        grid_reference (x_coordinate, y_coordinate, grid_reference) AS
        (
            SELECT
                x.x_coordinate,
                y.y_coordinate,
                'POLYGON( (' +
                CAST((x_coordinate+1.2) AS VARCHAR(7)) + ' ' + CAST((y_coordinate+1.2) AS VARCHAR(7)) + ','  +
                CAST((x_coordinate) AS VARCHAR(7)) + ' ' + CAST((y_coordinate+1.2) AS VARCHAR(7)) + ','  +
                CAST((x_coordinate) AS VARCHAR(7)) + ' ' + CAST((y_coordinate) AS VARCHAR(7)) + ','  +
                CAST((x_coordinate+1.2) AS VARCHAR(7)) + ' ' + CAST((y_coordinate) AS VARCHAR(7)) + ','  +
                CAST((x_coordinate+1.2) AS VARCHAR(7)) + ' ' + CAST((y_coordinate+1.2) AS VARCHAR(7)) +
            ') )' AS grid_reference
            FROM x_axis x
            CROSS JOIN y_axis y
        )
              
        SELECT m.x, m.y, CAST(g.grid_reference AS GEOMETRY)
        FROM dbo.Merkle m
        INNER JOIN grid_reference g  
            ON g.x_coordinate = m.x
            AND g.y_coordinate = m.y
        WHERE m.Pattern_ID = @Pattern_ID 
        AND Session_ID = @Session_ID OPTION ( MAXRECURSION 32767 );
   
    END TRY
    BEGIN CATCH
        IF @@trancount > 0 ROLLBACK TRANSACTION
        EXEC error_handler_sp
        RETURN 55555
    END CATCH                
              
END
GO
            
        
---------------------------------------------------------------------------------------------------------------
-- Procedure to generate x enumerations, test cycle factors
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'CA_GenPatterns_IO')
	EXEC ('CREATE PROC dbo.CA_GenPatterns_IO AS SELECT ''stub version, to be replaced''')
GO 
                
ALTER PROCEDURE dbo.CA_GenPatterns_IO @NewPatterns INT = 1
AS
BEGIN
    SET XACT_ABORT, NOCOUNT ON;    
    BEGIN TRY
   
        DECLARE @Iteration INT, @LastIteration INT, @CurrentIteration INT, @NewIteration INT, @RowCount INT;
        
        DECLARE @Session_ID INT;
        SELECT @Session_ID = @@SPID;
              
        -- Defaults
        IF ISNULL(@NewPatterns,0) = 0
            SET @NewPatterns = 1;
         
         -- Patterns Generated, Iterations
        SET @CurrentIteration = 1;
        WHILE @CurrentIteration <= @NewPatterns
        BEGIN
         
            SELECT @LastIteration  = MAX(Pattern_ID) FROM dbo.Merkle WHERE Session_ID = @Session_ID;
              
            IF NOT EXISTS (SELECT TOP 1 1 FROM dbo.Merkle WHERE Session_ID = @Session_ID)
            BEGIN
                RAISERROR('No patterns exist for this session, run the initialization procedure first.', 16, 1);
                RETURN
            END;
             
            --------------------------------------------------------------------------------------------------
            -- Create new working set of merkles, and the 8 cells adjacent to them
            SET @NewIteration = @LastIteration + 1;
   
            BEGIN TRANSACTION
         
                 -- Housekeeping - Tidy the Grid Reference worktable
                DELETE FROM dbo.GridReference WHERE Session_ID = @Session_ID; --Query Stats P-IO Q-DEL1
           
                INSERT INTO dbo.GridReference (Session_ID, x,y,merkle_exists, Pattern_ID)
                SELECT @Session_ID, x,y,1,@NewIteration FROM dbo.Merkle 
                WHERE Pattern_ID = @LastIteration
                AND Session_ID = @Session_ID;
         
                -- NE Neighbour
                INSERT INTO dbo.GridReference (Session_ID, x,y, Pattern_ID, neighbours) --Query Stats P-IO Q-INS1
                SELECT @Session_ID, x-1,y+1,@NewIteration,1 FROM dbo.Merkle 
                WHERE Pattern_ID = @LastIteration
                AND Session_ID = @Session_ID;
         
                -- N Neighbour
                INSERT INTO dbo.GridReference (Session_ID, x,y, Pattern_ID, neighbours) --Query Stats P-IO Q-INS1
                SELECT @Session_ID, x,y+1,@NewIteration,1 FROM dbo.Merkle 
                WHERE Pattern_ID = @LastIteration
                AND Session_ID = @Session_ID;
         
                -- NW Neighbour
                INSERT INTO dbo.GridReference (Session_ID,x,y, Pattern_ID, neighbours) --Query Stats P-IO Q-INS1
                SELECT @Session_ID, x+1,y+1,@NewIteration,1 FROM dbo.Merkle 
                WHERE Pattern_ID = @LastIteration
                AND Session_ID = @Session_ID;
         
                -- W Neighbour
                INSERT INTO dbo.GridReference (Session_ID, x,y, Pattern_ID, neighbours) --Query Stats P-IO Q-INS1
                SELECT @Session_ID, x+1,y,@NewIteration,1 FROM dbo.Merkle 
                WHERE Pattern_ID = @LastIteration
                AND Session_ID = @Session_ID;
         
                -- SW Neighbour
                INSERT INTO dbo.GridReference (Session_ID, x,y, Pattern_ID, neighbours) --Query Stats P-IO Q-INS1
                SELECT @Session_ID, x+1,y-1,@NewIteration,1 FROM dbo.Merkle 
                WHERE Pattern_ID = @LastIteration
                AND Session_ID = @Session_ID;
         
                -- S Neighbour
                INSERT INTO dbo.GridReference (Session_ID, x,y, Pattern_ID, neighbours) --Query Stats P-IO Q-INS1
                SELECT @Session_ID, x,y-1,@NewIteration,1 FROM dbo.Merkle 
                WHERE Pattern_ID = @LastIteration
                AND Session_ID = @Session_ID;
         
                -- SE Neighbour
                INSERT INTO dbo.GridReference (Session_ID, x,y, Pattern_ID, neighbours) --Query Stats P-IO Q-INS1
                SELECT @Session_ID, x-1,y-1,@NewIteration,1 FROM dbo.Merkle 
                WHERE Pattern_ID = @LastIteration
                AND Session_ID = @Session_ID;
         
                -- E Neighbour
                INSERT INTO dbo.GridReference (Session_ID, x,y, Pattern_ID, neighbours) --Query Stats P-IO Q-INS1
                SELECT @Session_ID, x-1,y,@NewIteration,1 FROM dbo.Merkle 
                WHERE Pattern_ID = @LastIteration
                AND Session_ID = @Session_ID;
         
                --------------------------------------------------------------------------------------------------
                -- Empty cells come alive at next iteration rule
                INSERT INTO dbo.Merkle (Session_ID, Pattern_ID,x,y) --Query Stats P-IO Q-INS2
                SELECT @Session_ID, @NewIteration,x,y
                FROM dbo.GridReference gr
                WHERE merkle_exists = 0
                AND gr.Session_ID = @Session_ID
                AND gr.Pattern_ID = @NewIteration
                AND NOT EXISTS 
                (
                    SELECT 1
                    FROM dbo.GridReference 
                    WHERE x = gr.x
                    AND y = gr.y
                    AND merkle_exists = 1
                    AND Session_ID = @Session_ID
                    AND Pattern_ID = @NewIteration
                )
                GROUP BY x,y 
                HAVING COUNT(*) = 3;
         
                --------------------------------------------------------------------------------------------------
                -- Merkle cells stays alive at next iteration rule
                INSERT INTO dbo.Merkle (Session_ID, Pattern_ID,x,y) --Query Stats P-IO Q-INS3
                SELECT @Session_ID, @NewIteration,gr.x,gr.y
                FROM dbo.GridReference gr
                INNER JOIN
                (
                    SELECT x,y, COUNT(*) AS adjacentmerkles
                    FROM dbo.GridReference 
                    WHERE merkle_exists = 0
                    AND Session_ID = @Session_ID
                    AND Pattern_ID = @NewIteration
                    GROUP BY x,y
                ) neighbours
                    ON neighbours.x = gr.x
                    AND neighbours.y = gr.y
                WHERE gr.merkle_exists = 1
                AND neighbours.adjacentmerkles IN (2,3)
                AND Session_ID = @Session_ID
                AND Pattern_ID = @NewIteration;
   
            COMMIT;
            --------------------------------------------------------------------------------------------------
            SET @CurrentIteration = @CurrentIteration + 1
            --------------------------------------------------------------------------------------------------
        END -- Iterations
    END TRY
    BEGIN CATCH
        IF @@trancount > 0 ROLLBACK TRANSACTION
        EXEC error_handler_sp
        RETURN 55555
    END CATCH   
END -- Procedure
   
GO
          
         
---------------------------------------------------------------------------------------------------------------
-- Procedure to generate x enumerations, test cycle factors
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'CA_GenPatterns_CPU')
	EXEC ('CREATE PROC dbo.CA_GenPatterns_CPU AS SELECT ''stub version, to be replaced''')
GO 
                
ALTER PROCEDURE dbo.CA_GenPatterns_CPU @NewPatterns INT = 1
AS
BEGIN
   
    SET XACT_ABORT, NOCOUNT ON;
    BEGIN TRY
   
        DECLARE @RowCount INT, @Iteration INT, @LastIteration INT, @CurrentIteration INT, @NewIteration INT;
        DECLARE @x_upper INT, @x_lower INT, @y_upper INT, @y_lower INT;
        DECLARE @x INT, @y INT, @NeighboursCount INT, @grid_reference GEOMETRY;
        DECLARE @Session_ID INT;
          
        SELECT @Session_ID = @@SPID;
               
        IF OBJECT_ID('tempdb..#MerkleGrid') IS NOT NULL
            DROP TABLE #MerkleGrid;
              
        -- Merkle Cells
        CREATE TABLE #MerkleGrid (x INT NOT NULL,y INT NOT NULL, grid_reference GEOMETRY NULL);
        ALTER TABLE #MerkleGrid ADD PRIMARY KEY CLUSTERED (x, y);
              
        -- Default to 1 iteration
        IF ISNULL(@NewPatterns,0) = 0
            SET @NewPatterns = 1;
              
        -- Patterns Generated Iterations Counter
        SET @CurrentIteration = 1;
        WHILE @CurrentIteration <= @NewPatterns
        BEGIN
              
            TRUNCATE TABLE #MerkleGrid;
              
     
            IF NOT EXISTS (SELECT TOP 1 1 FROM dbo.Merkle WHERE Session_ID = @Session_ID)
            BEGIN
                RAISERROR('No patterns exist for this session, run the initialization procedure first.', 16, 1);
                RETURN
            END;
              
            SET @LastIteration = (SELECT MAX(Pattern_ID) FROM dbo.Merkle WHERE Session_ID = @Session_ID)
              
            SELECT @x_upper = (SELECT MAX(x) FROM dbo.Merkle WHERE Pattern_ID = @LastIteration AND Session_ID = @Session_ID);
            SET @x_upper = @x_upper + 1;
                
            SELECT @x_lower = (SELECT MIN(x) FROM dbo.Merkle WHERE Pattern_ID = @LastIteration AND Session_ID = @Session_ID);
            SET @x_lower = @x_lower - 1;
                
            SELECT @y_upper = (SELECT MAX(y) FROM dbo.Merkle WHERE Pattern_ID = @LastIteration AND Session_ID = @Session_ID);
            SET @y_upper = @y_upper + 1;
                
            SELECT @y_lower = (SELECT MIN(y) FROM dbo.Merkle WHERE Pattern_ID = @LastIteration AND Session_ID = @Session_ID);
            SET @y_lower = @y_lower - 1;
              
            BEGIN TRANSACTION;
                --------------------------------------------------------------------------------------------------
                -- Merkle stays alive at next iteration rule (2 or 3 merkle neighbours)
                WITH x_axis (x_coordinate) AS --Query Stats P-CPU Q-INS1A
                (
                    SELECT @x_lower AS x_coordinate
                    UNION ALL
                    SELECT x_coordinate + 1
                    FROM x_axis
                    WHERE x_coordinate <= @x_upper
                ) ,
                y_axis (y_coordinate) AS
                (
                    SELECT @y_lower AS y_coordinate
                    UNION ALL
                    SELECT y_coordinate + 1
                    FROM y_axis
                    WHERE y_coordinate <= @y_upper
                ) ,
                grid_reference (x_coordinate, y_coordinate, grid_reference, merkle_exists) AS
                (
                    SELECT
                        x.x_coordinate,
                        y.y_coordinate,
                        'POLYGON( (' +
                            CAST((x_coordinate+1.2) AS VARCHAR(7)) + ' ' + CAST((y_coordinate+1.2) AS VARCHAR(7)) + ','  +
                            CAST((x_coordinate) AS VARCHAR(7)) + ' ' + CAST((y_coordinate+1.2) AS VARCHAR(7)) + ','  +
                            CAST((x_coordinate) AS VARCHAR(7)) + ' ' + CAST((y_coordinate) AS VARCHAR(7)) + ','  +
                            CAST((x_coordinate+1.2) AS VARCHAR(7)) + ' ' + CAST((y_coordinate) AS VARCHAR(7)) + ','  +
                            CAST((x_coordinate+1.2) AS VARCHAR(7)) + ' ' + CAST((y_coordinate+1.2) AS VARCHAR(7)) +
                        ') )' AS grid_reference
                        ,CASE ISNULL(m.Pattern_ID,0) WHEN 0 THEN 'N' ELSE 'Y' END AS merkle_exists
                    FROM x_axis x
                    CROSS JOIN y_axis y
                    LEFT OUTER JOIN dbo.Merkle m
                        ON m.Pattern_ID =  @LastIteration
                        AND m.x = x.x_coordinate
                        AND m.y = y.y_coordinate
                        AND Session_ID = @Session_ID
                )
               
                INSERT INTO #MerkleGrid (x, y, grid_reference)  --Query Stats P-CPU Q-INS1B
                SELECT x_coordinate, y_coordinate, gr.grid_reference
                FROM grid_reference gr
                WHERE gr.merkle_exists = 'Y'
                OPTION ( MAXRECURSION 32767 );
         
                --------------------------------------------------------------------------------------------------
                -- 'Cell Dies' rules (process existing Merkles, see if they still live at the next iteration)
                DECLARE c1 CURSOR FOR
                SELECT (@LastIteration + 1) AS NewIteration, x,y, grid_reference
                FROM #MerkleGrid g1
                
                OPEN c1;
                FETCH NEXT FROM c1 INTO @NewIteration, @x, @y, @grid_reference
                WHILE @@FETCH_STATUS = 0
                BEGIN
              
              
                    SELECT @NeighboursCount = COUNT(*)   --Query Stats P-CPU Q-INS2
                    FROM #MerkleGrid g
                    WHERE EXISTS 
                    ( 
                        SELECT 1 
                        FROM dbo.Merkle 
                        WHERE Pattern_ID = @LastIteration 
                        AND x = g.x
                        AND y = g.y
                        AND Session_ID = @Session_ID
                    )
                    -- The STIntersects CLR method is invoked for CPU testing 
                    AND g.grid_reference.STIntersects(@grid_reference) = 1
      
                    -- Comment out to increase CPU load.
                    --AND g.y IN (@y,@y+1, @y-1)
                    --AND g.x IN (@x,@x+1, @x-1);
                    -- Comment out to increase CPU load.
            
                    SELECT @NeighboursCount = @NeighboursCount - 1 --Ignore intersection with self.
          
                    IF @NeighboursCount = 2 OR @NeighboursCount = 3
                        INSERT INTO dbo.Merkle (Session_ID, Pattern_ID, x, y) VALUES(@Session_ID, @NewIteration, @x, @y);
              
                    FETCH NEXT FROM c1 INTO @NewIteration, @x, @y, @grid_reference
                END;
                CLOSE c1;
                DEALLOCATE c1;
              
                --------------------------------------------------------------------------------------------------
                -- Cell comes alive rules, a Merkle is born 
                ;WITH merkles (x,y) AS --Query Stats P-CPU Q-INS3
                (
                    SELECT x,y
                    FROM dbo.Merkle 
                    WHERE Pattern_ID = @LastIteration
                    AND Session_ID = @Session_ID
                ) ,
                all_framing_cells (x, y) AS
                (
                    SELECT x,y
                    FROM merkle
                    UNION
                    SELECT x-1, y+1 -- NE
                    FROM merkles m
                    UNION
                    SELECT x, y+1 -- N
                    FROM merkles m
                    UNION
                    SELECT x+1, y+1 -- NW
                    FROM merkles m
                    UNION
                    SELECT x+1, y -- W
                    FROM merkles m
                    UNION
                    SELECT x+1, y-1 -- SW
                    FROM merkles m
                    UNION
                    SELECT x, y-1 -- S
                    FROM merkles m
                    UNION
                    SELECT x-1, y-1 -- SE
                    FROM merkles m
                    UNION
                    SELECT x-1, y -- E
                    FROM merkles m
                ) ,
                dead_framing_cells (x, y, merkle_exists) AS
                (
                    SELECT afc.x, afc.y, ISNULL(m.merkle_exists,'N') AS merkle_exists 
                    FROM all_framing_cells afc
                    LEFT OUTER JOIN
                    (
                        SELECT x,y, 'Y' as merkle_exists  
                        FROM dbo.Merkle
                        WHERE Pattern_ID = @LastIteration
                        AND Session_ID = @Session_ID
                    ) m 
                        ON m.x = afc.x
                        AND m.y = afc.y
                ) ,
                CellChecker AS
                (
                    SELECT
                        g.x, g.y,
                        SUM
                        ( 
                            ISNULL(Merkle_Exists_NW,0) +
                            ISNULL(Merkle_Exists_N,0) +
                            ISNULL(Merkle_Exists_NE,0) +
                            ISNULL(Merkle_Exists_E,0) +
                            ISNULL(Merkle_Exists_SE,0) +
                            ISNULL(Merkle_Exists_S,0) +
                            ISNULL(Merkle_Exists_SW,0) +
                            ISNULL(Merkle_Exists_W,0)
                        ) AS NeighboursCount
                    FROM dead_framing_cells g
          
                    -- top_left
                    LEFT OUTER JOIN
                    (
                        SELECT x,y, 1 AS Merkle_Exists_NW
                        FROM dbo.Merkle  m
                        WHERE m.Pattern_ID = @LastIteration
                        AND Session_ID = @Session_ID
                    ) top_left
                        ON top_left.x = g.x - 1
                        AND top_left.y = g.y + 1
                
                    -- above
                    LEFT OUTER JOIN
                    (
                        SELECT x,y, 1 AS Merkle_Exists_N
                        FROM dbo.Merkle  m
                        WHERE m.Pattern_ID = @LastIteration
                        AND Session_ID = @Session_ID
                    ) top_over
                        ON top_over.x = g.x
                        AND top_over.y = g.y + 1
                
                    -- top right
                    LEFT OUTER JOIN
                    (
                        SELECT x,y, 1 AS Merkle_Exists_NE
                        FROM dbo.Merkle  m
                        WHERE m.Pattern_ID = @LastIteration
                        AND Session_ID = @Session_ID
                    ) top_right
                        ON top_right.x = g.x + 1
                        AND top_right.y = g.y + 1
                
                    -- bottom right
                    LEFT OUTER JOIN
                    (
                        SELECT x,y, 1 AS Merkle_Exists_SE
                        FROM dbo.Merkle  m
                        WHERE m.Pattern_ID = @LastIteration
                        AND Session_ID = @Session_ID
                    ) bottom_right
                        ON bottom_right.x = g.x + 1
                        AND bottom_right.y = g.y - 1
                
                    -- bottom below
                    LEFT OUTER JOIN
                    (
                        SELECT x,y, 1 AS Merkle_Exists_S
                        FROM dbo.Merkle  m
                        WHERE m.Pattern_ID = @LastIteration
                        AND Session_ID = @Session_ID
                    ) bottom_under
                        ON bottom_under.x = g.x
                        AND bottom_under.y = g.y - 1
                
                    -- bottom left
                    LEFT OUTER JOIN
                    (
                        SELECT x,y, 1 AS Merkle_Exists_SW
                        FROM dbo.Merkle  m
                        WHERE m.Pattern_ID = @LastIteration
                        AND Session_ID = @Session_ID
                    ) bottom_left
                        ON bottom_left.x = g.x - 1
                        AND bottom_left.y = g.y - 1
                
                    -- middle_left
                    LEFT OUTER JOIN
                    (
                        SELECT x,y, 1 AS Merkle_Exists_W
                        FROM dbo.Merkle  m
                        WHERE m.Pattern_ID = @LastIteration
                        AND Session_ID = @Session_ID
                    ) middle_left
                        ON middle_left.x = g.x - 1
                        AND middle_left.y = g.y
                
                    -- bottom left
                    LEFT OUTER JOIN
                    (
                        SELECT x,y, 1 AS Merkle_Exists_E
                        FROM dbo.Merkle  m
                        WHERE m.Pattern_ID = @LastIteration
                        AND Session_ID = @Session_ID
                    ) middle_right
                        ON middle_right.x = g.x + 1
                        AND middle_right.y = g.y
                
                    WHERE
                    (top_left.x IS NOT NULL
                    OR top_over.x IS NOT NULL
                    OR top_right.x IS NOT NULL
                    OR bottom_right.x IS NOT NULL
                    OR bottom_under.x IS NOT NULL
                    OR bottom_left.x IS NOT NULL
                    OR middle_left.x IS NOT NULL
                    OR middle_right.x IS NOT NULL
                    )
                    GROUP BY g.x,g.y
                )           
         
                INSERT INTO dbo.Merkle (Session_ID, Pattern_ID, x, y) 
                SELECT @Session_ID, @NewIteration, gr.x, gr.y
                FROM dead_framing_cells gr
                JOIN
                (
                    SELECT x, y, NeighboursCount
                    FROM CellChecker cc
                ) neighbours
                    ON neighbours.x = gr.x
                    AND neighbours.y = gr.y     
         
                WHERE gr.merkle_exists = 'N'
                AND neighbours.NeighboursCount = 3 OPTION ( MAXRECURSION 32767 );
              
                --------------------------------------------------------------------------------------------------
                SET @CurrentIteration = @CurrentIteration + 1
                --------------------------------------------------------------------------------------------------
            COMMIT; 
        END
    END TRY
    BEGIN CATCH
        IF @@trancount > 0 ROLLBACK TRANSACTION
        EXEC error_handler_sp
        RETURN 55555
    END CATCH   
END -- Procedure
GO
    
---------------------------------------------------------------------------------------------------------------
-- Benchmarking procedure
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'CA_Benchmark')
	EXEC ('CREATE PROC dbo.CA_Benchmark AS SELECT ''stub version, to be replaced''')
GO 
                
ALTER PROCEDURE [dbo].[CA_Benchmark]
    @Batches INT = 1, @CPU_Benchmark BIT = 0, @IO_Benchmark BIT = 1, 
    @NewPatternsInBatch INT = 3, @DisplayPatterns BIT = 0, @Initialize BIT = 1, @StressLevel TINYINT = 1, 
    @Description1 VARCHAR(50) = NULL, @Description2 VARCHAR(50) = NULL, @Description3 VARCHAR(50) = NULL
AS
BEGIN
       
    SET XACT_ABORT, NOCOUNT ON;
    BEGIN TRY
        DECLARE @CurrentBatch INT;
        DECLARE @SessionID INT;
        DECLARE @BenchmarkStartTime DATETIME;
        DECLARE @DefaultHeartBeat BIT;
  
        -- SQL Server 2005 compatibility
        SET @CurrentBatch = 0;
        SET @SessionID = @@SPID;
        SET @BenchmarkStartTime = GETDATE();
        SET @DefaultHeartBeat = 0;
  
  
        -- Default to all
        IF @CPU_Benchmark = 0 AND @IO_Benchmark = 0
        BEGIN
            SET @CPU_Benchmark = 1;
            SET @IO_Benchmark = 1;
            SET @Batches = 1;
            SET @DefaultHeartBeat = 1;
        END
     
        -- Validation
        IF @StressLevel NOT IN (1,2,3) 
        BEGIN
            RAISERROR('Valid stress Levels are 1 (gentle) 2 (mederate) and 3 (severe).', 16, 1);
            RETURN;
        END
  
        -- Initialise for display when batches = 0
        EXECUTE dbo.CA_InitPatterns @StressLevel = @StressLevel;
      
        WHILE @CurrentBatch < @Batches
        BEGIN
     
            -- Benchmark CPU
            IF @CPU_BenchMark = 1
            BEGIN
                IF @Initialize = 1
                    EXECUTE dbo.CA_InitPatterns @StressLevel = @StressLevel;
  
                IF @DefaultHeartBeat = 1
                BEGIN
                    SET @StressLevel = 3
                    SET @NewPatternsInBatch = 1
                END
                        
                EXECUTE dbo.CA_GenPatterns 
                    @BenchmarkStartTime = @BenchmarkStartTime, @NewPatternsInBatch = @NewPatternsInBatch, @BenchmarkPerspective = 'CPU', @StressLevel = @StressLevel,
                    @Description1 = @Description1, @Description2 = @Description2, @Description3 = @Description3
            END
  
            -- Benchmark IO
            IF @IO_BenchMark = 1
            BEGIN
                IF @Initialize = 1
                    EXECUTE dbo.CA_InitPatterns @StressLevel = @StressLevel;
    
                IF @DefaultHeartBeat = 1
                BEGIN
                    SET @StressLevel = 3
                    SET @NewPatternsInBatch = 25
                END
  
                EXECUTE dbo.CA_GenPatterns 
                    @BenchmarkStartTime = @BenchmarkStartTime, @NewPatternsInBatch = @NewPatternsInBatch, @BenchmarkPerspective = 'IO', @StressLevel = @StressLevel,
                    @Description1 = @Description1, @Description2 = @Description2, @Description3 = @Description3
            END
  
            SET @CurrentBatch = @CurrentBatch + 1
     
        END
  
  
        IF @DisplayPatterns = 1 
            EXECUTE dbo.CA_DspPatterns_SQL;   
  
    END TRY
    BEGIN CATCH
        IF @@trancount > 0 ROLLBACK TRANSACTION
        EXEC error_handler_sp;
        RETURN 55555;
    END CATCH  
       
END --Procedure
GO

---------------------------------------------------------------------------------------------------------------
-- Procedure to generate x enumerations, test cycle factors, calls IO or CPU procedures
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'CA_GenPatterns')
	EXEC ('CREATE PROC dbo.CA_GenPatterns AS SELECT ''stub version, to be replaced''')
GO 
                
ALTER PROCEDURE dbo.CA_GenPatterns 
    @BenchmarkStartTime DATETIME, @NewPatternsInBatch INT, @BenchmarkPerspective VARCHAR(3), @StressLevel TINYINT,
    @Description1 VARCHAR(50) = NULL, @Description2 VARCHAR(50) = NULL, @Description3 VARCHAR(50) = NULL
AS
BEGIN
         
SET XACT_ABORT, NOCOUNT ON;
BEGIN TRY
  
    -- Generate patterns
    IF @BenchmarkPerspective = 'IO'
        EXEC dbo.CA_GenPatterns_IO @NewPatternsInBatch;
      
    IF @BenchmarkPerspective = 'CPU'
        EXEC dbo.CA_GenPatterns_CPU @NewPatternsInBatch;
              
END TRY
BEGIN CATCH
    IF @@trancount > 0 ROLLBACK TRANSACTION
    EXEC error_handler_sp
    RETURN 55555
END CATCH   
         
END;
GO
