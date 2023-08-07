
   
CREATE PROCEDURE tlnd_median_alerts()                                                                                          -- Should this be specific to one alert or a catch-all for all criteria? Maybe log the alert reason into the AlertProcessing table?
RETURNS VARCHAR
LANGUAGE SQL
AS

INSERT INTO AlertProcessing (Unique_ID, LoadJob, RowsLoaded, Status,LoadDateTime, ProcessingStatus)   
    WITH MedianRowsLoadedByJob AS (                                                                                            -- Loads median rows by LoadJob, this bit essentially is for comparison and could be anything.
      SELECT LoadJob, MEDIAN(RowsLoaded) AS RowsLoadedMedian
      FROM FILELOADREFERENCE
       -- WHERE LoadJob = ''
      GROUP BY LoadJob
    ),
    AlertDetail AS (                                                                                                           -- Creates hashes for all entries in FILELOADREFERENCE, aswell as the detail for the alert. 
      SELECT LoadJob, RowsLoaded, LoadDateTime, status, HASH(LoadJob, RowsLoaded, LoadDateTime) as unique_id
      FROM FILELOADREFERENCE f 
        WHERE NOT EXISTS (SELECT 1 FROM AlertProcessing c WHERE c.unique_id = HASH(LoadJob, RowsLoaded, LoadDateTime))
        AND LoadDateTime >= DATEADD(HOUR, -9000, SYSDATE())                                                                    -- This is simply a reference point for the window in which we want alerts to generate, set to 9000 hours for testing. I would set this in a variable in production. 
        AND Status = 'Successful'
   -- AND LoadJob = ''
    )
    SELECT a.unique_id, a.LoadJob, a.RowsLoaded, a.Status, a.LoadDateTime, 'To-Process'                                        -- This is the select into AlertProcessing value from FILELOADR
    FROM AlertDetail a
    JOIN MedianRowsLoadedByJob b ON b.LoadJob = a.LoadJob
    WHERE (a.RowsLoaded > b.RowsLoadedMedian * 1.50 OR a.RowsLoaded < b.RowsLoadedMedian * 0.50);
    
  DECLARE                                                                                                                      -- Declaring all varliables which are then used to bind cursor column values in order to use dynamically in email procedure.
  hashval varchar(100);
  jobname varchar(100);
  loadcount int;
  loaddate TIMESTAMP_NTZ;
  jobstatus varchar(10);
  subject varchar(80);
  body varchar(350);
  
  cur CURSOR FOR                                                                                                               -- Cursor to grab and iterate through rows in the AlertProcessing table. 
  SELECT unique_id, loadjob, loaddatetime, rowsloaded, status 
  FROM ALERTPROCESSING 
  WHERE ProcessingStatus = 'To-Process'; 
   
BEGIN                                                                                                                          -- Beginning of Cursor, 'FOR x' is just an alias to be able to reference values specific to each iteration of the run. IE x.unique_id. 
FOR x in cur DO
    hashval := x.unique_id;
    jobname := x.loadjob;
    loadcount := x.rowsloaded;
    loaddate := x.loaddatetime;
    jobstatus := x.status;


CREATE OR REPLACE NOTIFICATION INTEGRATION INTEGRATION_DBA
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('example@email.com');


CALL SYSTEM$SEND_EMAIL(
    'INTEGRATION_DBA',
    'example@email.com',
    'Example Subject',
    'Email Body'
);




    CALL SYSTEM$SEND_EMAIL(                                                                                                     -- SEND_EMAIL Proc which enables us to push the above information through via email. The body and subject are dynamically populated by bind variables for each loop.
        'de_email',
        'example@email.com',
        '[Talend Alerts] ' || :jobname || ' outside of rows loaded threshold on ' || :loaddate,
        'The job ' || :jobname || ' has ran 50% above or below the median for rows loaded. Loading ' || :loadcount || ' at ' || :loaddate || ' with the status ' || :jobstatus || '. If this was expected, the criteria for failure may need configuring further. This alert was sent from Snowflake (DATABASE.LOG.TLND_MEDIAN_ALERTS())'
    );







    UPDATE DATABASE.LOG.ALERTPROCESSING a SET ProcessingStatus = 'Processed' WHERE a.unique_id = :hashval;                      -- Once sent, the current unique ID for the alert row is flagged as processed. 
       
  END FOR;  -- End Looping Component when the cursor is exhausted.
END;        
       
       /* This is the Alert Processing Table DDL 
       
       create or replace TABLE ALERTPROCESSING ( 
unique_id VARCHAR(128) NOT NULL, 
LOADJOB VARCHAR(128), 
ROWSLOADED NUMBER(38,0), 
STATUS VARCHAR(128), 
LOADDATETIME DATE, 
ROWSLOADEDMEDIAN int,
PROCESSINGSTATUS VARCHAR(128), primary key (unique_id) );

     */

