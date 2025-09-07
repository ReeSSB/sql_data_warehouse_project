-- Create a schema for logging
CREATE SCHEMA etl_log;
GO

-- Batch-level log
CREATE TABLE etl_log.bronze_batch_runs (
    batch_id       INT IDENTITY(1,1) PRIMARY KEY,
    batch_start    DATETIME NOT NULL,
    batch_end      DATETIME NULL,
    duration_sec   INT NULL,
    status         VARCHAR(20) NOT NULL,
    error_message  NVARCHAR(4000) NULL
);

-- Table-level log (per table load within a batch)
CREATE TABLE etl_log.bronze_table_runs (
    run_id        INT IDENTITY(1,1) PRIMARY KEY,
    batch_id      INT NOT NULL FOREIGN KEY REFERENCES etl_log.bronze_batch_runs(batch_id),
    table_name    NVARCHAR(200) NOT NULL,
    start_time    DATETIME NOT NULL,
    end_time      DATETIME NULL,
    duration_sec  INT NULL,
    row_count     INT NULL,
    status        VARCHAR(20) NOT NULL,
    error_message NVARCHAR(4000) NULL
);
