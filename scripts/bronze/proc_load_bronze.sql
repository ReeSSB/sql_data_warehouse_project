/* ======================================================================
   Stored Procedure : bronze.load_bronze
   Purpose          : Load raw data into Bronze Layer tables from CSV files
   Author           : Shashi Bhushan
   Last Modified    : 07th September 2025
   Notes            : 
       - Logs batch start, end, duration, and status in etl_log.bronze_batch_runs
       - Logs per-table load stats in etl_log.bronze_table_runs
       - File paths are parameterized for flexibility
   ====================================================================== */

CREATE OR ALTER PROCEDURE bronze.load_bronze 
    @base_path NVARCHAR(500) -- Base folder path for CSV files
AS
BEGIN
    DECLARE @batch_start_time DATETIME,
            @batch_end_time   DATETIME,
            @batch_id         INT,
            @start_time       DATETIME, 
            @end_time         DATETIME,
            @file_path        NVARCHAR(1000),
            @sql              NVARCHAR(MAX),
            @row_count        INT,
            @table_name       NVARCHAR(200);

    BEGIN TRY
        -- Capture batch start
        SET @batch_start_time = GETDATE();

        -- Insert log row for start
        INSERT INTO etl_log.bronze_batch_runs (batch_start, status)
        VALUES (@batch_start_time, 'RUNNING');

        -- Get this batch run id
        SET @batch_id = SCOPE_IDENTITY();

        PRINT '=================================================';
        PRINT ' STARTING BRONZE LAYER LOAD PROCESS ';
        PRINT ' Batch Start Time: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
        PRINT '=================================================';

        /* ============================================================== 
           Helper: Load Table Procedure Block
        ============================================================== */
        DECLARE @proc_template NVARCHAR(MAX) = '
            BEGIN TRY
                SET @start_time = GETDATE();
                TRUNCATE TABLE {TABLE_NAME};

                SET @sql = N''
                    BULK INSERT {TABLE_NAME}
                    FROM ''''{FILE_PATH}''''
                    WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);'';
                EXEC sp_executesql @sql;

                SET @end_time = GETDATE();
                SELECT @row_count = COUNT(*) FROM {TABLE_NAME};

                INSERT INTO etl_log.bronze_table_runs
                (batch_id, table_name, start_time, end_time, duration_sec, row_count, status)
                VALUES
                (@batch_id, ''{TABLE_NAME}'', @start_time, @end_time,
                 DATEDIFF(SECOND, @start_time, @end_time), @row_count, ''SUCCESS'');
            END TRY
            BEGIN CATCH
                INSERT INTO etl_log.bronze_table_runs
                (batch_id, table_name, start_time, status, error_message)
                VALUES
                (@batch_id, ''{TABLE_NAME}'', GETDATE(), ''FAILED'', ERROR_MESSAGE());
            END CATCH;
        ';

        /* ============================================================== 
           CRM TABLES 
        ============================================================== */
        PRINT '--------------------------------------------------';
        PRINT ' LOADING CRM TABLES ';
        PRINT '--------------------------------------------------';

        -- crm_cust_info
        SET @table_name = 'bronze.crm_cust_info';
        SET @file_path = @base_path + '\source_crm\cust_info.csv';
        EXEC sp_executesql REPLACE(REPLACE(@proc_template, '{TABLE_NAME}', @table_name), '{FILE_PATH}', @file_path),
             N'@batch_id INT, @start_time DATETIME, @end_time DATETIME, @sql NVARCHAR(MAX), @row_count INT OUTPUT',
             @batch_id=@batch_id, @row_count=@row_count OUTPUT;

        -- crm_prd_info
        SET @table_name = 'bronze.crm_prd_info';
        SET @file_path = @base_path + '\source_crm\prd_info.csv';
        EXEC sp_executesql REPLACE(REPLACE(@proc_template, '{TABLE_NAME}', @table_name), '{FILE_PATH}', @file_path),
             N'@batch_id INT, @start_time DATETIME, @end_time DATETIME, @sql NVARCHAR(MAX), @row_count INT OUTPUT',
             @batch_id=@batch_id, @row_count=@row_count OUTPUT;

        -- crm_sales_details
        SET @table_name = 'bronze.crm_sales_details';
        SET @file_path = @base_path + '\source_crm\sales_details.csv';
        EXEC sp_executesql REPLACE(REPLACE(@proc_template, '{TABLE_NAME}', @table_name), '{FILE_PATH}', @file_path),
             N'@batch_id INT, @start_time DATETIME, @end_time DATETIME, @sql NVARCHAR(MAX), @row_count INT OUTPUT',
             @batch_id=@batch_id, @row_count=@row_count OUTPUT;

        /* ============================================================== 
           ERP TABLES 
        ============================================================== */
        PRINT '--------------------------------------------------';
        PRINT ' LOADING ERP TABLES ';
        PRINT '--------------------------------------------------';

        -- erp_cust_az12
        SET @table_name = 'bronze.erp_cust_az12';
        SET @file_path = @base_path + '\source_erp\CUST_AZ12.csv';
        EXEC sp_executesql REPLACE(REPLACE(@proc_template, '{TABLE_NAME}', @table_name), '{FILE_PATH}', @file_path),
             N'@batch_id INT, @start_time DATETIME, @end_time DATETIME, @sql NVARCHAR(MAX), @row_count INT OUTPUT',
             @batch_id=@batch_id, @row_count=@row_count OUTPUT;

        -- erp_loc_a101
        SET @table_name = 'bronze.erp_loc_a101';
        SET @file_path = @base_path + '\source_erp\LOC_A101.csv';
        EXEC sp_executesql REPLACE(REPLACE(@proc_template, '{TABLE_NAME}', @table_name), '{FILE_PATH}', @file_path),
             N'@batch_id INT, @start_time DATETIME, @end_time DATETIME, @sql NVARCHAR(MAX), @row_count INT OUTPUT',
             @batch_id=@batch_id, @row_count=@row_count OUTPUT;

        -- erp_px_cat_g1v2
        SET @table_name = 'bronze.erp_px_cat_g1v2';
        SET @file_path = @base_path + '\source_erp\PX_CAT_G1V2.csv';
        EXEC sp_executesql REPLACE(REPLACE(@proc_template, '{TABLE_NAME}', @table_name), '{FILE_PATH}', @file_path),
             N'@batch_id INT, @start_time DATETIME, @end_time DATETIME, @sql NVARCHAR(MAX), @row_count INT OUTPUT',
             @batch_id=@batch_id, @row_count=@row_count OUTPUT;

        /* ============================================================== 
           END OF PROCESS 
        ============================================================== */
        SET @batch_end_time = GETDATE();

        -- Update batch log for success
        UPDATE etl_log.bronze_batch_runs
        SET batch_end = @batch_end_time,
            duration_sec = DATEDIFF(SECOND, @batch_start_time, @batch_end_time),
            status = 'SUCCESS'
        WHERE batch_id = @batch_id;

        PRINT '=================================================';
        PRINT ' BRONZE LAYER LOAD COMPLETED SUCCESSFULLY ';
        PRINT ' Batch End Time: ' + CONVERT(NVARCHAR, @batch_end_time, 120);
        PRINT ' Total Batch Duration: ' 
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '=================================================';

    END TRY
    BEGIN CATCH
        DECLARE @error_msg NVARCHAR(4000) = ERROR_MESSAGE();
        SET @batch_end_time = GETDATE();

        UPDATE etl_log.bronze_batch_runs
        SET batch_end = @batch_end_time,
            duration_sec = DATEDIFF(SECOND, @batch_start_time, @batch_end_time),
            status = 'FAILED',
            error_message = @error_msg
        WHERE batch_id = @batch_id;

        PRINT '=================================================';
        PRINT ' ERROR OCCURRED DURING BRONZE LAYER LOAD ';
        PRINT ' Error Message: ' + @error_msg;
        PRINT '=================================================';
    END CATCH;
END;
GO
