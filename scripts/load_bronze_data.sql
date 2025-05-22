/*
==========================================================================
Procedure: bronze.load_bronze

Purpose:
- Load raw data from CSV files into Bronze Layer tables.
- Truncate tables before inserting new data.
- Provide logging messages and error handling during the load process.

Parameters:
- None.

Usage Example:
EXEC bronze.load_bronze;

Notes:
- CSV files must exist at the specified server paths.
- BULK INSERT requires appropriate file access permissions on the server.
==========================================================================
*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME = GETDATE();
    DECLARE @end_time DATETIME;
    DECLARE @duration_seconds INT;

    BEGIN TRY
        RAISERROR('=======================================', 0, 1) WITH NOWAIT;
        RAISERROR('Loading Bronze Layer', 0, 1) WITH NOWAIT;
        RAISERROR('=======================================', 0, 1) WITH NOWAIT;

        RAISERROR('---------------------------------------', 0, 1) WITH NOWAIT;
        RAISERROR('Loading CRM Tables', 0, 1) WITH NOWAIT;
        RAISERROR('---------------------------------------', 0, 1) WITH NOWAIT;

        RAISERROR('>> Truncating Table: bronze.crm_cust_info', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISERROR('>> Inserting Data Into: bronze.crm_cust_info', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.crm_cust_info
        FROM '/tmp/dataset/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        RAISERROR('>> Truncating Table: bronze.crm_prd_info', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.crm_prd_info;

        RAISERROR('>> Inserting Data Into: bronze.crm_prd_info', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.crm_prd_info
        FROM '/tmp/dataset/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        RAISERROR('>> Truncating Table: bronze.crm_sales_detail', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.crm_sales_detail;

        RAISERROR('>> Inserting Data Into: bronze.crm_sales_detail', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.crm_sales_detail 
        FROM '/tmp/dataset/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        RAISERROR('---------------------------------------', 0, 1) WITH NOWAIT;
        RAISERROR('Loading ERP Tables', 0, 1) WITH NOWAIT;
        RAISERROR('---------------------------------------', 0, 1) WITH NOWAIT;

        RAISERROR('>> Truncating Table: bronze.erp_cust_az12', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISERROR('>> Inserting Data Into: bronze.erp_cust_az12', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.erp_cust_az12 
        FROM '/tmp/dataset/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        RAISERROR('>> Truncating Table: bronze.erp_loc_a101', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISERROR('>> Inserting Data Into: bronze.erp_loc_a101', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.erp_loc_a101
        FROM '/tmp/dataset/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        RAISERROR('>> Truncating Table: bronze.erp_px_cat_g1v2', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISERROR('>> Inserting Data Into: bronze.erp_px_cat_g1v2', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/tmp/dataset/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- End time and duration
        SET @end_time = GETDATE();
        SET @duration_seconds = DATEDIFF(SECOND, @start_time, @end_time);

        RAISERROR('---------------------------------------', 0, 1) WITH NOWAIT;
        RAISERROR('Bronze Layer Loaded Successfully.', 0, 1) WITH NOWAIT;
        RAISERROR('Total Duration: %d seconds', 0, 1, @duration_seconds) WITH NOWAIT;
        RAISERROR('=======================================', 0, 1) WITH NOWAIT;
    END TRY
    BEGIN CATCH
        DECLARE @err_num INT = ERROR_NUMBER();
        DECLARE @err_msg NVARCHAR(4000) = ERROR_MESSAGE();

        RAISERROR('=======================================', 0, 1) WITH NOWAIT;
        RAISERROR('ERROR OCCURRED DURING LOADING BRONZE LAYER!', 0, 1) WITH NOWAIT;
        RAISERROR('Error Number: %d', 0, 1, @err_num) WITH NOWAIT;
        RAISERROR('Error Message: %s', 0, 1, @err_msg) WITH NOWAIT;
        RAISERROR('=======================================', 0, 1) WITH NOWAIT;
    END CATCH
END;




EXEC bronze.load_bronze;
