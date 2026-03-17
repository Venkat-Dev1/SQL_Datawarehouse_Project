USE Datawarehouse;
GO
CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN
    PRINT 'TRUNCCATING TABLE :Silver.crm_cust_info';

    TRUNCATE TABLE Silver.crm_cust_info; 
    PRINT 'iNSERT DATA INTO TABLE :Silver.crm_cust_info';
    INSERT INTO Silver.crm_cust_info(
   cst_id,
   cst_key,
   cst_firstName,
   cst_lastName,
   cst_material_status,
   cst_gender,
   cst_create_date) 
         SELECT cst_id,
         cst_key,
         TRIM(cst_firstName) AS cst_firstName,
         TRIM(cst_lastName) AS cst_lastName,
         CASE 
            WHEN UPPER(TRIM(cst_material_status))='S' THEN 'Single'
            WHEN UPPER(TRIM(cst_material_status))='M' THEN 'Married'
            ELSE 'Unknown'
         END AS
         cst_material_status,
         CASE 
            WHEN UPPER(TRIM(cst_gender))='M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gender))='F' THEN 'Female'
            ELSE 'Unknown'
         END AS cst_gender,
         cst_create_date
         FROM (
            SELECT *,
               ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_id DESC) AS flag_last
            FROM Bronze.crm_cust_info
         ) AS t WHERE flag_last = 1
         AND cst_id IS NOT NULL;

   PRINT 'TRUNCATE TABLE :Silver.crm_prd_info';
   TRUNCATE TABLE Silver.crm_prd_info;
   PRINT 'INSERT DATA INTO TABLE :Silver.crm_prd_info';
   INSERT INTO Silver.crm_prd_info(
   prd_id, 
   prd_key, 
   cat_id,  
   prd_nm,
   prd_cost,
   prd_line,
   prd_start_dt,
   prd_end_dt)
   SELECT prd_id,
       REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
       REPLACE(SUBSTRING(prd_key, 7, LEN(prd_key)), '-', '_') AS prd_key,
       prd_nm,
       ISNULL(prd_cost, 0) AS prd_cost,
       CASE 
            WHEN UPPER(prd_line) ='M' THEN 'Mountain'
            WHEN UPPER(prd_line) ='R' THEN 'Road'
            WHEN UPPER(prd_line) ='S' THEN 'Standard'   
            WHEN UPPER(prd_line) ='T' THEN 'Touring'
            ELSE 'N/a'
       END AS
       prd_line,
       CAST(prd_start_dt AS DATE) AS prd_start_dt,
       CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
   FROM Bronze.crm_prd_info;

   PRINT 'TRUNCATE TABLE : Silver.crm_sales_details';
   TRUNCATE TABLE Silver.crm_sales_details;
   PRINT 'INSERT DATA INTO TABLE : Silver.crm_sales_details';
   INSERT INTO Silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price  
   )

   SELECT 
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)<>8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR(10)) AS DATE)
      END AS sls_order_dt,
      CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)<>8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR(10)) AS DATE)
      END AS sls_ship_dt,
      CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)<>8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR(10)) AS DATE)
      END AS sls_due_dt,
      CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price)
            THEN sls_quantity*ABS(sls_price)
      ELSE sls_sales
      END AS sls_sales,
      sls_quantity,
      CASE WHEN sls_price IS NULL OR sls_price<=0 
            THEN sls_sales/NULLIF(sls_quantity,1)
        ELSE sls_price
      END AS sls_price
   FROM Bronze.crm_sales_details;

   PRINT 'TRUNCATE TABLE : Silver.erp_cust_az12';
   TRUNCATE TABLE Silver.erp_cust_az12;

   PRINT 'INSERT DATA INTO TABLE : Silver.erp_cust_az12';
   INSERT INTO Silver.erp_cust_az12 (cid, bdate, gen) 
   SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
    ELSE cid END AS cid,
    CASE WHEN bdate > GETDATE() THEN NULL
         WHEN DATEDIFF(YEAR,bdate,GETDATE()) >=100 THEN NULL
    ELSE bdate END AS bdate,    
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
         WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
         ELSE 'n/a' 
    END AS gen
   FROM Bronze.erp_cust_az12;

   PRINT 'TRUNCATE TABLE : Silver.erp_loc_a101';
   TRUNCATE TABLE Silver.erp_loc_a101;
   PRINT 'INSERT DATA INTO TABLE : Silver.erp_loc_a101';
   INSERT INTO Silver.erp_loc_a101 (cid, cntry)
   SELECT REPLACE(cid,'-','') AS cid,
       CASE WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
            WHEN TRIM(cntry) IN ('UK') THEN 'United Kingdom'
            WHEN TRIM(cntry) IN ('DE') THEN 'Germany'
            WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'Unknown'
            ELSE TRIM(cntry) END AS cntry
   FROM Bronze.erp_loc_a101;

   PRINT 'TRUNCATE TABLE : Silver.erp_px_cat_g1v2';
   TRUNCATE TABLE Silver.erp_px_cat_g1v2;
   PRINT 'INSERT DATA INTO TABLE : Silver.erp_px_cat_g1v2';
   INSERT INTO Silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
   SELECT id , cat, subcat,maintenance FROM Bronze.erp_px_cat_g1v2;
END;
