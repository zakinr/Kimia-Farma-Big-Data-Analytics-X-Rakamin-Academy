-- Create a new analytical table named 'kf_analysis' to consolidate data from four source tables.
-- The table is partitioned by 'date' (for faster filtering by time)
-- and clustered by 'provinsi' and 'branch_id' (to optimize query performance on those dimensions).
CREATE TABLE `project-rakamin-477002.dataset.kf_analysis`
PARTITION BY date
CLUSTER BY provinsi, branch_id AS

-- Step 1: Combine all related datasets into a single temporary source table.
WITH src AS (
  SELECT
    ft.transaction_id,                        -- Unique identifier for each transaction
    CAST(ft.date AS DATE) AS date,            -- Convert date string to DATE type for partitioning
    ft.branch_id,                             -- Branch ID where the transaction occurred
    kc.branch_name,                           -- Branch name from branch master data
    kc.kota,                                  -- City name
    kc.provinsi,                              -- Province name
    kc.rating AS rating_cabang,               -- Branch rating
    ft.customer_name,                         -- Name of the customer
    ft.product_id,                            -- Product ID involved in the transaction
    p.product_name,                           -- Product name from product master data
    CAST(p.price AS NUMERIC) AS actual_price, -- Actual product price
    CAST(ft.discount_percentage AS FLOAT64) AS discount_percentage, -- Discount applied to the transaction
    ft.rating AS rating_transaksi,            -- Rating given by customer for the transaction
    inv.opname_stock                          -- Stock quantity from inventory table
  FROM `project-rakamin-477002.dataset.kf_final_transaction` ft
  -- Join with product data to enrich transaction details
  JOIN `project-rakamin-477002.dataset.kf_product` p
    ON ft.product_id = p.product_id
  -- Join with branch data to get branch info and rating
  JOIN `project-rakamin-477002.dataset.kf_kantor_cabang` kc
    ON ft.branch_id = kc.branch_id
  -- Left join with inventory to attach available stock data
  LEFT JOIN `project-rakamin-477002.dataset.kf_inventory` inv
    ON ft.branch_id = inv.branch_id
   AND ft.product_id = inv.product_id
)

-- Step 2: Select and compute derived metrics for analysis
SELECT
  transaction_id, date, branch_id, branch_name, kota, provinsi,
  rating_cabang, customer_name, product_id, product_name,
  actual_price, discount_percentage,

  -- Calculate gross profit percentage based on product price tiers
  CASE
    WHEN actual_price <= 50000 THEN 0.10
    WHEN actual_price > 50000 AND actual_price <= 100000 THEN 0.15
    WHEN actual_price > 100000 AND actual_price <= 300000 THEN 0.20
    WHEN actual_price > 300000 AND actual_price <= 500000 THEN 0.25
    WHEN actual_price > 500000 THEN 0.30
    ELSE 0.0
  END AS persentase_gross_laba,

  -- Compute net sales after discount
  ROUND(actual_price * (1 - discount_percentage/100), 2) AS nett_sales,

  -- Compute net profit = net sales × gross profit percentage
  ROUND(
    (actual_price * (1 - discount_percentage/100)) *
    CASE
      WHEN actual_price <= 50000 THEN 0.10
      WHEN actual_price > 50000 AND actual_price <= 100000 THEN 0.15
      WHEN actual_price > 100000 AND actual_price <= 300000 THEN 0.20
      WHEN actual_price > 300000 AND actual_price <= 500000 THEN 0.25
      WHEN actual_price > 500000 THEN 0.30
      ELSE 0.0
    END, 2
  ) AS nett_profit,

  rating_transaksi                           -- Transaction rating for customer satisfaction analysis
FROM src;
