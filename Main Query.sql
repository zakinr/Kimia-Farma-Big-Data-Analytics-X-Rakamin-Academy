CREATE TABLE `project-rakamin-477002.dataset.kf_analysis`
PARTITION BY date
CLUSTER BY provinsi, branch_id AS
WITH src AS (
  SELECT
    ft.transaction_id,
    CAST(ft.date AS DATE) AS date,
    ft.branch_id,
    kc.branch_name,
    kc.kota,
    kc.provinsi,
    kc.rating AS rating_cabang,
    ft.customer_name,
    ft.product_id,
    p.product_name,
    CAST(p.price AS NUMERIC) AS actual_price,
    CAST(ft.discount_percentage AS FLOAT64) AS discount_percentage,
    ft.rating AS rating_transaksi,
    inv.opname_stock
  FROM `project-rakamin-477002.dataset.kf_final_transaction` ft
  JOIN `project-rakamin-477002.dataset.kf_product` p
    ON ft.product_id = p.product_id
  JOIN `project-rakamin-477002.dataset.kf_kantor_cabang` kc
    ON ft.branch_id = kc.branch_id
  LEFT JOIN `project-rakamin-477002.dataset.kf_inventory` inv
    ON ft.branch_id = inv.branch_id
   AND ft.product_id = inv.product_id
)
SELECT
  transaction_id, date, branch_id, branch_name, kota, provinsi,
  rating_cabang, customer_name, product_id, product_name,
  actual_price, discount_percentage,
  CASE
    WHEN actual_price <= 50000 THEN 0.10
    WHEN actual_price > 50000 AND actual_price <= 100000 THEN 0.15
    WHEN actual_price > 100000 AND actual_price <= 300000 THEN 0.20
    WHEN actual_price > 300000 AND actual_price <= 500000 THEN 0.25
    WHEN actual_price > 500000 THEN 0.30
    ELSE 0.0
  END AS persentase_gross_laba,
  ROUND(actual_price * (1 - discount_percentage/100), 2) AS nett_sales,
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
  rating_transaksi
FROM src;