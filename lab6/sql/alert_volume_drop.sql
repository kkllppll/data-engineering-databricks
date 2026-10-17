-- Volume check for the gold sales fact: compares the latest month of order lines
-- with the average of the three months before it. The alert fires when volume_ratio < 0.5.
WITH monthly AS (
  SELECT d.year, d.month, COUNT(*) AS line_count
  FROM dbr_dev_ua5816bd.elina_sharabura_gold.fact_order_items f
  JOIN dbr_dev_ua5816bd.elina_sharabura_gold.dim_date d ON f.date_key = d.date_key
  GROUP BY d.year, d.month
),
ranked AS (
  SELECT line_count, ROW_NUMBER() OVER (ORDER BY year DESC, month DESC) AS months_ago
  FROM monthly
)
SELECT
  MAX(CASE WHEN months_ago = 1 THEN line_count END)              AS latest_month_rows,
  AVG(CASE WHEN months_ago BETWEEN 2 AND 4 THEN line_count END)  AS baseline_rows,
  ROUND(
    MAX(CASE WHEN months_ago = 1 THEN line_count END)
    / AVG(CASE WHEN months_ago BETWEEN 2 AND 4 THEN line_count END), 2
  )                                                              AS volume_ratio
FROM ranked