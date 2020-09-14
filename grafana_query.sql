SELECT
  "timestamp" AS "time",
  AVG(extract (epoch from (processedat - "timestamp") )) OVER(
      ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) as delta_prices
  
FROM prices
WHERE
  $__timeFilter("timestamp")
ORDER BY 1