CREATE OR REPLACE TABLE REFINED."pricesByCurrency" AS

WITH pricesByProduct AS (
  SELECT p."EnglishProductName", AVG(f."UnitPrice") as "UnitPriceSum", c."CurrencyAlternateKey"
  FROM dbo."FactInternetSales" f
  LEFT JOIN dbo."DimCurrency" c
      ON f."CurrencyKey" = c."CurrencyKey"
  LEFT JOIN dbo."DimProduct" p
      ON f."ProductKey" = p."ProductKey"
  GROUP BY p."EnglishProductName", c."CurrencyAlternateKey"
)

SELECT b."CurrencyName", SUM(a."UnitPriceSum") as "UnitPriceSum"
FROM pricesByProduct a
LEFT JOIN DBO."DimCurrency" b
    ON a."CurrencyAlternateKey" = b."CurrencyAlternateKey"
GROUP BY b."CurrencyName";