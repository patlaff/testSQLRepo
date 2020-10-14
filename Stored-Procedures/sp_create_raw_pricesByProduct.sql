CREATE OR REPLACE TABLE REFINED."pricesByProduct" AS
SELECT p."EnglishProductName", AVG(f."UnitPrice") as "UnitPriceSum", c."CurrencyAlternateKey"
FROM dbo."FactInternetSales" f
LEFT JOIN dbo."DimCurrency" c
    ON f."CurrencyKey" = c."CurrencyKey"
LEFT JOIN dbo."DimProduct" p
    ON f."ProductKey" = p."ProductKey"
GROUP BY p."EnglishProductName", c."CurrencyAlternateKey";