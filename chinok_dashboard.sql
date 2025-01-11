
--Graf: Stĺpcový graf, kde os X reprezentuje krajiny (Country z dim_store) a os Y celkové tržby (SUM(Revenue) z fact_sales).
    CONCAT(c.FirstName, ' ', c.LastName) AS CustomerName,
    SUM(f.Revenue) AS TotalRevenue
FROM 
    fact_sales f
JOIN 
    dim_customer c ON f.CustomerKey = c.CustomerKey
GROUP BY 
    CustomerName
ORDER BY 
    TotalRevenue DESC
LIMIT 10;


--Top 10 najpredávanejších produktov
--Graf: Horizontálny barový graf, kde os X zobrazuje predané množstvo (SUM(QuantitySold)) a os Y názvy produktov (Name z dim_product), 
SELECT 
    p.Name AS ProductName,
    SUM(f.QuantitySold) AS TotalQuantitySold
FROM 
    fact_sales f
JOIN 
    dim_product p ON f.ProductKey = p.ProductKey
GROUP BY 
    p.Name
ORDER BY 
    TotalQuantitySold DESC
LIMIT 10;


--Mesačný trend predaja
--Graf: Čiarový graf, kde os X zobrazuje mesiace (Month z dim_date) a os Y celkové tržby (SUM(Revenue) z fact_sales).


SELECT 
    d.Year,
    d.Month,
    SUM(f.Revenue) AS TotalRevenue
FROM 
    fact_sales f
JOIN 
    dim_date d ON f.DateKey = d.DateKey
GROUP BY 
    d.Year, d.Month
ORDER BY 
    d.Year, d.Month;


 --Predaje podľa hudobných žánrov
--Graf: Koláčový graf, kde jednotlivé sekcie reprezentujú hudobné žánre (Name z dim_genre) a ich podiel na celkových predajoch 
SELECT 
    g.Name AS GenreName,
    SUM(f.QuantitySold) AS TotalQuantitySold
FROM 
    fact_sales f
JOIN 
    dim_product p ON f.ProductKey = p.ProductKey
JOIN 
    dim_genre g ON p.GenreID = g.GenreKey
GROUP BY 
    g.Name
ORDER BY 
    TotalQuantitySold DESC;
    

--Graf: Predaje podľa hudobných žánrov
--Tento graf by ukazoval, koľko kusov sa predalo z rôznych hudobných žánrov, čo môže byť užitočné na analýzu preferencií zákazníkov.


SELECT 
    g.Name AS GenreName,
    SUM(f.QuantitySold) AS TotalQuantitySold
FROM 
    fact_sales f
JOIN 
    dim_product p ON f.ProductKey = p.ProductKey
JOIN 
    dim_genre g ON p.GenreID = g.GenreKey
GROUP BY 
    g.Name
ORDER BY 
    TotalQuantitySold DESC;



