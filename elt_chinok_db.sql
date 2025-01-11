-- Vytvoření databáze a schématu
CREATE DATABASE IF NOT EXISTS Chinook_ET_DB;
CREATE SCHEMA IF NOT EXISTS Chinook_ET_DB.staging;

USE SCHEMA Chinook_ET_DB.staging;
use database chinook_et_db;

-- Staging tabulky
CREATE OR REPLACE TABLE customers_staging (
    CustomerID varchar(50) PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Company VARCHAR(50),
    Address VARCHAR(100),
    City VARCHAR(100),
    State VARCHAR(10),
    Country VARCHAR(50),
    PostalCode VARCHAR(40),
    Phone VARCHAR(50),
    Fax VARCHAR(50),
    Email VARCHAR(100),
    SupportRepId VARCHAR(10)
);

CREATE OR REPLACE TABLE playlist_staging (
    PlaylistID varchar(20) PRIMARY KEY,
    Name VARCHAR(50)
);

CREATE OR REPLACE TABLE playlisttrack_staging (
    PlaylistID VARCHAR(25),
    TrackID VARCHAR(25),
    PRIMARY KEY (PlaylistID, TrackID)
);

CREATE OR REPLACE TABLE employees_staging (
    EmployeeId VARCHAR(30) PRIMARY KEY,
    LastName VARCHAR(50),
    FirstName VARCHAR(50),
    Title VARCHAR(50),
    ReportsTo VARCHAR(20) null,
    BirthDate DATE null,
    HireDate DATE null,
    Address VARCHAR(100),
    City VARCHAR(50),
    State VARCHAR(50),
    Country VARCHAR(50),
    PostalCode VARCHAR(20),
    Phone VARCHAR(50),
    Fax VARCHAR(50),
    Email VARCHAR(100)
);

CREATE OR REPLACE TABLE invoices_staging (
    InvoiceId INT PRIMARY KEY,
    CustomerId INT,
    InvoiceDate DATETIME,
    BillingAddress VARCHAR(100),
    BillingCity VARCHAR(50),
    BillingState VARCHAR(50),
    BillingCountry VARCHAR(50),
    BillingPostalCode VARCHAR(20),
    Total DECIMAL(10, 2)
);

CREATE OR REPLACE TABLE invoiceline_staging (
    InvoiceLineId VARCHAR(30) PRIMARY KEY,
    InvoiceId VARCHAR(30),
    TrackId VARCHAR(30),
    UnitPrice DECIMAL(10, 2),
    Quantity INT
);

CREATE OR REPLACE TABLE tracks_staging (
    TrackID VARCHAR(30) PRIMARY KEY,
    Name VARCHAR(200),
    AlbumID VARCHAR(30),
    MediaTypeID VARCHAR(30),
    GenreID VARCHAR(30),
    Composer VARCHAR(255),
    Milliseconds INT,
    Bytes INT,
    UnitPrice DECIMAL(10, 2)
);

CREATE OR REPLACE TABLE albums_staging (
    AlbumID VARCHAR(30) PRIMARY KEY,
    Title VARCHAR(200),
    ArtistID VARCHAR(30)
);

CREATE OR REPLACE TABLE artists_staging (
    ArtistID VARCHAR(30) PRIMARY KEY,
    Name VARCHAR(200)
);

CREATE OR REPLACE TABLE genres_staging (
    GenreID INT PRIMARY KEY,
    Name VARCHAR(100)
); 
CREATE OR REPLACE TABLE mediaType_staging(
    mediaTypeID INT PRIMARY KEY,
    Name varchar(50)
);
CREATE OR REPLACE  STAGE MY_STAGE;

-- COPY INTO příkazy
COPY INTO customers_staging
FROM @my_stage/customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO playlist_staging
FROM @my_stage/playlist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO playlisttrack_staging
FROM @my_stage/playlisttrack.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO employees_staging
FROM @my_stage/employee.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO invoices_staging
FROM @my_stage/invoce.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO invoiceline_staging
FROM @my_stage/invoiceline.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO tracks_staging
FROM @my_stage/track.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO albums_staging
FROM @my_stage/album.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO artists_staging
FROM @my_stage/artist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_staging
FROM @my_stage/genger.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO mediaType_staging
FROM @my_stage/mediatype.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Faktová tabuľka: fact_sales
CREATE OR REPLACE TABLE fact_sales (
    SaleID INT PRIMARY KEY,
    DateKey INT,
    CustomerKey INT,
    ProductKey INT,
    EmployeeKey INT,
    StoreKey INT,
    QuantitySold INT,
    Revenue DECIMAL(10, 2),
    Cost DECIMAL(10, 2),
    Profit DECIMAL(10, 2)
);

-- Dimenzia: dim_date
CREATE OR REPLACE TABLE dim_date (
    DateKey INT PRIMARY KEY,
    CalendarDate DATE,
    Year INT,
    Quarter INT,
    Month INT,
    Week INT,
    Day INT,
    Weekday VARCHAR(20)
);

-- Dimenzia: dim_customer
CREATE OR REPLACE TABLE dim_customer (
    CustomerKey INT PRIMARY KEY,
    FirstName VARCHAR(40),
    LastName VARCHAR(40),
    Company VARCHAR(80),
    Address VARCHAR(100),
    City VARCHAR(50),
    State VARCHAR(50),
    Country VARCHAR(50),
    PostalCode VARCHAR(20),
    Phone VARCHAR(24),
    Email VARCHAR(100)
);

-- Dimenzia: dim_product
CREATE OR REPLACE TABLE dim_product (
    ProductKey INT PRIMARY KEY,
    Name VARCHAR(200),
    AlbumID INT,
    GenreID INT,
    MediaTypeID INT,
    UnitPrice DECIMAL(10, 2),
    Composer VARCHAR(220),
    Milliseconds INT,
    Bytes INT
);

-- Dimenzia: dim_employee
CREATE OR REPLACE TABLE dim_employee (
    EmployeeKey INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Title VARCHAR(50),
    ReportsTo VARCHAR(10) NULL,
    BirthDate VARCHAR(50) NULL,
    HireDate VARCHAR(50) NULL,
    Address VARCHAR(100),
    City VARCHAR(50),
    State VARCHAR(50),
    Country VARCHAR(50),
    PostalCode VARCHAR(20),
    Phone VARCHAR(50),
    Fax VARCHAR(50),
    Email VARCHAR(100)
);

-- Dimenzia: dim_store
CREATE OR REPLACE TABLE dim_store (
    StoreKey INT PRIMARY KEY,
    StoreName VARCHAR(100),
    City VARCHAR(50),
    State VARCHAR(50),
    Country VARCHAR(50)
);

-- Dimenzia: dim_album
CREATE OR REPLACE TABLE dim_album (
    AlbumKey INT PRIMARY KEY,
    Title VARCHAR(200),
    ArtistID INT
);

-- Dimenzia: dim_artist
CREATE OR REPLACE TABLE dim_artist (
    ArtistKey INT PRIMARY KEY,
    Name VARCHAR(120)
);

-- Dimenzia: dim_genre
CREATE OR REPLACE TABLE dim_genre (
    GenreKey INT PRIMARY KEY,
    Name VARCHAR(100)
);

-- Dimenzia: dim_media_type
CREATE OR REPLACE TABLE dim_media_type (
    MediaTypeKey INT PRIMARY KEY,
    Name VARCHAR(120)
);

INSERT INTO dim_customer (CustomerKey, FirstName, LastName, Company, Address, City, State, Country, PostalCode, Phone, Email)
SELECT
    CustomerID::INT AS CustomerKey, 
    FirstName, 
    LastName, 
    Company, 
    Address, 
    City, 
    State, 
    Country, 
    PostalCode, 
    Phone, 
    Email
FROM customers_staging;

-- Dimenzia: dim_product
INSERT INTO dim_product (ProductKey, Name, AlbumID, GenreID, MediaTypeID, UnitPrice, Composer, Milliseconds, Bytes)
SELECT
    TrackID::INT AS ProductKey,
    Name, 
    AlbumID::INT, 
    GenreID::INT, 
    MediaTypeID::INT, 
    UnitPrice, 
    Composer, 
    Milliseconds, 
    Bytes
FROM tracks_staging;

-- Dimenzia: dim_date
INSERT INTO dim_date (DateKey, CalendarDate, Year, Quarter, Month, Week, Day, Weekday)
SELECT
    DATE_PART('epoch', InvoiceDate) / 86400 AS DateKey,
    InvoiceDate AS CalendarDate,
    YEAR(InvoiceDate) AS Year,
    QUARTER(InvoiceDate) AS Quarter,
    MONTH(InvoiceDate) AS Month,
    WEEK(InvoiceDate) AS Week,
    DAY(InvoiceDate) AS Day,
    CASE WHEN DAYOFWEEK(InvoiceDate) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS Weekday
FROM invoices_staging
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

-- Dimenzia: dim_employee
INSERT INTO dim_employee (
    EmployeeKey, FirstName, LastName, Title, ReportsTo, BirthDate, HireDate, Address,
    City, State, Country, PostalCode, Phone, Fax, Email
)
SELECT
    EmployeeId::INT AS EmployeeKey,
    FirstName,
    LastName,
    Title,
    CASE
        WHEN ReportsTo IS NULL THEN 'Continie'
        ELSE ReportsTo::VARCHAR
    END AS ReportsTo,
    CASE
        WHEN BirthDate IS NULL THEN 'Continie'
        ELSE TRY_TO_DATE(BirthDate)::VARCHAR
    END AS BirthDate,
    CASE
        WHEN HireDate IS NULL THEN 'Continie'
        ELSE TRY_TO_DATE(HireDate)::VARCHAR
    END AS HireDate,
    Address,
    City,
    State,
    Country,
    PostalCode,
    Phone,
    Fax,
    Email
FROM employees_staging;

INSERT INTO dim_employee (
    EmployeeKey, FirstName, LastName, Title, ReportsTo, BirthDate, HireDate, Address,
    City, State, Country, PostalCode, Phone, Fax, Email
)
SELECT
    EmployeeId::INT AS EmployeeKey,
    FirstName,
    LastName,
    Title,
    CASE
        WHEN ReportsTo = 'NULL' OR ReportsTo IS NULL THEN 'Continie'
        ELSE ReportsTo::VARCHAR
    END AS ReportsTo,
    CASE
        WHEN BirthDate = 'NULL' OR BirthDate IS NULL THEN 'Continie'
        ELSE TRY_TO_DATE(BirthDate)::VARCHAR
    END AS BirthDate,
    CASE
        WHEN HireDate = 'NULL' OR HireDate IS NULL THEN 'Continie'
        ELSE TRY_TO_DATE(HireDate)::VARCHAR
    END AS HireDate,
    Address,
    City,
    State,
    Country,
    PostalCode,
    Phone,
    Fax,
    Email
FROM employees_staging;



SELECT EmployeeId, BirthDate, HireDate
FROM employees_staging
WHERE BirthDate = 'NULL' OR HireDate = 'NULL' OR BirthDate IS NULL OR HireDate IS NULL;






INSERT INTO dim_album (AlbumKey, Title, ArtistID)
SELECT
    AlbumID::INT AS AlbumKey,
    Title,
    ArtistID::INT
FROM albums_staging;

-- Dimenzia: dim_artist
INSERT INTO dim_artist (ArtistKey, Name)
SELECT
    ArtistID::INT AS ArtistKey,
    Name
FROM artists_staging;


-- Dimenzia: dim_genre
INSERT INTO dim_genre (GenreKey, Name)
SELECT
    GenreID::INT AS GenreKey,
    Name
FROM genres_staging;

-- Dimenzia: dim_media_type
INSERT INTO dim_media_type (MediaTypeKey, Name)
SELECT
    MediaTypeID::INT AS MediaTypeKey,
    Name
FROM mediaType_staging;

-- Faktová tabuľka: fact_sales
INSERT INTO fact_sales (SaleID, DateKey, CustomerKey, ProductKey, EmployeeKey, StoreKey, QuantitySold, Revenue, Cost, Profit)
SELECT
    ROW_NUMBER() OVER (ORDER BY i.InvoiceID) AS SaleID,
    DATE_PART('epoch', i.InvoiceDate) / 86400 AS DateKey,
    c.CustomerID::INT AS CustomerKey,
    il.TrackID::INT AS ProductKey,
    NULL AS EmployeeKey, -- Doplniť, ak je dostupné
    1 AS StoreKey,       -- Predpokladáme, že StoreKey je 1
    il.Quantity AS QuantitySold,
    il.UnitPrice * il.Quantity AS Revenue,
    (il.UnitPrice * il.Quantity) * 0.6 AS Cost, -- Predpokladané náklady
    (il.UnitPrice * il.Quantity) - ((il.UnitPrice * il.Quantity) * 0.6) AS Profit
FROM invoices_staging i
JOIN customers_staging c ON i.CustomerID = c.CustomerID
JOIN invoiceline_staging il ON i.InvoiceID = il.InvoiceID;



-- SCD Typ dimenzií
-- dim_customer: SCD Type 2 (história zákazníkov sa uchováva)
-- dim_product: SCD Type 1 (aktuálne údaje o produktoch)
-- dim_store: SCD Type 1 (aktuálne údaje o obchodoch)
-- dim_date: SCD Type 1 (časové údaje bez zmien)
-- Odstranění staging tabulek
DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS invoices_staging;
DROP TABLE IF EXISTS invoiceline_staging;
DROP TABLE IF EXISTS tracks_staging;
DROP TABLE IF EXISTS albums_staging;
DROP TABLE IF EXISTS artists_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS employees_staging;


 select * from fact_sales;
