# **ETL proces datasetu AmazonBooks**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **Chinnok** datasetu. Projekt sa zameriava na preskúmanie správania používateľov a ich čitateľských preferencií na základe hodnotení kníh a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa kníh, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v čitateľských preferenciách, najpopulárnejšie knihy a správanie používateľov.

Zdrojové dáta pochádzajú z Kaggle datasetu dostupného [tu](https://www.kaggle.com/datasets/saurabhbagchi/books-dataset). Dataset obsahuje 11 hlavných tabuliek:
- `playlisttrack`
- `playlist`
- `customer`
- `employee`
- `genre`
- `invoiceline`
- `invoice`
- `track`
- `mediatype`
- `album`
- `artist`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/LxLeoXu/chinnok/blob/main/erd_shcema.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma Chinnok</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:

-**`dim_date`**: Obsahuje informácie o dátumoch predaja, vrátane kalendárnych údajov (rok, štvrťrok, mesiac, týždeň, deň) a kategorizácie dní na pracovné dni a víkendy.
**`dim_customer`**: Obsahuje podrobné údaje o zákazníkoch, ako sú meno, priezvisko, adresa, mesto, štát, krajina, PSČ, telefón a e-mail.
**`dim_product`**: Obsahuje informácie o produktoch vrátane názvu, ID albumu, ID žánru, ID média, ceny, skladateľa, dĺžky skladby (v milisekundách) a veľkosti súboru.
**`dim_employee`**: Zahrňuje údaje o zamestnancoch, ako sú meno, priezvisko, pracovná pozícia, nadriadený, dátumy narodenia a zamestnania, adresa, kontaktné údaje a ďalšie.
**`dim_store`**: Obsahuje údaje o obchodoch vrátane názvu obchodu, mesta, štátu a krajiny.
**`dim_album`**: Obsahuje informácie o albumoch, ako sú názov albumu a ID interpreta.
**`dim_artist`**: Zahrňuje informácie o interpretoch, konkrétne názov interpreta.
**`dim_genre`**: Obsahuje kategórie hudobných žánrov.
**`dim_media_type`**: Poskytuje informácie o typoch médií.

Faktová tabuľka: `fact_sales`
Táto tabuľka obsahuje všetky kľúčové metriky predaja, ako je množstvo predaných produktov, príjem, náklady a zisk. Taktiež zahŕňa cudzí kľúč na každú z dimenzionálnych tabuliek pre účely analýzy.

Štruktúra hviezdicového modelu
Hviezdicový model je navrhnutý tak, aby zabezpečil rýchlu a efektívnu analýzu dát. Faktová tabuľka fact_sales je prepojená s viacerými dimenziami prostredníctvom cudzích kľúčov, čo umožňuje jednoduché vytváranie dotazov a analýz.

Diagram modelu:

Faktová tabuľka (`fact_sales`) je v centre modelu.
Dimenzie (`dim_date`, `dim_customer`, `dim_product`, `dim_employee`, `dim_store`, `dim_album`, `dim_artist`, `dim_genre`, `dim_media_type`) sú prepojené priamo s faktovou tabuľkou.

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/LxLeoXu/chinnok/blob/main/star_schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre Chinnok</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostáva z troch hlavných fáz: extrahovanie (Extract), transformácia (Transform) a načítanie (Load). Tento proces bol implementovaný v Snowflake za účelom transformácie zdrojových dát zo staging vrstvy do viacdimenzionálneho modelu, vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Zdrojové dáta vo formáte .csv boli nahrané do Snowflake prostredníctvom interného úložiska my_stage. Toto úložisko slúži na dočasné uloženie dát pred ich spracovaním.

Vytvorenie stage bolo realizované príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE my_stage;
```
Do stage boli následne nahraté súbory obsahujúce údaje o knihách, používateľoch, hodnoteniach, zamestnaniach a úrovniach vzdelania. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO customers_staging
FROM @my_stage/customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

V prípade nekonzistentných záznamov bol použitý parameter `ON_ERROR = 'CONTINUE'`, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.

---
### **3.1 Transfor (Transformácia dát)**

Transformačná fáza zahŕňala čistenie, obohatenie a reorganizáciu dát zo staging tabuliek do finálnych dimenzií a faktovej tabuľky. Tento krok bol nevyhnutný na vytvorenie viacdimenzionálneho modelu, ktorý podporuje efektívnu analýzu.

Dimenzie
`dim_costomer:` Obsahuje údaje o používateľoch vrátane vekových kategórií, pohlavia, zamestnania a vzdelania. Transformácia zahŕňala kategorizáciu veku a priradenie popisov zamestnaní a vzdelania. Táto dimenzia je typu SCD 2, čo znamená, že uchováva historické zmeny:
```sql
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
```
Dimenzia `dim_date` je navrhnutá tak, aby uchovávala informácie o dátumoch hodnotení kníh. Obsahuje odvodené údaje, ako sú deň, mesiac, rok, deň v týždni (v textovom aj číselnom formáte) a štvrťrok. Táto dimenzia je štruktúrovaná tak, aby umožňovala podrobné časové analýzy, ako sú trendy hodnotení podľa dní, mesiacov alebo rokov. Z hľadiska SCD je táto dimenzia klasifikovaná ako SCD Typ 0. To znamená, že existujúce záznamy v tejto dimenzii sú nemenné a uchovávajú statické informácie.

V prípade, že by bolo potrebné sledovať zmeny súvisiace s odvodenými atribútmi , bolo by možné prehodnotiť klasifikáciu na SCD Typ 1 (aktualizácia hodnôt) alebo SCD Typ 2 (uchovávanie histórie zmien). V aktuálnom modeli však táto potreba neexistuje, preto je `dim_date` navrhnutá ako SCD Typ 0 s rozširovaním o nové záznamy podľa potreby.

```sql
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
```
Podobne `dim_books` obsahuje údaje o knihách, ako sú názov, autor, rok vydania a vydavateľ. Táto dimenzia je typu SCD Typ 0, pretože údaje o knihách sú považované za nemenné, napríklad názov knihy alebo meno autora sa nemenia. 

Faktová tabuľka `fact_ratings` obsahuje záznamy o hodnoteniach a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, ako je hodnota hodnotenia a časový údaj.
```sql
INSERT INTO fact_sales (SaleID, DateKey, CustomerKey, ProductKey, EmployeeKey, StoreKey, QuantitySold, Revenue, Cost, Profit)
SELECT
    ROW_NUMBER() OVER (ORDER BY i.InvoiceID) AS SaleID,
    DATE_PART('epoch', i.InvoiceDate) / 86400 AS DateKey,
    c.CustomerID::INT AS CustomerKey,
    il.TrackID::INT AS ProductKey,
    NULL AS EmployeeKey,
    1 AS StoreKey,
    il.Quantity AS QuantitySold,
    il.UnitPrice * il.Quantity AS Revenue,
    (il.UnitPrice * il.Quantity) * 0.6 AS Cost,
    (il.UnitPrice * il.Quantity) - ((il.UnitPrice * il.Quantity) * 0.6) AS Profit
FROM invoices_staging i
JOIN customers_staging c ON i.CustomerID = c.CustomerID
JOIN invoiceline_staging il ON i.InvoiceID = il.InvoiceID;

```

---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS invoices_staging;
DROP TABLE IF EXISTS invoiceline_staging;
DROP TABLE IF EXISTS tracks_staging;
DROP TABLE IF EXISTS albums_staging;
DROP TABLE IF EXISTS artists_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS employees_staging;
```
ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analýzu čitateľských preferencií a správania používateľov, pričom poskytuje základ pre vizualizácie a reporty.

---
## **4 Vizualizácia dát**

Dashboard obsahuje `6 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa kníh, používateľov a hodnotení. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.

<p align="center">
  <img src="https://github.com/LxLeoXu/chinnok/blob/main/chinok_dashboard%20(2).png" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard Chinnok datasetu</em>
</p>

---
### **Graf 1: Predaje podľa krajín**
Stĺpcový graf, kde os X reprezentuje krajiny (Country z dim_store) a os Y celkové tržby (SUM(Revenue) z fact_sales).
Otázka: Ktoré krajiny generujú najvyššie tržby?
Popis: Tento graf zobrazuje celkové tržby za produkty v jednotlivých krajinách. Pomáha identifikovať najvýkonnejšie trhy.

```sql
SELECT
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
```
---
### **Top 10 najpredávanejších produktov**
Graf: Horizontálny barový graf, kde os X zobrazuje predané množstvo (SUM(QuantitySold)) a os Y názvy produktov (Name z dim_product), zoradené podľa predajnosti.
Otázka: Ktoré produkty sú najpopulárnejšie?
Popis: Tento graf ukáže, ktoré produkty sa predávajú najlepšie, čo môže byť užitočné pre rozhodovanie o zásobách alebo propagácii.

```sql
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

```
---
### **Graf 3: Mesačný trend predaja**
Graf: Čiarový graf, kde os X zobrazuje mesiace (Month z dim_date) a os Y celkové tržby (SUM(Revenue) z fact_sales).
Otázka: Aký je sezónny trend predaja?
Popis: Tento graf ukazuje, ako sa predaje menia počas roka, čo môže pomôcť plánovať marketingové kampane a predpovedať predaje.

```sql
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

```
---
### **Graf 4: Predaje podľa hudobných žánrov**
Graf: Koláčový graf, kde jednotlivé sekcie reprezentujú hudobné žánre (Name z dim_genre) a ich podiel na celkových predajoch (SUM(QuantitySold) z fact_sales).
Otázka: Ktoré hudobné žánre sú najobľúbenejšie?
Popis: Tento graf vizualizuje popularitu jednotlivých žánrov, čo môže byť užitočné pre marketing alebo pridanie nových skladieb.

```sql
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
```
---
### **Graf 5:  Predaje podľa hudobných žánrov**
Tento graf by ukazoval, koľko kusov sa predalo z rôznych hudobných žánrov, čo môže byť užitočné na analýzu preferencií zákazníkov.

```sql
SELECT 
    u.occupation AS occupation,
    COUNT(f.fact_ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userId
GROUP BY u.occupation
ORDER BY total_ratings DESC
LIMIT 10;
```
---

 **Autor:** Ján Homola # chinnok
```

Dashboard poskytuje komplexný pohľad na dáta, pričom zodpovedá dôležité otázky týkajúce sa čitateľských preferencií a správania používateľov. Vizualizácie umožňujú jednoduchú interpretáciu dát a môžu byť využité na optimalizáciu odporúčacích systémov, marketingových stratégií a knižničných služieb.

----


