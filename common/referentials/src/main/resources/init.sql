CREATE SCHEMA IF NOT EXISTS dbo;
SET SCHEMA dbo;
CREATE TABLE dim_currency (brand_id INTEGER NOT NULL, currency_id INTEGER NOT NULL, currency_description VARCHAR(100) NOT NULL);
INSERT INTO dim_currency(brand_id, currency_id, currency_description)
  VALUES (21, 776, 'EURO'),
         (106, 776, 'CAD'),
         (96, 776, 'CAD'),
         (98, 776, 'CAD'),
         (74, 776, 'CAD');

CREATE TABLE dim_countries (brand_id INTEGER NOT NULL, country_id INTEGER NOT NULL, country_description VARCHAR(100) NOT NULL);
INSERT INTO dim_countries(brand_id, country_id, country_description)
  VALUES (21, 777, 'BAMAKO'),
         (106, 777, 'A'),
         (96, 777, 'A'),
         (74, 777, 'A'),
         (98, 777, 'A');
