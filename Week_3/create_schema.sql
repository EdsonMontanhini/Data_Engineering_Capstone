-- This script was generated by a beta version of the ERD tool in pgAdmin 4.
-- Please log an issue at https://redmine.postgresql.org/projects/pgadmin4/issues/new if you find any bugs, including reproduction steps.
BEGIN;


CREATE TABLE public."softcartDimDate"
(
    dateid integer NOT NULL,
    date date NOT NULL,
    year integer NOT NULL,
    month integer NOT NULL,
    day integer NOT NULL,
    weekday integer NOT NULL,
    PRIMARY KEY (dateid)
);

CREATE TABLE public."softcartDimCategory"
(
    categoryid integer NOT NULL,
    category character varying NOT NULL,
    PRIMARY KEY (categoryid)
);

CREATE TABLE public."softcartDimItem"
(
    itemid integer NOT NULL,
    itemname character varying NOT NULL,
    categoryid integer NOT NULL,
    PRIMARY KEY (itemid)
);

CREATE TABLE public."softcartDimCountry"
(
    countryid integer NOT NULL,
    country character varying NOT NULL,
    PRIMARY KEY (countryid)
);

CREATE TABLE public."softcartFactSales"
(
    orderid integer NOT NULL,
    itemid integer NOT NULL,
    catgeoryid integer NOT NULL,
    price double precision NOT NULL,
    countryid integer NOT NULL,
    dateid integer NOT NULL,
    PRIMARY KEY (orderid)
);

ALTER TABLE public."softcartFactSales"
    ADD FOREIGN KEY (itemid)
    REFERENCES public."softcartDimItem" (itemid)
    NOT VALID;


ALTER TABLE public."softcartFactSales"
    ADD FOREIGN KEY (catgeoryid)
    REFERENCES public."softcartDimCategory" (categoryid)
    NOT VALID;


ALTER TABLE public."softcartFactSales"
    ADD FOREIGN KEY (dateid)
    REFERENCES public."softcartDimDate" (dateid)
    NOT VALID;


ALTER TABLE public."softcartFactSales"
    ADD FOREIGN KEY (countryid)
    REFERENCES public."softcartDimCountry" (countryid)
    NOT VALID;

END;