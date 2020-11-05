CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS sales;

DROP TYPE IF EXISTS hr.gender CASCADE;
CREATE TYPE hr.gender AS ENUM (
    'Male', 'Female', 'Unknown'
);

DROP TABLE IF EXISTS hr.employee CASCADE;
CREATE TABLE IF NOT EXISTS hr.employee (
    employee_id                SERIAL           PRIMARY KEY,
    employee_name              TEXT             NOT NULL,
    employee_salary            NUMERIC(9, 2)    NOT NULL,
    employee_dob               DATE             NOT NULL,
    date_added                 TIMESTAMP        NOT NULL,
    date_updated               TIMESTAMP        NULL,
    employee_dependents        INTEGER          NOT NULL,
    employee_phone             BIGINT           NOT NULL,
    employee_performance_score DOUBLE PRECISION DEFAULT 0 NOT NULL,
    employee_middle_initial    CHAR             NULL,
    active                     BOOLEAN          DEFAULT TRUE NOT NULL,
    employee_gender            hr.GENDER        DEFAULT 'Unknown'::hr.GENDER NOT NULL,
    quotes                     TEXT[]
);
ALTER TABLE hr.employee OWNER TO marks;
INSERT INTO hr.employee (
    employee_id
,   employee_name
,   employee_salary
,   employee_dob
,   date_added
,   date_updated
,   employee_dependents
,   employee_phone
,   employee_performance_score
,   employee_middle_initial
,   active
,   employee_gender
,   quotes
)
VALUES
    (
        1
    ,   'Mark Stefanovic'
    ,   123456.00
    ,   '2020-10-28'
    ,   '2020-10-28 02:59:25.000000'
    ,   NULL
    ,   0
    ,   6058675309
    ,   9.1
    ,   'E'
    ,   TRUE
    ,   'Male'
    ,   '{''Getter done!''}'
    )
,   (
        2
    ,   'Bill Robinson'
    ,   7436.00
    ,   '1980-10-28'
    ,   '2020-10-21 02:59:25.000000'
    ,   '2020-10-28 04:31:41.000000'
    ,   2
    ,   1234567
    ,   8.4
    ,   'X'
    ,   TRUE
    ,   'Male'
    ,   '{''What does this button do?''}'
    )
;

DROP TABLE IF EXISTS sales.employee_customer CASCADE;
CREATE TABLE sales.employee_customer (
    employee_id INT NOT NULL REFERENCES hr.employee
,   customer_id INT NOT NULL REFERENCES sales.customer
,   date_added TIMESTAMP NOT NULL DEFAULT now()
,   PRIMARY KEY (employee_id, customer_id)
);
ALTER TABLE sales.employee_customer OWNER TO marks;
INSERT INTO sales.employee_customer (employee_id, customer_id)
VALUES
    (1, 1)
,   (1, 2)
;

DROP TABLE IF EXISTS sales.customer CASCADE;
CREATE TABLE IF NOT EXISTS sales.customer (
    customer_id SERIAL PRIMARY KEY
,   customer_first_name TEXT NOT NULL
,   customer_last_name TEXT NOT NULL
,   date_added TIMESTAMP NOT NULL DEFAULT now()
,   date_updated TIMESTAMP NULL
);
ALTER TABLE sales.customer OWNER TO marks;
INSERT INTO sales.customer (
    customer_id
,   customer_first_name
,   customer_last_name
,   date_added
)
VALUES
    (1, 'Amy', 'Adamant', CAST('2010-01-02 03:04:05' AS TIMESTAMP))
,   (2, 'Billy', 'Bob', CAST('2010-02-03 04:05:06' AS TIMESTAMP))
,   (3, 'Chris', 'Claus', CAST('2010-04-05 06:07:08' AS TIMESTAMP))
,   (4, 'Dan', 'Danger', CAST('2010-09-10 11:12:13' AS TIMESTAMP))
,   (5, 'Eric', 'Eerie', CAST('2010-04-15 06:17:18' AS TIMESTAMP))
,   (6, 'Fred', 'Finkle', CAST('2010-09-20 01:22:23' AS TIMESTAMP))
,   (7, 'George', 'Goose', CAST('2010-04-25 06:27:28' AS TIMESTAMP))
,   (8, 'Mandie', 'Mandelbrot', CAST('2010-09-30 01:32:33' AS TIMESTAMP))
,   (9, 'Steve', 'Smith', CAST('2010-04-05 06:37:38' AS TIMESTAMP))
;





