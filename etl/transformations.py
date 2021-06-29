import sqlite3
from sqlite3.dbapi2 import Error

def transform(cleanse, db_name):
    """
    Transform data in DB
    """
    conn = set_connection(db_name)
    if conn is not None:
        # Drop tables
        drop_tables(conn)
        # Create tables
        create_comp_services_table(conn)
        create_ict_gdp_table(conn)
        create_countries_table(conn)
        create_time_table(conn)
        create_countries_gdp_table(conn)
        create_fact_attractiveness_office_table(conn)
        # Load Tables
        load_table_comp_services(conn)
        load_table_ict_gdp(conn)
        load_table_countries(conn)
        load_table_time(conn)
        load_table_countries_gdp(conn)
        load_table_fact_attractiveness_office(conn)

        conn.close()

def set_connection(db_name):
    """
    Return the connection to the db
    """
    conn = None
    try:
        conn = sqlite3.connect(f"{db_name}.db")
        return conn
    except Exception as e:
        print(e)

    return conn

def execute_sql(conn, sql):
    """ 
    create a table from the sql statement
    """
    try:
        c = conn.cursor()
        c.execute(sql)
        return c
    except Exception as e:
        print(e)
    return None

def create_comp_services_table(conn):
    """
    Create table computing services
    """
    sql = """CREATE TABLE IF NOT EXISTS comp_services_prod (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        geo TEXT,
        time INTEGER,
        value REAL
    );"""
    execute_sql(conn, sql)


def create_ict_gdp_table(conn):
    """
    Create table ict gdp
    """
    sql = """CREATE TABLE IF NOT EXISTS ict_gdp_prod (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        geo TEXT,
        time INTEGER,
        value REAL
    );"""
    execute_sql(conn, sql)


def create_countries_table(conn):
    """
    Create table countries
    """
    sql = """CREATE TABLE IF NOT EXISTS countries_prod (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country TEXT
    );"""
    execute_sql(conn, sql)


def create_time_table(conn):
    """
    Create table time
    """
    sql = """CREATE TABLE IF NOT EXISTS time_prod (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        time INTEGER
    );"""
    execute_sql(conn, sql)


def create_countries_gdp_table(conn):
    """
    Create table countries gdp
    """
    sql = """CREATE TABLE IF NOT EXISTS countries_gdp_prod (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        time INTEGER,
        gdp REAL,
        country TEXT
    );"""
    execute_sql(conn, sql)


def create_fact_attractiveness_office_table(conn):
    """
    Create table time
    """
    sql = """CREATE TABLE IF NOT EXISTS fact_country_attractiveness_prod (
        time INTEGER,
        country_id INTEGER,
        country_gdp REAL,
        ict_gdp REAL,
        comp_services_usage REAL,
        country_attractiveness REAL
    );"""
    execute_sql(conn, sql)

def drop_tables(conn):
    """
    Drop all tables
    """
    conn.execute("DROP TABLE IF EXISTS comp_services_prod;")
    conn.execute("DROP TABLE IF EXISTS ict_gdp_prod;")
    conn.execute("DROP TABLE IF EXISTS countries_prod;")
    conn.execute("DROP TABLE IF EXISTS time_prod;")
    conn.execute("DROP TABLE IF EXISTS countries_gdp_prod;")
    conn.execute("DROP TABLE IF EXISTS fact_country_attractiveness_prod;")
    conn.commit()

def load_table_comp_services(conn):
    """
    Load table from staging
    """
    sql = """INSERT INTO comp_services_prod
            (
                geo,
                time,
                value
            )
            SELECT DISTINCT
                geo, 
                time, 
                AVG(
                    case 
                        when value is null then 0
                        else value 
                    end
                ) value
            FROM comp_services
            GROUP BY geo, time
    ;"""
    execute_sql(conn, sql)
    conn.commit()


def load_table_ict_gdp(conn):
    """
    Load table from staging
    """
    sql = """INSERT INTO ict_gdp_prod
            (
                geo, 
                time,
                value
            )
            SELECT DISTINCT
            geo, 
            time, 
            AVG(
                case 
                    when value is null then 0
                    else value 
                end
            ) value
            FROM ict_gdp
            GROUP BY geo, time
    ;"""
    execute_sql(conn, sql)
    conn.commit()


def load_table_countries(conn):
    """
    Load table from staging
    """
    sql = """INSERT INTO countries_prod
            (
                country
            )
            SELECT DISTINCT
            country
            FROM countries_gdp
    ;"""
    execute_sql(conn, sql)
    conn.commit()


def load_table_time(conn):
    """
    Load table from staging
    """
    sql = """INSERT INTO time_prod
            (
                time
            )
            VALUES
            (2008),
            (2009),
            (2010),
            (2011),
            (2012),
            (2013),
            (2014),
            (2015),
            (2016),
            (2017),
            (2018),
            (2019),
            (2020)
    ;"""
    execute_sql(conn, sql)
    conn.commit()


def load_table_countries_gdp(conn):
    """
    Load table from staging
    """
    # Select column names    
    sql_select = "SELECT name FROM PRAGMA_TABLE_INFO('countries_gdp') WHERE name GLOB '*[^A-Za-z]*';" 
    cursor = execute_sql(conn, sql_select)
    rows = cursor.fetchall()

    # Stack gpd on years and country
    for column_name in rows:
        sql = """INSERT INTO countries_gdp_prod
                (
                    time,
                    country,
                    gdp
                )
                SELECT
                {0} as time,
                country,
                REPLACE(
                    case 
                            when "{0}" is null then 0
                            when "{0}" GLOB '*[A-Za-z]*' then 0
                            else "{0}" 
                    end,
                    ',',
                    '.'
                ) gdp
                FROM countries_gdp
        ;""".format(column_name[0])

        execute_sql(conn, sql)
        conn.commit()


def load_table_fact_attractiveness_office(conn):
    """
    Load table from staging
    """

    sql = """INSERT INTO fact_country_attractiveness_prod
            (
                time,
                country_id,
                ict_gdp,
                comp_services_usage,
                country_gdp,
                country_attractiveness
            )
            SELECT DISTINCT
            c.time AS time_id,
            b.id AS country_id,
            IFNULL(d.value,0) AS comp_ser_usage,
            IFNULL(e.value,0) AS ict_gdp,
            IFNULL(a.gdp,0) AS country_gdp,
            IFNULL(d.value,0) * IFNULL(e.value,0) * IFNULL(a.gdp,0) as country_attractiveness

            FROM countries_gdp_prod a
            LEFT JOIN countries_prod b
                ON LOWER(a.country) = LOWER(b.country)
            LEFT JOIN time_prod c
                ON a.time = c.time
            LEFT JOIN comp_services_prod d
                ON (a.time = d.time AND LOWER(a.country) = LOWER(d.geo))
            LEFT JOIN ict_gdp_prod e
                ON (a.time = e.time AND LOWER(a.country) = LOWER(e.geo))

    ;"""
    execute_sql(conn, sql)
    conn.commit()

def get_list_country_attractiveness(db_name):
    """
    Select list of countries with attractiveness metric
    """
    conn = set_connection(db_name)
    if conn is not None:
        # Select column names    
        sql_select = """SELECT 
            a.time,
            b.country,
            a.ict_gdp,
            a.comp_services_usage,
            a.country_gdp,
            a.country_attractiveness
        FROM fact_country_attractiveness_prod a
        LEFT JOIN countries_prod b
            ON LOWER(a.country_id) = LOWER(b.id)
        ;"""

        try:
            cursor = execute_sql(conn, sql_select)
            rows = cursor.fetchall()
        except Error as e:
            return {
                'request_data': {}
            }

        data = [{
            'time': time, \
            'country': country, \
            'ict_gdp': ict_gdp, \
            'comp_ser_usage': comp_services_usage, \
            'country_gdp': country_gdp, \
            'country_attractiveness': country_attractiveness} \
            for time, country, ict_gdp, comp_services_usage, country_gdp, country_attractiveness in rows]

        conn.close()    

        return {
            'request_data': data
        }

        