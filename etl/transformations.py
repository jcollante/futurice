import sqlite3

def transform(cleanse, db_name):
    """
    Transform data in DB
    """
    conn = set_connection(db_name)
    if conn is not None:
        # Drop tables
        drop_tables(conn)
        # Create tables
            # create_comp_services_table(conn)
            # create_ict_gdp_table(conn)
        create_countries_table(conn)
        create_time_table(conn)
        create_countries_gdp_table(conn)
        create_fact_attractiveness_office_table(conn)
        # Load Tables

        
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

def execute_sql(conn, create_table_sql):
    """ 
    create a table from the create_table_sql statement
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Exception as e:
        print(e)

def execute_sql(conn, execute_sql_sql):
    """ 
    create a table from the execute_sql_sql statement
    """
    try:
        c = conn.cursor()
        c.execute(execute_sql_sql)
    except Exception as e:
        print(e)


def create_comp_services_table(conn):
    """
    Create table computing services
    """
    sql = """CREATE TABLE IF NOT EXISTS comp_services_prod (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        unit TEXT,
        sizen_r2 TEXT,
        indic_is TEXT,
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
        nace_r2 TEXT,
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
    sql = """CREATE TABLE IF NOT EXISTS fact_attractiveness_office_prod (
        time INTEGER,
        country INTEGER,
        countries_gdp REAL,
        ict_gdp REAL,
        comp_services_usage REAL,
        FOREIGN KEY(time) REFERENCES time_prod(id),
        FOREIGN KEY(country) REFERENCES countries_prod(id)
    );"""
    execute_sql(conn, sql)

def drop_tables(conn):
    """
    Drop all tables
    """
    conn.execute("DROP TABLE IF EXISTS comp_services;")
    conn.execute("DROP TABLE IF EXISTS ict_gdp;")
    conn.execute("DROP TABLE IF EXISTS countries;")
    conn.execute("DROP TABLE IF EXISTS time;")
    conn.execute("DROP TABLE IF EXISTS countries_gdp;")
    conn.execute("DROP TABLE IF EXISTS fact_attractiveness_office;")
    conn.commit()

def load_table_comp_services(conn):
    """
    Load table from staging
    """
    sql = """INSERT INTO comp_services
            SELECT unit, sizen_r2, indic_is, geo, CAST(time AS INTEGER), CAST(value AS REAL)
            FROM comp_services
    );"""
    execute_sql(conn, sql)


def load_table_ict_gdp(conn):
    """
    Load table from staging
    """
    sql = """INSERT INTO ict_gdp
            SELECT geo, nace_r2, CAST(time AS INTEGER), CAST(value AS REAL)
            FROM comp_services
    );"""
    execute_sql(conn, sql)


def load_table_countries(conn):
    """
    Load table from staging
    """
    pass


def load_table_time(conn):
    """
    Load table from staging
    """
    pass


def load_table_countries_gdp(conn):
    """
    Load table from staging
    """
    pass

def load_table_fact_attractiveness_office(conn):
    """
    Load table from staging
    """
    pass