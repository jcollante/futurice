import sqlite3

def load(cleansed_df, db_name):
    """
    Save raw data into the DB
    """
    c = set_connection(db_name)

    for tb_name, df in cleansed_df.items():
        try:
            upload_table(df, c, tb_name)
        except Exception as e:
            print(e)
            return 400
        
    return 200


def set_connection(db_name):
    """
    Return the connection to the db
    """
    return sqlite3.connect(f"{db_name}.db")


def upload_table(df, conn, tb_name):
    """ 
    upload to a database
    """
    df.to_sql(tb_name, con=conn, index=False, if_exists='replace')