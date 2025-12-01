import psycopg #https://www.psycopg.org/psycopg3/docs/basic/usage.html
"""
TODO:
- implement threading or async to speed up the connection and insertion process
 -> threading in psycopg3 would usually be done with connection pool!
 -> might not be
- error handling
    -> timeout handling

Processing is as follows:

-> Transformed and simulated data is ready 
    -> check if db & tables ready
        -> if not create them
    -> connect to the db
    -> insert the data into the appropriate tables
"""


def insert_data(db_host_addr: str, db_port: str, db_name: str, db_user: str, db_password: str, db_timeout: int, data: list[]) -> None: #cool thing to look into is how to make use of the *args and **kwargs in python functions


    try:
        psycopg.connect(f"hostaddr={db_host_addr} port={db_port} dbname={db_name} user={db_user} password={db_password} connect_timeout={db_timeout}").close()
    except psycopg.DatabaseError:
        #creates the database if it does not exist by connecting to the default 'postgres' database
        with psycopg.connect(f"hostaddr={db_host_addr} port={db_port} dbname=postgres user={db_user} password={db_password} connect_timeout={db_timeout}") as conn:
            conn.autocommit = True #this helps as it automatically commits each statement without needing to call conn.commit()!!!
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE {db_name};")
    finally:
        print(f'Connected to database {db_name} successfully!')

    #for refrence on the param kwargs: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    with psycopg.connect(f"hostaddr={db_host_addr} port={db_port} dbname={db_name} user={db_user} password={db_password} connect_timeout={db_timeout}") as conn:
        with conn.cursor() as cur:
            
            #lets put insertion query here then we can print it with the code below
            #here we might have to do a loop over the data for mass insertions



            #this will print out the data to make sure we actually inserted the proper data
            cur.execute("SELECT * from stock_data;")
            print(cur.fetchall()) # can use this method or iterate over the cursor...

            cur.execute("SELECT * from simulation;")
            print(cur.fetchall())

            conn.commit()