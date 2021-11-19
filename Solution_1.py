import psycopg2
import psycopg2.extras
import csv


# load csv files to postgresql tables
def load_data_from_csv_to_postgres():
    # Invoking the cursor
    cur = connection.cursor()

    # Truncate tables if the data already exists
    cur.execute("Truncate table raw_data; Truncate table update_data")

    # Loading the raw as well the changes files
    with open(file_path + '\\question1_raw_data.csv', 'r') as f:
        # raw_data_rd = csv.reader(f)
        next(f)  # Skip the header row.

        cur.copy_from(f, 'raw_data', sep=',')

    with open(file_path + '\\question1_new_changes.csv', 'r') as b:
        # new_changes_rd = csv.reader(b)
        next(b)  # Skip the header row.

        cur.copy_from(b, 'update_data', sep=',')

    cur.close()
    connection.commit()


# Query use to update as per the changes.
def update_data():
    # Invoking the cursor
    cur = connection.cursor()
    
    cur.execute("""
               BEGIN; 

                SET LOCAL temp_buffers = '3000MB';
                
                DROP TABLE IF EXISTS RAW_DATA_TEMP;
                CREATE TEMP TABLE RAW_DATA_TEMP AS  
                SELECT ID,col_1,col_2,col_3
                FROM   raw_data   ;
                
                ALTER TABLE RAW_DATA_TEMP SET (FILLFACTOR = 90);
                
                UPDATE RAW_DATA_TEMP
                                SET col_1 = UD.col_1,col_3=UD.col_3
                                FROM  UPDATE_DATA AS UD
                                WHERE RAW_DATA_TEMP.id = UD.id ; 
                
                DROP TABLE RAW_DATA; 
                
                CREATE TABLE RAW_DATA AS
                SELECT * FROM RAW_DATA_TEMP;
                
                COMMIT;
                """)
    cur.close()            
    connection.commit()


if __name__ == '__main__':

    # csv file path
    file_path = r'C:\Users\laksh\Documents\data-engineer-talent-test\data-engineer-talent-test-master\question1_data'

    # connect to DB
    connection = psycopg2.connect(
        host="localhost",
        database="demo",
        user="postgres",
        password="password3"
    )

    # Call the function to load csv files to postgres sql tables
    load_data_from_csv_to_postgres()

    # Function to update as per the changes tables
    update_data()

    connection.close()