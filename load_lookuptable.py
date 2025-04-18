import happybase
import pandas as pd

# Initialize HBase connection
connection = happybase.Connection('localhost', port=9090, autoconnect=False)

def open_connection():
    connection.open()

def close_connection():
    connection.close()

def list_tables():
    print("Fetching all tables")
    open_connection()
    tables = connection.tables()
    close_connection()
    print("All tables fetched")
    return tables

def create_table(name, cf):
    print("Creating table " + name)
    tables = list_tables()
    if name.encode('utf-8') not in tables:
        open_connection()
        connection.create_table(name, cf)
        close_connection()
        print("Table created")
    else:
        print("Table already present")

def get_table(name):
    open_connection()
    table = connection.table(name)
    close_connection()
    return table

# Create the lookup table
create_table('look_up_table', {'info': dict(max_versions=5)})

# Load data from the CSV file into a DataFrame
def load_csv_to_dataframe(file_path):
    print(f"Loading data from {file_path}")
    df = pd.read_csv(file_path)
    return df

# To batch insert data from the DataFrame into HBase
def batch_insert_data(df, tableName):
    print("Starting batch insert of events")
    table = get_table(tableName)
    
    open_connection()
    with table.batch(batch_size=4) as b:
        for index, row in df.iterrows():
            b.put(
                bytes(str(row['card_id']), 'utf-8'), {
                    b'info:card_id': bytes(str(row['card_id']), 'utf-8'),
                    b'info:transaction_dt': bytes(str(row['transaction_dt']), 'utf-8'),
                    b'info:score': bytes(str(row['score']), 'utf-8'),
                    b'info:postcode': bytes(str(row['postcode']), 'utf-8'),
                    b'info:UCL': bytes(str(row['UCL']), 'utf-8')
                }
            )
    print("Batch insert done")
    close_connection()

# Path to the CSV file
csv_file_path = '/home/hadoop/look_up_table.csv'

# Load data and insert into HBase
df = load_csv_to_dataframe("/home/hadoop/look_up_table.csv")
batch_insert_data(df, 'look_up_table')

