import happybase

# To create a connection
connection = happybase.Connection('localhost', port=9090, autoconnect=False)

# To open connection and perform operations
def open_connection():
    connection.open()

# To close the opened connection
def close_connection():
    connection.close()

# To list all tables in HBase
def list_tables():
    print("Fetching all tables")
    open_connection()
    tables = connection.tables()
    close_connection()
    print("All tables fetched")
    return tables

# To create the required table
def create_table(name, cf):
    print(f"Creating table {name}")
    tables = list_tables()
    if name.encode('utf-8') in tables:  # Ensure name is in bytes format for comparison
        print(f"Table {name} already exists, deleting table {name}")
        open_connection()
        connection.disable_table(name)
        connection.delete_table(name)
        close_connection()
        print(f"Table {name} deleted")
    
    open_connection()
    connection.create_table(name, cf)
    close_connection()
    print(f"Table {name} created")

# To get the created table
def get_table(name):
    open_connection()
    table = connection.table(name)
    close_connection()
    return table

# To batch insert data
def batch_insert_data(filename, table_name):
    print("Starting batch insert of events")
    with open(filename, "r") as file:
        table = get_table(table_name)
        open_connection()
        i = 0
        for line in file:
            temp = line.strip().split(",")
            # To skip the first row
            if temp[0] != 'card_id':
                table.put(bytes(str(i), 'utf-8'), {
                    'info:card_id': bytes(temp[0], 'utf-8'),
                    'info:member_id': bytes(temp[1], 'utf-8'),
                    'info:amount': bytes(temp[2], 'utf-8'),
                    'info:postcode': bytes(temp[3], 'utf-8'),
                    'info:pos_id': bytes(temp[4], 'utf-8'),
                    'info:transaction_dt': bytes(temp[5], 'utf-8'),
                    'info:status': bytes(temp[6], 'utf-8')
                })
                i += 1
        close_connection()
    print("Batch insert done")

# To batch insert data of card_transactions.csv
create_table('card_transactions', {'info': dict()})
batch_insert_data('/home/hadoop/card_transactions.csv', 'card_transactions')

