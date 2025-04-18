import happybase

class HBaseDao:
    _instance = None  # Class-level attribute for the singleton instance

    @staticmethod
    def get_instance():
        # Static method to get the singleton instance of the class
        if HBaseDao._instance is None:
            HBaseDao()  # Create an instance if it does not exist
        return HBaseDao._instance

    def __init__(self):
        # Initialize the singleton instance
        if HBaseDao._instance is not None:
            raise Exception("This class is a singleton!")  # Prevent multiple instances
        else:
            HBaseDao._instance = self  # Set the singleton instance
            try:
                # Establish the connection to HBase Thrift server
                self.connection = happybase.Connection('ec2-44-205-16-47.compute-1.amazonaws.com', port=9090)
                self.connection.open()  # Open the connection
            except Exception as e:
                # Handle and print any connection errors
                print(f"Error connecting to HBase: {e}")
                raise

    def get_data(self, key, table):
        # Retrieve data from HBase for a given key and table
        try:
            if not isinstance(key, str):
                # Debug information for non-string keys
                print(f"Debug: Key is not a string. Type: {type(key)}, Value: {key}")
                key = str(key)  # Convert the key to string if it's not already
            table = self.connection.table(table)  # Get the HBase table
            row = table.row(bytes(key, 'utf-8'))  # Retrieve the row by key
            if row is None:
                # Return default values if no row is found
                return {
                    'info:UCL': 0,
                    'info:transaction_dt': '',
                    'info:postcode': '',
                    'info:pos_id': '',
                    'info:card_id': '',
                    'info:amount': '',
                    'info:member_id': '',
                    'info:status': ''
                }
            else:
                return row  # Return the retrieved row
        except Exception as e:
            # Handle and print any errors during data retrieval
            print(f"Error retrieving data: {e}")
            return {
                'info:UCL': 0,
                'info:transaction_dt': '',
                'info:postcode': '',
                'info:pos_id': '',
                'info:card_id': '',
                'info:amount': '',
                'info:member_id': '',
                'info:status': ''
            }

    def write_data(self, key, data, table):
        # Write data to HBase for a given key and table
        try:
            if not isinstance(key, str):
                # Debug information for non-string keys
                print(f"Debug: Key is not a string. Type: {type(key)}, Value: {key}")
                key = str(key)  # Convert the key to string if it's not already
            table = self.connection.table(table)  # Get the HBase table
            table.put(bytes(key, 'utf-8'), data)  # Put data into the table
        except Exception as e:
            # Handle and print any errors during data writing
            print(f"Error writing data: {e}")
