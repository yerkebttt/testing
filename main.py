import pandas as pd
import clickhouse_connect

# Step 1: Load the CSV file
csv_file_path = r'C:/Users/ybaty/OneDrive - OneVision/Dictionaries/Банк эмитент.csv'  # Update with your file path
data = pd.read_csv(csv_file_path)

# Verify the data structure
print("Columns in the dataset:", data.columns)
print("First 5 rows of the dataset:")
print(data.head())

# Step 2: Rename columns to match the ClickHouse table structure
data.rename(columns={
    data.columns[0]: 'bank_name',  # Adjust as per your actual column names
    data.columns[1]: 'simple_name'  # Adjust as per your actual column names
}, inplace=True)

# Step 3: Connect to ClickHouse
try:
    client = clickhouse_connect.get_client(
        host='localhost',  # Ensure this matches your ClickHouse host
        username='default',  # Replace with your username
        password='Zzxcvbnm1!'  # Leave empty if no password is required
    )
    print("Connected to ClickHouse successfully.")
except Exception as e:
    print("Error connecting to ClickHouse:", e)
    exit()

# Step 4: Insert data into the table
try:
    print("Preparing data for insertion...")
    
    # Convert the DataFrame to a list of tuples
    records = list(data.itertuples(index=False, name=None))
    
    # Debugging: Print a few records to verify the format
    print("First 5 records to insert:", records[:5])
    
    # Insert data into the ClickHouse table
    client.insert(
        table='onevision.banks',  # Specify the correct database and table name
        data=records,  # Use the list of tuples
        column_names=['bank_name', 'simple_name']  # Ensure columns match the table schema
    )
    print("Data inserted into the 'banks' table successfully.")
except Exception as e:
    print("Error during data insertion:", e)
