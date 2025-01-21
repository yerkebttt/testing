import pandas as pd
import clickhouse_connect
import logging
import logging.handlers
import os

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)

# Load secrets
try:
    SOME_SECRET = os.environ["SOME_SECRET"]
except KeyError:
    SOME_SECRET = "Token not available!"
    logger.info("Token not available!")

if __name__ == "__main__":
    try:
        logger.info(f"Token value: {SOME_SECRET}")

        # Step 1: Load the Excel file
        excel_file_path = "Банк эмитент.xlsx"  # Update with your uploaded Excel file name
        csv_file_path = "Банк эмитент.csv"

        logger.info(f"Reading Excel file from {excel_file_path}...")
        data = pd.read_excel(excel_file_path)

        # Step 2: Convert Excel to CSV
        logger.info(f"Converting Excel file to CSV at {csv_file_path}...")
        data.to_csv(csv_file_path, index=False)

        # Step 3: Rename columns to match the ClickHouse table structure
        logger.info("Renaming columns to match ClickHouse table structure...")
        data.rename(columns={
            data.columns[0]: 'bank_name',  # Adjust as per your actual column names
            data.columns[1]: 'simple_name'  # Adjust as per your actual column names
        }, inplace=True)

        # Step 4: Connect to ClickHouse
        logger.info("Connecting to ClickHouse...")
        try:
            client = clickhouse_connect.get_client(
                host='localhost',  # Ensure this matches your local ClickHouse setup
                username='default',  # Replace with your username
                password='Zzxcvbnm1!'  # Replace with your password or leave empty if none
            )
            logger.info("Connected to ClickHouse successfully.")
        except Exception as e:
            logger.error(f"Error connecting to ClickHouse: {e}")
            exit()

        # Step 5: Truncate the existing table
        try:
            logger.info("Truncating existing data in the ClickHouse table...")
            client.command("TRUNCATE TABLE onevision.banks")
            logger.info("Table truncated successfully.")
        except Exception as e:
            logger.error(f"Error during table truncation: {e}")
            exit()

        # Step 6: Insert data into the ClickHouse table
        try:
            logger.info("Preparing data for insertion into ClickHouse...")

            # Convert the DataFrame to a list of tuples
            records = list(data.itertuples(index=False, name=None))

            # Debugging: Print a few records to verify the format
            logger.debug(f"First 5 records to insert: {records[:5]}")

            # Insert data into the ClickHouse table
            logger.info("Inserting data into the 'onevision.banks' table...")
            client.insert(
                table='onevision.banks',  # Specify the correct database and table name
                data=records,
                column_names=['bank_name', 'simple_name']  # Ensure columns match the table schema
            )
            logger.info("Data inserted into the 'banks' table successfully.")
        except Exception as e:
            logger.error(f"Error during data insertion: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
