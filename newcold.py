import sys, os, gzip, shutil, logging
from logging.handlers import RotatingFileHandler
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError, AzureError

# Get $SPLUNK_HOME from environment variables
splunk_home = os.getenv('SPLUNK_HOME')
if not splunk_home:
    sys.exit("Environment variable SPLUNK_HOME is not set. Exiting.")

# Set the logging path to $SPLUNK_HOME/var/log/splunk/cold_to_frozen.log
log_file_path = os.path.join(splunk_home, 'var', 'log', 'splunk', 'cold_to_frozen.log')

# Set up logging with rotation and timestamps
log_handler = RotatingFileHandler(log_file_path, maxBytes=10*1024*512, backupCount=1)  # 10MB file size limit
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  # Adding timestamp to logs
log_handler.setFormatter(formatter)

logging.basicConfig(level=logging.INFO, handlers=[log_handler])

# Azure Blob Storage configuration
AZURE_CONNECTION_STRING = 'your-azure-connection-string'
CONTAINER_NAME = 'splunk-archive-container'

# Initialize Azure Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

# Function to extract index name from the bucket path
def get_index_name_from_bucket(bucket_path):
    parts = bucket_path.split(os.sep)
    if len(parts) >= 5:
        index_name = parts[-3]
        logging.info(f"Extracted index name from bucket path: {index_name}")
        return index_name
    else:
        logging.error("Unable to extract index name from bucket path. Exiting.")
        sys.exit(1)

# Upload the archived bucket to Azure Blob Storage
def upload_to_azure(file_path, blob_name):
    try:
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Successfully uploaded {file_path} to Azure Blob Storage as {blob_name}")
    except ResourceExistsError:
        logging.warning(f"Blob {blob_name} already exists. Skipping upload.")
    except AzureError as e:
        logging.error(f"Failed to upload {file_path}. Error: {str(e)}")

# For new-style buckets (v4.2+), we can remove all files except for the rawdata.
def handleNewBucket(base, files):
    logging.info('Archiving bucket: ' + base)
    for f in files:
        full = os.path.join(base, f)
        if os.path.isfile(full):
            os.remove(full)

# For buckets created before 4.2, simply gzip the tsidx files
def handleOldBucket(base, files):
    logging.info('Archiving old-style bucket: ' + base)
    for f in files:
        full = os.path.join(base, f)
        if os.path.isfile(full) and (f.endswith('.tsidx') or f.endswith('.data')):
            with open(full, 'rb') as fin, gzip.open(full + '.gz', 'wb') as fout:
                fout.writelines(fin)
            os.remove(full)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit('usage: python coldToFrozenTest.py /opt/splunk/var/lib/splunk/frozentest/colddb [--search-files-required]')

    bucket = sys.argv[1]

    # Log the provided bucket argument
    logging.info(f"Bucket provided: {bucket}")

    # Ensure the bucket name doesn't start with 'rb_'
    if os.path.basename(bucket).startswith('rb_'):
        logging.info(f"Skipping bucket {bucket} as it starts with 'rb_'")
        sys.exit(0)

    # Ensure the bucket directory is valid
    if not os.path.isdir(bucket):
        logging.error('Given bucket is not a valid directory: ' + bucket)
        sys.exit(1)

    # Extract index name from the bucket path
    index_name = get_index_name_from_bucket(bucket)

    # Use $SPLUNK_HOME for the archive directory
    #ARCHIVE_DIR = os.path.join(splunk_home, 'var', 'lib', 'splunk', index_name, 'colddb')
    
    # Use below archive directory if splunk_db variable is not set
    ARCHIVE_DIR = os.path.join('/coldvolume/splunkdb/splunk', index_name, 'colddb')
    AZURE_CONTAINER_FOLDER = f'frozen-buckets/{index_name}/'

    # Log the constructed paths
    logging.info(f"Archive directory: {ARCHIVE_DIR}")
    logging.info(f"Azure Blob Container folder: {AZURE_CONTAINER_FOLDER}")

    # Create archive directory if it doesn't exist
    if not os.path.isdir(ARCHIVE_DIR):
        try:
            os.makedirs(ARCHIVE_DIR, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to create archive directory {ARCHIVE_DIR}. Error: {str(e)}")
            sys.exit(1)

    # Handle the bucket files
    rawdatadir = os.path.join(bucket, 'rawdata')
    if not os.path.isdir(rawdatadir):
        logging.error('No rawdata directory, given bucket is likely invalid: ' + bucket)
        sys.exit(1)

    files = os.listdir(bucket)
    journal = os.path.join(rawdatadir, 'journal.gz')
    searchFilesRequired = '--search-files-required' in sys.argv

    if os.path.isfile(journal) and not searchFilesRequired:
        handleNewBucket(bucket, files)
    else:
        handleOldBucket(bucket, files)

    # Move the bucket to the archive directory
    destdir = os.path.join(ARCHIVE_DIR, os.path.basename(bucket))
    try:
        shutil.move(bucket, destdir)
        logging.info(f"Bucket {bucket} archived to {destdir}")
    except Exception as e:
        logging.error(f"Failed to move bucket {bucket} to {destdir}. Error: {str(e)}")
        sys.exit(1)

    # Upload the archived bucket to Azure Blob Storage
    for root, dirs, files in os.walk(destdir):
        for file in files:
            file_path = os.path.join(root, file)
            blob_name = os.path.join(AZURE_CONTAINER_FOLDER, os.path.relpath(file_path, ARCHIVE_DIR))
            upload_to_azure(file_path, blob_name)
