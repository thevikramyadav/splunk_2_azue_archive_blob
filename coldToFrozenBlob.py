from azure.storage.blob import BlobServiceClient
import sys, os, gzip, shutil, logging
from logging.handlers import RotatingFileHandler

# Get $SPLUNK_HOME from environment variables
splunk_home = os.getenv('SPLUNK_HOME')
if not splunk_home:
    sys.exit("Environment variable SPLUNK_HOME is not set. Exiting.")

# Set up logging
log_file_path = os.path.join(splunk_home, 'var', 'log', 'splunk', 'cold_to_frozen.log')
log_handler = RotatingFileHandler(log_file_path, maxBytes=10*1024*512, backupCount=1)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)
logging.basicConfig(level=logging.INFO, handlers=[log_handler])

# **Azure Blob Storage Configuration**
AZURE_STORAGE_ACCOUNT = "your_account_name"
AZURE_STORAGE_KEY = "your_storage_key"
AZURE_CONTAINER_NAME = "your_container_name"

# Create BlobServiceClient
connection_string = f"DefaultEndpointsProtocol=https;AccountName={AZURE_STORAGE_ACCOUNT};AccountKey={AZURE_STORAGE_KEY};EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

def upload_to_blob(file_path, blob_path):
    """Uploads a file to Azure Blob Storage."""
    try:
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        blob_client = container_client.get_blob_client(blob_path)

        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        logging.info(f"Successfully uploaded {file_path} to Azure Blob Storage: {blob_path}")
    except Exception as e:
        logging.error(f"Failed to upload {file_path}. Error: {str(e)}")

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

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit('Usage: python coldToFrozenTest.py /opt/splunk/var/lib/splunk/frozentest/colddb [--search-files-required]')

    bucket = sys.argv[1]
    logging.info(f"Bucket provided: {bucket}")

    if os.path.basename(bucket).startswith('rb_'):
        logging.info(f"Skipping bucket {bucket} as it starts with 'rb_'")
        sys.exit(0)

    if not os.path.isdir(bucket):
        logging.error(f"Given bucket is not a valid directory: {bucket}")
        sys.exit(1)

    index_name = get_index_name_from_bucket(bucket)

    # Set archive directory path
    ARCHIVE_DIR = os.path.join('/coldvolume/splunkdb/splunk', index_name, 'colddb')
    BLOB_FOLDER = f'frozen-buckets/{index_name}/'

    logging.info(f"Archive directory: {ARCHIVE_DIR}")
    logging.info(f"Blob Storage folder: {BLOB_FOLDER}")

    if not os.path.isdir(ARCHIVE_DIR):
        try:
            os.makedirs(ARCHIVE_DIR, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to create archive directory {ARCHIVE_DIR}. Error: {str(e)}")
            sys.exit(1)

    rawdatadir = os.path.join(bucket, 'rawdata')
    if not os.path.isdir(rawdatadir):
        logging.error(f"No rawdata directory, given bucket is likely invalid: {bucket}")
        sys.exit(1)

    files = os.listdir(bucket)
    journal = os.path.join(rawdatadir, 'journal.gz')
    searchFilesRequired = '--search-files-required' in sys.argv

    if os.path.isfile(journal) and not searchFilesRequired:
        for f in files:
            full = os.path.join(bucket, f)
            if os.path.isfile(full):
                os.remove(full)
    else:
        for f in files:
            full = os.path.join(bucket, f)
            if os.path.isfile(full) and (f.endswith('.tsidx') or f.endswith('.data')):
                with open(full, 'rb') as fin, gzip.open(full + '.gz', 'wb') as fout:
                    fout.writelines(fin)
                os.remove(full)

    destdir = os.path.join(ARCHIVE_DIR, os.path.basename(bucket))
    try:
        shutil.move(bucket, destdir)
        logging.info(f"Bucket {bucket} archived to {destdir}")
    except Exception as e:
        logging.error(f"Failed to move bucket {bucket} to {destdir}. Error: {str(e)}")
        sys.exit(1)

    # Upload the archived bucket to Azure Blob Storage
    for root, _, files in os.walk(destdir):
        for file in files:
            file_path = os.path.join(root, file)
            blob_path = os.path.join(BLOB_FOLDER, os.path.relpath(file_path, ARCHIVE_DIR)).replace("\\", "/")
            upload_to_blob(file_path, blob_path)
