import os
import snowflake.connector
import glob
import datetime
from cryptography.hazmat.primitives import serialization

# Load environment variables
account = os.getenv("SNOWFLAKE_ACCOUNT")
user = os.getenv("SNOWFLAKE_USER")
role = os.getenv("SNOWFLAKE_ROLE")
database = os.getenv("SNOWFLAKE_DATABASE")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
private_key_path = "snowflake_cicd_key.p8"
private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

# Load private key
with open(private_key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=private_key_passphrase.encode(),
    )

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=user,
    account=account,
    role=role,
    warehouse=warehouse,
    database=database,
    private_key=private_key,
)

cursor = conn.cursor()

# Find and sort .sql files
sql_files = sorted(glob.glob("Snowflake/**/*.sql", recursive=True))

# Execute and log each file
for file_path in sql_files:
    try:
        with open(file_path, "r") as f:
            sql_script = f.read()
        cursor.execute(sql_script)
        status = "SUCCESS"
        error_message = None
    except Exception as e:
        status = "FAILURE"
        error_message = str(e)

    filename = os.path.basename(file_path)
    commit_sha = os.getenv("GITHUB_SHA", "manual-run")
    github_actor = os.getenv("GITHUB_ACTOR", "local-user")
    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(f"""
        INSERT INTO {database}.PUBLIC.DEPLOYMENT_HISTORY (
            DEPLOYMENT_TIMESTAMP, FILENAME, COMMIT_SHA, GITHUB_ACTOR, STATUS, ERROR_MESSAGE
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """, (timestamp, filename, commit_sha, github_actor, status, error_message))

cursor.close()
conn.close()
