import mysql.connector
from decouple import config

print("Connecting to MySQL")
# Connect to server

cnx = mysql.connector.connect(
    host=config("MYSQL_HOST"),
    database=config("MYSQL_DATABASE"),
    port=3306,
    user=config("MYSQL_USER"),
    password=config("MYSQL_PASSWORD"),
    ssl_ca=config("MYSQL_CERT_PATH"),
    collation="utf8mb4_unicode_ci"
)
print("Connected to MySQL")

# Get a cursor
cur = cnx.cursor()

# Execute a query
cur.execute("SELECT * FROM permit_data LIMIT 10")

# Fetch one result
row = cur.fetchone()
print("Test data: {0}".format(row[0]))

# Close connection
cnx.close()