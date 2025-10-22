import mysql.connector
from decouple import config

# Connect to server
cnx = mysql.connector.connect(
    host=config("MYSQL_HOST"),
    database=config("MYSQL_DATABASE"),
    port=3306,
    user=config("MYSQL_USER"),
    password=config("MYSQL_PASSWORD"),
    ssl_ca=config("MYSQL_CERT_PATH")
)

# Get a cursor
cur = cnx.cursor()

# Execute a query
cur.execute("SELECT status, source FROM permit_data WHERE id=10")

# Fetch one result
row = cur.fetchone()
print("Test data: {0}".format(row[0]))

# Close connection
cnx.close()