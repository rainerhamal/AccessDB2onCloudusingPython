# Task 1: Import the ibm_db Python library
# The ibm_db API provides a variety of useful Python functions for accessing and manipulating data in an IBM® data server database, including functions for connecting to a database, preparing and issuing SQL statements, fetching rows from result sets, calling stored procedures, committing and rolling back transactions, handling errors, and retrieving metadata.
import ibm_db

# These libraries are pre-installed in SN Labs. If running in another environment please uncomment lines below to install them:
# !pip install --force-reinstall ibm_db==3.1.0 ibm_db_sa==0.3.3
# Ensure we don't load_ext with sqlalchemy>=1.4 (incompadible)
# !pip uninstall sqlalchemy==1.4 -y && pip install sqlalchemy==1.3.24
# !pip install ipython-sql


# Task 2: Identify the database connection credentials (located in config.py for security added in gitignore)
# Connecting to dashDB or DB2 database requires the following information:
#Replace the placeholder values with the actuals for your Db2 Service Credentials
# dsn_driver = "{IBM DB2 ODBC DRIVER}"
# dsn_database = "database"            # e.g. "BLUDB"
# dsn_hostname = "hostname"            # e.g.: "dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net"
# dsn_port = "port"                    # e.g. "50000" 
# dsn_protocol = "protocol"            # i.e. "TCPIP"
# dsn_uid = "username"                 # e.g. "abc12345"
# dsn_pwd = "password"                 # e.g. "7dBZ3wWt9XN6$o0J"
# dsn_security = "SSL"              #i.e. "SSL"
from config import dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid , dsn_pwd, dsn_security


# Task 3: Create the database connection
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};"
).format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid , dsn_pwd, dsn_security)

try:
    conn = ibm_db.connect(dsn, "", "")
    print("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)
except:
    print("Unable to connect: ", ibm_db.conn_errormsg())


# Task 4: Create a table in the database
# Let's first drop the table Instructor in case it exists from a previous attempt
dropQuery = "drop table INSTRUCTOR"

# Now execute the drop statement
drpStmt = ibm_db.exec_immediate(conn, dropQuery)
# Dont worry if you get this error:
# If you see an exception/error similar to the following, indicating that INSTRUCTOR is an undefined name, that's okay. It just implies that the INSTRUCTOR table does not exist in the table - which would be the case if you had not created it previously.
# Exception: [IBM][CLI Driver][DB2/LINUXX8664] SQL0204N "ABC12345.INSTRUCTOR" is an undefined name. SQLSTATE=42704 SQLCODE=-204

# Construct the Create Table DDl statement -replace the... with trest of the statement
createQuery = "create table INSTRUCTOR(ID INTEGER PRIMARY KEY NOT NULL, FNAME VARCHAR(20), LNAME VARCHAR(20), CITY VARCHAR(20), CCODE CHAR(20))"
createStmt = ibm_db.exec_immediate(conn, createQuery)


# # Task 5: Insert data into the table
# # Construct the query - replace ... with the insert statement
insertQuery = "insert into INSTRUCTOR values(1, 'Rav', 'Ahuja', 'TORONTO', 'CA')"
insertStmt = ibm_db.exec_immediate(conn, insertQuery)

# # Now use a single query to insert the remaining two rows of data
insertQuery2 = "insert into INSTRUCTOR values(2, 'Raul', 'Chong', 'MARKHAM', 'CA'), (3, 'Hima', 'Vasudevan', 'CHICAGO', 'US')"
insertStmt2 = ibm_db.exec_immediate(conn, insertQuery2)


# Task 6: Query data in the table
# Construct the query that retrieves all rows from the INSTRUCTOR table
selectQuery = "select * from INSTRUCTOR"

# Execute the statement
selectStmt = ibm_db.exec_immediate(conn, selectQuery)

# Fetch the Dictionary (for the first row only) - replace ... with your code
print(ibm_db.fetch_both(selectStmt))

# Fetch the rest of the rows and print the ID and FNAME for those rows
while ibm_db.fetch_row(selectStmt) != False:
    print("ID: ", ibm_db.result(selectStmt, 0), "FNAME: ", ibm_db.result(selectStmt, "FNAME"))


# write and execute an update statement that changes the Rav's CITY to MOOSETOWN
updateQuery = "update INSTRUCTOR set CITY = 'MOOSETOWN' where FNAME = 'Rav'"
updateStmt = ibm_db.exec_immediate(conn, updateQuery)

while ibm_db.fetch_row(selectStmt) != False:
    print("ID: ", ibm_db.result(selectStmt, 0), "FNAME: ", ibm_db.result(selectStmt, "FNAME"), "CITY: ", ibm_db.result(selectStmt, "CITY"))


# Task 7: Retrieve data into Pandas
import pandas
import ibm_db_dbi

# connection for pandas
pconn = ibm_db_dbi.Connection(conn)


# query statement to retrieve all rows in INSTRUCTOR table
selectQuery2 = "select * from INSTRUCTOR"

# retrieve the query results into a pandas dataframe
pdf = pandas.read_sql(selectQuery2, pconn)

# print just the LNAME for first row in the pandas dataframe
print("LNAME for first row in the pandas dataframe: ", pdf.LNAME[0])

# print the netire dataframe
print("Entire dataframe: \n", pdf)

# Once the data is in a Pandas dataframe, you can do the typical pandas operations on it. For example you can use the shape method to see how many rows and columns are in the dataframe
print("Applying Shape Method: \n", pdf.shape)

# Task 8: Close the Connection
ibm_db.close(conn)