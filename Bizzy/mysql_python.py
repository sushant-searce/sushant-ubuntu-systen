import MySQLdb
list = []

connection = MySQLdb.connect(
                host = 'bizzy-test-database-instance-1.chbcar19iy5o.us-east-1.rds.amazonaws.com',
                user = 'admin',
                passwd = 'password')  # create the connection

cursor = connection.cursor()     # get the cursor


cursor.execute("USE bhuvi") # select the database

cursor.execute("SHOW TABLES")

for (table_name,) in cursor:
    list.append(table_name)
print(list)