# SQLtoMySQLOLD.py

# Usage: Ensure that the correct credentials for SQL Server and MySQL are correct,
#        and that the stored procedure spCreateUser has been saved into your
#        MySQL database. Then, simply run the script and all the drivers from
#        SQL Server will be transfered to MySQL.

# import the needed packages
import pypyodbc
from flask import Flask
from flask_restful import Resource, Api
from flask_restful import reqparse
from flaskext.mysql import MySQL
import gc
import requests

# Create mysql and flask instances
mysql = MySQL()
app = Flask(__name__)

# Credentials for MySQL go here

app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = ''
app.config['MYSQL_DATABASE_DB'] = 'ppt'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'

mysql.init_app(app)
api = Api(app)

# Credentials for SQL Server go here
connection = pypyodbc.connect('Driver={SQL Server Native Client 11.0};'
                              'Server=localhost;'
                              'Database=ppt;'
                              'trusted_connection=yes;'
                              'uid=root;'  # this is the SQL user name
                              'pwd=********') # this is the SQL password

# This creates the cursor for SQL and the command needed to get the drivers
cursor = connection.cursor()

getMin = "SELECT min(id) FROM largeset"
cursor.execute(getMin)
min = cursor.fetchone()
getMax = "SELECT max(id) FROM largeset"
cursor.execute(getMax)
max = cursor.fetchone()
low = 0
interval = 500
high = low + interval

if (high > max[0]):
    high = max[0]
    interval = high

# Creates a mysql connection and cursor
conn = mysql.connect()
mycursor = conn.cursor()
while high <= max[0]:  # this loop will run until there are no more drivers left in SQL

    row = []  # row is a list which is required for passing variables to execute

    getChunk = "SELECT * FROM largeset order by id offset ? rows fetch next ? rows only"

    row.append(low)
    row.append(interval) # adds i to the row list which gives getRow the correct driver from SQL db
    print('row = ', row)
    cursor.execute(getChunk, row)
    result = cursor.fetchall() # result is a list containing s(ql)id, user(name), and p(a)sswd
    #print('result = ', result)
    i = 0
    while (i < interval):
        #print('loop')
        sid = result[0 + i][0]
        user = result[0 + i][1]
        pswd = result[0 + i][2] # seperation of the variables in result[]
        date = result[0 + i][3]
        dele = result[0 + i][4]
        i = i + 1
        print('sid = ', sid)
        if (sid == None):
            break

        checkDupe = "SELECT count(*) from driver_logins where sqlid = %s" # checks that the MySQL db does not get a duplicate driver
        mycursor.execute(checkDupe, [sid])

        isDupe = mycursor.fetchone()
        iDnum = isDupe[0] #iDnum will be 0 if there is not a duplicate detected

        if(iDnum > 0): # this if statement deletes the first instance of the duplicate to add the newer one instead
            delDupe = "DELETE FROM driver_logins where sqlid = %s"
            mycursor.execute(delDupe, [sid])

        checkName = "SELECT count(*) from driver_logins where username = %s" # checks for a duplicate username, which should almost
        mycursor.execute(checkName, [user])                                  # never happen, mainly for initial testing, but is failsafe
        dupeName = mycursor.fetchone()
        dNnum = dupeName[0]


        if (dNnum > 0): # similarly, this if statement deletes the old duplicate to add the newer version instead
            delName = "DELETE FROM driver_logins where username = %s"
            mycursor.execute(delName, [user])

        addNew = "INSERT INTO driver_logins (`sqlid`, `username`, `passwd`, `date`, `deleted`) values (%s, %s, %s, %s, %s)" #adds the driver to MySQL

        mycursor.execute(addNew, [sid, user, pswd, date, dele]) # note the different execute syntax for flask and pypyodbc
        conn.commit()

        connection.commit()

    del row
    del result
    if (high == max[0]):
        break
    conn.commit()
    connection.commit()
    gc.collect()

    low = low + interval
    high = high + interval
    if (high > max[0]):
        high = max[0]
        intveral = high - low


# closes all cursors and connections to SQL and MySQL after printing a finishing statement
mycursor.close()
conn.commit()
conn.close()
connection.close()
cursor.close()
print("finished")
