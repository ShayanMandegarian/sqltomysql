# SQLtoMySQL.py

# Usage: Run script while apache2 server is running the required pt_admin project.
#        Transfers data from MSSQL to MySQL via pypyodbc, requests, and slim
#        This is the 'client side'

# import the needed packages
import pypyodbc
import gc
import requests
import time

# beg = time.clock() # uncomment to allow showing time spent while program runs

s = requests.Session() # creates a constant http connection
site = "http://localhost/pt_admin/server/index.php/chunk" #the correct URL for adding a user to MySQL via slim

# Credentials for SQL Server go here
connection = pypyodbc.connect('Driver={SQL Server Native Client 11.0};'
                              'Server=localhost;'
                              'Database=ppt;'
                              'trusted_connection=yes;'
                              'uid=root;'  # this is the SQL user name
                              'pwd=**********') # this is the SQL password

# This creates the cursor for SQL and the command needed to get the drivers
cursor = connection.cursor()

getMax = "SELECT COUNT(*) FROM largeset"  # finds how many rows are in SQL table
cursor.execute(getMax)
max = cursor.fetchone()
low = 0
interval = 1000   # change this value to change how many mssql rows are loaded per chunk
high = low + interval

if (high > max[0]):
    high = max[0]
    interval = high

row = []  # row is a list which is required for passing variables to execute
getChunk = "SELECT * FROM largeset order by id offset ? rows fetch next ? rows only" #grabs chunk starting at offset
row.append(low)
row.append(interval) # adds the necessary vaiables to the row list which gives getChunk the correct drivers from SQL db
#print('row = ', row) # uncomment for debug
cursor.execute(getChunk, row)
result = cursor.fetchall() # retrieve first chunk of data before entering loop
#print('result = ', result)
while result:  # this loop will run until there are no more drivers left in SQL

    #print("high = ", high, " low = ", low) # uncomment this for debugging purposes
    #print("result = ", result) #uncomment for debug

    start = time.clock() #uncomment to see how long each interval takes

    sid = [] # prepares empty arrays for each data column
    user = []
    pswd = []
    date = []
    dele = []
    i = 0

    while (i < interval): # run loop <interval> times over the large 2D array 'result'
        sid.append(str(result[0 + i][0]))
        user.append(result[0 + i][1])
        pswd.append(result[0 + i][2]) # seperation of the variables in result[]
        date.append(str(result[0 + i][3]))
        dele.append(str(result[0 + i][4]))
        i = i + 1
        #print('sid[',i,'] = ', sid[i-1], 'user[',i,'] = ', user[i-1]) # comment this to not see progress
        if (sid == None):
            break

    sidstr = ','.join(sid)
    userstr = ','.join(user)
    pswdstr = ','.join(pswd)
    datestr = ','.join(date)
    delestr = ','.join(dele) #turns the arrays into a long string to be passed via post
    args = {'sqlid':sidstr, 'username':userstr, 'passwd':pswdstr, 'date':datestr, 'deleted':delestr} #sends post request

    r = s.post(url = site, data = args) #sends a post request to the URL with the arguments in place
    #data = r.text
    #print("data = ", data, " user = ", user) #uncomment these two for debugging

    del sid
    del user
    del pswd
    del date
    del dele
    del row
    del result
    del args    #clears the memory in the arrays
    if (high == max[0]):
        break

    gc.collect() # deallocates memory no longer in use

    low = low + interval
    high = high + interval
    if (high > max[0]):
        high = max[0]
        intveral = high - low   # commits changes and sets vars for the next loop if there is one
    row = []  # row is a list which is required for passing variables to execute

    getChunk = "SELECT * FROM largeset order by id offset ? rows fetch next ? rows only" #grabs chunk starting at offset
    row.append(low)
    row.append(interval) # adds the necessary vaiables to the row list which gives getChunk the correct drivers from SQL db
    #print('row = ', row) # uncomment for debug
    cursor.execute(getChunk, row)
    result = cursor.fetchall() # result is a list containing id, username, passwd, date, deleted

    #end = (time.clock() - start) #same as below
    #print("Time taken: ", end) #uncomment to see how long each interval takes

# closes all cursors and connections to SQL before printing a finishing statement
connection.close()
cursor.close()
print("finished")
#print("Total time: ", (time.clock() - beg)) #uncomment to view total program run time
