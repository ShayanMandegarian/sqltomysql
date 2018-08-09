
# deletemysql.py

from flask import Flask
from flask_restful import Resource, Api
from flask_restful import reqparse
from flaskext.mysql import MySQL

mysql = MySQL()
app = Flask(__name__)

# Credentials for MySQL go here
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = ''
app.config['MYSQL_DATABASE_DB'] = 'ppt'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'

mysql.init_app(app)
api = Api(app)

conn = mysql.connect()
cursor = conn.cursor()

try:
    truncate = "truncate table driver_logins"
    cursor.execute(truncate)
    reset = "alter table driver_logins AUTO_INCREMENT = 1"
    cursor.execute(reset)
except:
    print("Truncate failed")

cursor.close()
conn.close()
print("Finished")
