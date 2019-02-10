import sqlite3
import re
from datetime import date, datetime


connection = sqlite3.connect("chats.db")
cursor = connection.cursor()

def create_tables():

    sql_command = "DROP TABLE IF EXISTS message"
    cursor.execute(sql_command)

    sql_command = """
    CREATE TABLE message (
    user VARCHAR(50),
    message VARCHAR(10000),
    date DATE)
    """
    cursor.execute(sql_command)

def analyse(archivo):
    with open(archivo) as chats:
        numA = 0
        numB = 0
        lines = chats.read()
        a = re.findall(r'^([0-9]+)\/([0-9]+)\/([0-9]+), ([0-9]+):([0-9]+) (AM|PM) - (.*?):((.|\n)*?)(?=^[0-9])', lines, re.M)
        for elem in a:
            month = int(elem[0])
            day = int(elem[1])
            year = int(elem[2])
            hour = int(elem[3])
            if(elem[5] == "PM"):
                hour=(hour+12)%24
            minute = int(elem[4])
            name = elem[6]
            message = elem[7].rstrip()
            sql_command = """ INSERT INTO message(user,message,date) VALUES(?,?,?)"""
            date = datetime(year,month,day,hour,minute)
            cursor.execute(sql_command, (name, message, date))
    connection.commit()


create_tables()
analyse("chats.txt")
cursor.execute("SELECT DISTINCT user FROM message")
print("Users")
results = cursor.fetchall()
for result in results:
    print(result)

print("Data")
sql_command = """
SELECT strftime('%m', date) as valMonth, strftime('%Y', date) as valYear, count(message), user
FROM message
GROUP BY valMonth, ValYear, user
ORDER BY user, valYear, valMonth
"""
#
cursor.execute(sql_command)
results = cursor.fetchall()
for result in results:
    print(result)
