import sqlite3 as sql

def insertUser(username, password):
    print("connecting")
    con = sql.connect("database.db")
    print("connected")
    cur = con.cursor()
    print("inserting")
    cur.execute("INSERT INTO users (username,password) VALUES (?,?)", (username,password))
    print("insert committing")
    con.commit()
    print("inserted")
    con.close()

def retrieveUsers(username, password):
    print('retrieving')
    con = sql.connect("database.db")
    cur = con.cursor()
    statement = 'SELECT username, password FROM users WHERE username = "%s" AND password = "%s"' % (username, password)
    print(statement)
    cur.execute(statement)
    users = cur.fetchall()
    con.close()
    return users
