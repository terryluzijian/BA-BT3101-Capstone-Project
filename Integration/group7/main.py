import click
from flask import Flask
from flask import render_template
from flask import request, redirect, url_for
from flask import g, flash
import models as dbHandler
import sqlite3

DATABASE = 'database.db'

app = Flask(__name__)

##### Functions to init database
# Connect to database
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

# Initialize the database
def init_db():
    db = get_db()
    cursor = db.cursor()
    with app.open_resource('schema.sql', mode='r') as f:
        cursor.executescript(f.read())
    db.commit()
    print('Database initiated')
    print('Inserting sample professor db')
    

@app.cli.command('initdb')
def initdb():
    """Creates the database tables."""
    init_db()
    click.echo('Init the db')

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@app.route('/', methods=['POST', 'GET'])
def login():
    if request.method=='POST':
        username = request.form.get('username')
        print(username)
        password = request.form.get('password')
        print(password)
        cur = get_db().cursor()
        statement = 'SELECT username, password FROM users WHERE username = "%s" AND password = "%s"' % (username, password)
        print(statement)
        cur.execute(statement)
        users = cur.fetchall()
        print(users)
        if len(users) == 1:
            return render_template('main_loggedin.html', username=username)
        else:
            print("no user found")
            flash('Your username/password is not valid.')
            return render_template('main.html')
    else:
        return render_template('main.html')

@app.route('/logout/', methods=['GET'])
def logout():
    return redirect(url_for('login'))

if __name__ == '__main__':
    app.run()

