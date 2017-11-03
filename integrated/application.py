from flask import Flask, session, render_template, request, redirect, url_for, g, flash, jsonify
import sqlite3
import click
import helper
import pandas as pd
import datetime
import json
from forms import BenchmarkerForm

DATABASE = 'database.db'

app = Flask(__name__)
app.secret_key = 'shhhhh'

##### Functions to init database
# Connect to database
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

def init_db():
    """Initializes the database."""
    db = get_db()
    cursor = db.cursor()
    with app.open_resource('schema.sql', mode='r') as f:
        cursor.executescript(f.read())
    db.commit()
    print('Database initiated')

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

##### Main homepage (not signed in)
@app.route('/')
def main():
    if 'username' in session:
        return render_template('main_loggedin.html', username=session['username'])
    else:
        return render_template('main.html')

##### About page
@app.route("/about/")
def about():
    if 'username' in session:
        return render_template('about_loggedin.html')
    else:
        return render_template("about.html")

##### Login (for main homepage and about page)
@app.route('/login/', methods=['POST'])
def login():
    username = request.form.get('username')
    password = request.form.get('password')
    statement = "SELECT username, password FROM users WHERE username = '%s' AND password = '%s'" % (username, password)
    print(statement)
    
    cursor = get_db().cursor()
    cursor.execute(statement)
    users = cursor.fetchall()
    if len(users) == 1:
        session['username'] = request.form.get('username')
        return redirect(url_for('main'))
    else:
        print('No user found')
        flash('Your username/password is not valid.')
        return redirect(url_for('main'))

##### Logout
@app.route('/logout/')
def logout():
    session.pop('username', None)
    return redirect(url_for('main'))

##### Profile page
@app.route("/profile/")
def profile():
    return render_template("profile.html")

##### Crawler page
@app.route('/crawler/')
# Set default department as bme as it is the first department on the list
# Automatically display the preview of that department
def crawler_preview(dep='bme', length=9):
    if request.args.get('length'):
        length = request.args.get('length', type=int)
    if request.args.get('dep'):
        dep = request.args.get('dep')
    preview = helper.get_preview_json('SAMPLE_JSON.json', dep)[:length]
    in_db_peer = preview[preview['tag'] == 'peer']['university'].unique() 
    in_db_asp =  preview[preview['tag'] == 'aspirant']['university'].unique()
    return render_template(
        "crawler.html",
        dep=dep,
        length=length,
        peer_unis=helper.get_peer_unis(dep),
        asp_unis=helper.get_asp_unis(dep),
        selected_peer=in_db_peer,
        selected_asp=in_db_asp,
        preview=preview)

##### Export crawl database to local directory
@app.route('/crawler/export', methods=['POST'])
def crawler_export():
    dep = request.form.get('dep')
    export = helper.export_db('SAMPLE_JSON.json', dep, '../%s.xlsx' % (dep))
    return redirect(url_for('crawler_preview', dep=dep))

##### Choose universities to be crawled by crawler
@app.route('/crawler/choose_unis')
def crawler_choose_unis():
    dep = request.args['dep']
    return(render_template(
        'crawler_choose_unis.html',
        dep=dep,
        peer_unis = helper.get_peer_unis(dep),
        asp_unis = helper.get_asp_unis(dep)))

##### Initiate crawling process
@app.route('/crawler/crawl', methods=['POST'])
def start_crawler():
    dep = request.form.get('dep')
    selected_peer = request.form.getlist('selected_peer')
    selected_asp = request.form.getlist('selected_asp')
    print(dep)
    print(selected_peer)
    print(selected_asp)
    print("CRAWL!")
    return redirect(url_for(
        'get_crawler_result', 
        dep=dep,
        selected_peer=selected_peer, 
        selected_asp=selected_asp,
        progress=0))

##### Get crawler result
@app.route('/crawler/result')
def get_crawler_result():
    dep = request.args.get('dep')
    selected_peer = request.args.getlist('selected_peer')
    selected_asp = request.args.getlist('selected_asp')
    progress = int(request.args.get('progress'))
    return(render_template(
        'crawler_result.html',
        dep=dep,
        peer_unis=helper.get_peer_unis(dep),
        asp_unis=helper.get_asp_unis(dep),
        selected_peer=selected_peer,
        selected_asp=selected_asp,
        progress=helper.check_crawler_progress(progress)))

##### Database manager page
@app.route("/database/")
def database():
    return render_template("database.html")

@app.route('/database/show')
def retrieve_database():
    dep = request.args.get('dep')
    incomplete = request.args.get('incomplete', type=bool)
    print(incomplete)
    preview = helper.get_preview_json('SAMPLE_JSON.json', dep)
    if incomplete:
        preview = preview[(preview['phd_year'] == 'Unknown') | 
        (preview['phd_school'] == 'Unknown') | 
        (preview['promotion_year'] == 'Unknown') | 
        (preview['text_raw'] == 'Unknown')]
    return render_template('database.html', dep=dep, incomplete=incomplete, preview=preview)

##### Edit database field
# Superficial method which keeps on passing the modified database around
# Need to modify and update database instead
@app.route('/database/edit', methods=['POST'])
def edit_database():
    print(request.form)
    dep = request.form.get('dep')
    incomplete = request.form.get('incomplete', type=bool)
    profile_link = request.form.get('profile_link')
    field = request.form.get('field')
    new_value = request.form.get('new_value')
    preview = helper.get_preview_json('SAMPLE_JSON.json', dep)
    preview.loc[preview['profile_link'] == profile_link, field] = new_value
    print(preview[preview['profile_link'] == profile_link][field])
    return render_template('database.html', dep=dep, incomplete=incomplete, preview=preview)

##### Benchmarker page
@app.route('/benchmarker/')
def benchmarker():
    return render_template('benchmarker.html')

##### Start benchmarker
@app.route('/benchmarker/benchmark', methods=['POST'])
def start_benchmarker():
    form = BenchmarkerForm(request.form)
    if form.validate():
        nus = {
            'name': form.name.data,
            'department': form.department.data,
            'phd_year': form.phd_year.data,
            'phd_school': form.phd_school.data,
            'text_raw': form.text_raw.data,
            'position':form.position.data,
            'metrics': form.metrics.data,
            'promotion_year': datetime.datetime.now().year
        }
        result = helper.get_preview_json('SAMPLE_JSON.json', 'geo')[:50]
        result = pd.concat([result], ignore_index=True)
        result.to_excel('../benchmarker_result.xlsx', index=False)
        return render_template(
            'benchmarker_result.html',
            name=form.name.data,
            department=form.department.data,
            phd_year=form.phd_year.data,
            phd_school=form.phd_school.data,
            text_raw=form.text_raw.data,
            position=form.position.data,
            metrics=form.metrics.data,
            length=20,
            result=result)
    else:
        for field in form:
            if field.errors:
                for err in field.errors:
                    flash('%s: %s' % (field.label.text, err))
        return redirect(url_for('benchmarker'))

##### Initialize app
if __name__ == "__main__":
    app.run()
    
