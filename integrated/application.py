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
    cur = db.cursor()
    with app.open_resource('schema.sql', mode='r') as f:
        cur.executescript(f.read())
    db.commit()
    print('Database initiated')

def query_db(query, args=(), one=False):
    db = get_db()
    db.row_factory = sqlite3.Row
    cur = db.cursor().execute(query, args)
    rv = cur.fetchall()
    return (rv[0] if rv else None) if one else rv

def insert_db(query, args=()):
    db = get_db()
    cur = db.cursor().execute(query, args)
    db.commit()
    print('Values inserted')

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
    
    user = query_db('select user_id, username from users where username = ? and password = ?', (username, password), one=True)
    if user is not None:
        # store username into session
        session['user_id'] = user['user_id']
        session['username'] = user['username']
        insert_db('insert into activities (activity_timestamp, user_id, activity_name, remark) values (?, ?, ?, ?)',
            (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), user['user_id'], 'login', 'None'))
        return redirect(url_for('main'))
    else:
        flash('Your username/password is not valid.')
        return redirect(url_for('main'))

##### Logout
@app.route('/logout/')
def logout():
    session.pop('user_id', None)
    session.pop('username', None)
    return redirect(url_for('main'))

##### Profile page
@app.route("/profile/")
def profile():
    if 'username' in session:
        user_id = session['user_id']
        full_user = query_db('select username, first_name, last_name, institution, team, email, staff_id from users where user_id = ?', (user_id,), one=True)
        activities = query_db('select activity_timestamp, activity_name, remark from activities where user_id = ? order by datetime(activity_timestamp) desc limit 5', (user_id,))
        benchmarks = query_db('select benchmark_timestamp, name, department, position, metrics from benchmarks where user_id = ? order by datetime(benchmark_timestamp) desc limit 5', (user_id,))
        return render_template("profile.html", user=full_user, activities=activities, benchmarks=benchmarks)
    else:
        return redirect(url_for('main'))


##### Crawler page
@app.route('/crawler/')
# Set default department as bme as it is the first department on the list
# Automatically display the preview of that department
def crawler_preview(dep='bme', length=9):
    if 'username' in session:
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
    else:
        return redirect(url_for('main'))

##### Export crawl database to local directory
@app.route('/crawler/export', methods=['POST'])
def crawler_export():
    if 'username' in session:
        dep = request.form.get('dep')
        export = helper.export_db('SAMPLE_JSON.json', dep, '../%s.xlsx' % (dep))
        insert_db('insert into activities (activity_timestamp, user_id, activity_name, remark) values (?, ?, ?, ?)',
            (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), session['user_id'], 'export database', helper.get_full_name(dep)))
        return redirect(url_for('crawler_preview', dep=dep))
    else:
        return redirect(url_for('main'))

##### Choose universities to be crawled by crawler
@app.route('/crawler/choose_unis')
def crawler_choose_unis():
    if 'username' in session:
        dep = request.args['dep']
        return(render_template(
            'crawler_choose_unis.html',
            dep=dep,
            peer_unis = helper.get_peer_unis(dep),
            asp_unis = helper.get_asp_unis(dep)))
    else:
        return redirect(url_for('main'))

##### Initiate crawling process
@app.route('/crawler/crawl', methods=['POST'])
def start_crawler():
    if 'username' in session:
        dep = request.form.get('dep')
        selected_peer = request.form.getlist('selected_peer')
        selected_asp = request.form.getlist('selected_asp')
        print(dep)
        print(selected_peer)
        print(selected_asp)
        print("CRAWL!")
        insert_db('insert into activities (activity_timestamp, user_id, activity_name, remark) values (?, ?, ?, ?)',
            (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), session['user_id'], 'crawl database', helper.get_full_name(dep)))
        return redirect(url_for(
            'get_crawler_result', 
            dep=dep,
            selected_peer=selected_peer, 
            selected_asp=selected_asp,
            progress=0))
    else:
        return redirect(url_for('main'))

##### Get crawler result
@app.route('/crawler/result')
def get_crawler_result():
    if 'username' in session:
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
    else:
        return redirect(url_for('main'))

##### Database manager page
@app.route("/database/")
def database():
    if 'username' in session:
        return render_template('database.html')
    else:
        return redirect(url_for('main'))

@app.route('/database/show')
def retrieve_database():
    if 'username' in session:
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
    else:
        return redirect(url_for('main'))

##### Edit database field
# Superficial method which keeps on passing the modified database around
# Need to modify and update database instead
@app.route('/database/edit', methods=['POST'])
def edit_database():
    if 'username' in session:
        print(request.form)
        dep = request.form.get('dep')
        incomplete = request.form.get('incomplete', type=bool)
        profile_link = request.form.get('profile_link')
        field = request.form.get('field')
        new_value = request.form.get('new_value')
        preview = helper.get_preview_json('SAMPLE_JSON.json', dep)
        preview.loc[preview['profile_link'] == profile_link, field] = new_value
        insert_db('insert into activities (activity_timestamp, user_id, activity_name, remark) values (?, ?, ?, ?)',
            (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), session['user_id'], 'edit database', helper.get_full_name(dep)))
        return render_template('database.html', dep=dep, incomplete=incomplete, preview=preview)
    else:
        return redirect(url_for('main'))

##### Benchmarker page
@app.route('/benchmarker/')
def benchmarker():
    if 'username' in session:
        return render_template('benchmarker.html')
    else:
        return redirect(url_for('main'))

##### Start benchmarker
@app.route('/benchmarker/benchmark', methods=['POST'])
def start_benchmarker():
    if 'username' in session:
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
            insert_db('insert into activities (activity_timestamp, user_id, activity_name, remark) values (?, ?, ?, ?)',
                (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), session['user_id'], 'benchmark request', form.department.data))
            insert_db(
                'insert into benchmarks (benchmark_timestamp, user_id, name, department, position, metrics) values (?, ?, ?, ?, ?, ?)',
                (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), session['user_id'], 
                    form.name.data, form.department.data, form.position.data, ', '.join(form.metrics.data)))
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
    else:
        return redirect(url_for('main'))

##### Initialize app
if __name__ == "__main__":
    app.run()
    