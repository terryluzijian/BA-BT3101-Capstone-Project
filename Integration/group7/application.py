from flask import Flask, render_template, request, redirect, url_for
import helper
import pandas as pd
app = Flask(__name__)

##### Main homepage (not signed in)
@app.route('/')
def main():
    return render_template('main.html')

##### About page
@app.route("/about/")
def about():
    return render_template("about.html")

##### Login (for main homepage and about page)
@app.route('/login/', methods=['POST'])
def login():
    if request.method == 'POST':
        return redirect(url_for('main_loggedin'))

##### Logout
@app.route('/logout/', methods=['GET'])
def logout():
    return redirect(url_for('main'))

##### Main homepage (signed in)
@app.route("/main_loggedin/")
def main_loggedin():
    return render_template('main_loggedin.html')

##### About page (signed in)
@app.route('/about_loggedin/')
def about_loggedin():
    return render_template('about_loggedin.html')

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
    preview = helper.get_preview_json('SAMPLE_JSON.json', dep)
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

##### Benchmarker page
@app.route('/benchmarker/')
def benchmarker():
    return render_template('benchmarker.html')

##### Initialize app
if __name__ == "__main__":
    app.run()
    
