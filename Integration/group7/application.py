from flask import Flask, render_template, request, redirect, url_for
from helper import get_peer_unis, get_asp_unis
app = Flask(__name__)

##### Main homepage (not signed in)
@app.route('/')
def main():
    return render_template('main.html')

##### About page
@app.route("/about/")
def about():
    return render_template("about.html")

##### Main login page
@app.route('/login/', methods=['POST'])
def login():
    if request.method == 'POST':
        return redirect(url_for('main_loggedin'))

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
def crawler(dep='bme'):
    if len(request.args) > 0:
        dep = request.args['dep']
    return render_template(
        "crawler.html",
        dep = dep,
        peer_unis = get_peer_unis(dep),
        asp_unis = get_asp_unis(dep))

##### Crawler result preview page
@app.route('/crawler/preview', methods=['POST'])
def get_preview():
    dep = request.args['dep']
    selected_peer = request.form.getlist('selected_peer')
    selected_asp = request.form.getlist('selected_asp')
    peer_unis = get_peer_unis(dep)
    asp_unis = get_asp_unis(dep)
    return(render_template(
        'crawler.html', 
        dep=dep,
        peer_unis=get_peer_unis(dep),
        asp_unis=get_asp_unis(dep),
        selected_peer=selected_peer,
        selected_asp=selected_asp))

@app.route('/crawler/crawl', methods=['POST'])
def start_crawler():
    dep = request.form.get('dep')
    selected_peer = request.form.getlist('selected_peer')
    selected_asp = request.form.getlist('selected_asp')
    print(dep)
    print(selected_peer)
    print(selected_asp)
    print("CRAWL!")
    return(render_template(
        'crawler.html',
        dep=dep,
        peer_unis=get_peer_unis(dep),
        asp_unis=get_asp_unis(dep),
        selected_peer=selected_peer,
        selected_asp=selected_asp))

##### Database page
@app.route("/database/")
def database():
    return render_template("database.html")

##### Initialize app
if __name__ == "__main__":
    app.run()
    
