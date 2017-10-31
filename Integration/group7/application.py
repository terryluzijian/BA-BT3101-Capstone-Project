from flask import Flask, render_template, request, redirect, url_for
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
@app.route('about_loggedin')
def about_loggedin():
    return render_template('about_loggedin.html')

##### Profile page
@app.route("/profile/")
def profile():
    return render_template("profile.html")

##### Crawler page
@app.route("/crawler/")
def crawler():
    return render_template("crawler.html")

##### Database page
@app.route("/database/")
def database():
    return render_template("database.html")

##### Initialize app
if __name__ == "__main__":
    app.run()
    
