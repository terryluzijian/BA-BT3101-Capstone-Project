from flask import Flask, render_template
app = Flask(__name__)

@app.route("/")
def main():
    return render_template("main.html")

@app.route("/about/")
def about():
    return render_template("about.html")

@app.route("/main_loggedin/")
def main_loggedin():
	return render_template("main_loggedin.html")

@app.route("/profile/")
def profile():
	return render_template("profile.html")

@app.route("/crawler/")
def crawler():
	return render_template("crawler.html")

@app.route("/database/")
def database():
	return render_template("database.html")

if __name__ == "__main__":
    app.run()
    
