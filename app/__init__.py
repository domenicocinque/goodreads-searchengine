from flask import Flask, render_template, request, Blueprint, current_app
from app.core import get_search_engine

# Define the main blueprint
main = Blueprint('main', __name__)

# Define your route within the blueprint
@main.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        query = request.form["query"]

        # Make sure you import or define get_search_engine somewhere
        se = get_search_engine(current_app.config.get("INDEX_DIR"))
        results = se.search(query=query)
        return render_template("index.html", results=results)
    
    return render_template("index.html")


def create_app(config):
    app = Flask(__name__)
    app.config.from_object(config)

    # Register the blueprint with the app
    app.register_blueprint(main)

    return app