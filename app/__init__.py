from flask import Blueprint, Flask, current_app, render_template, request

from app.core.search_engine import get_search_engine

# Define the main blueprint
main = Blueprint("main", __name__)


# Define your route within the blueprint
@main.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        query = request.form["query"]
        search_engine = request.form["search_engine"]

        se = get_search_engine(
            searcher=search_engine,
            config=current_app.config,
        )
        results = se.search(query=query)
        return render_template("index.html", results=results)

    return render_template("index.html")


def create_app(config):
    app = Flask(__name__)
    app.config.from_object(config)

    # Register the blueprint with the app
    app.register_blueprint(main)

    return app
