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
            index_dir=current_app.config.get("INDEX_DIR"),
            data_path=current_app.config.get("DATA_PATH"),
            searcher=search_engine,
            vector_size=current_app.config.get("VECTOR_SIZE"),
            metric=current_app.config.get("METRIC")
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
