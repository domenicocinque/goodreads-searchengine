from typing import List

from flask import Flask, render_template, request

from app.service.dataset import Book
from app.service.search_engine import get_search_engine

app = Flask(__name__)


def search(query: str) -> List[Book]:
    search_engine = get_search_engine()
    return search_engine.search(query)


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        query = request.form["query"]
        results = get_search_engine().search(query)
        return render_template("index.html", results=results)
    return render_template("index.html")


def run():
    app.run(debug=True)
