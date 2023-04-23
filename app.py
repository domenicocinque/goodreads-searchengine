from pathlib import Path

from flask import Flask, render_template, request

from src.dataset import BookDataset
from src.indexer import SearchEngine

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def main():
    if request.method == "POST":
        query = request.form["query"]
        results = search(query)
        return render_template("index.html", results=results)

    return render_template("index.html")


def search(query: str):
    DATA_DIR = Path("data")
    INDEX_NAME = "index"

    indexer = SearchEngine(data_dir=DATA_DIR, dataset=BookDataset(DATA_DIR), index_name=INDEX_NAME)
    results = indexer.search(query)
    return results


if __name__ == "__main__":
    app.run()
