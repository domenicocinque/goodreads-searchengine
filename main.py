from argparse import ArgumentParser

from app import create_app
from config import config


def main():
    app = create_app(config)
    app.run(debug=True)


if __name__ == "__main__":
    main()
