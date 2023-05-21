from argparse import ArgumentParser
from app import create_app
from config import config 

def main(args):
    if args.run:
        app = create_app(config)
        app.run(debug=True)
    else:
        print("No command specified. Use --help to see available commands.")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run the web app.",
    )
    args = parser.parse_args()
    main(args)
