import argparse


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--run", help="Run the app", action="store_true")
    parser.add_argument("-i", "--index", help="Index the data", action="store_true")
    parser.add_argument("-s", "--setup", help="Run data setup", action="store_true")
    return parser
