import argparse
from .greeter import greet


def main():
    parser = argparse.ArgumentParser(description="Greeter CLI")
    parser.add_argument("name", nargs="?", default="Mundo", help="Nome para saudar")
    args = parser.parse_args()
    print(greet(args.name))


if __name__ == "__main__":
    main()
