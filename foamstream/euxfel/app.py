import argparse

from foamgraph import mkQApp

from .gui import FileStreamWindow


def application():
    parser = argparse.ArgumentParser(prog="foamstream-euxfel")
    parser.add_argument("--port",
                        help="TCP port to run server on",
                        default="45454")

    args = parser.parse_args()

    app = mkQApp()
    streamer = FileStreamWindow(port=args.port)
    app.exec()


if __name__ == "__main__":
    application()
