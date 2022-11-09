import argparse

from foamgraph import mkQApp

from gui import FileStreamWindow


def stream_file():
    parser = argparse.ArgumentParser(prog="extra-foam-stream")
    parser.add_argument("--port",
                        help="TCP port to run server on",
                        default="45454")

    args = parser.parse_args()

    app = mkQApp()
    streamer = FileStreamWindow(port=args.port)
    app.exec()


if __name__ == "__main__":
    stream_file()
