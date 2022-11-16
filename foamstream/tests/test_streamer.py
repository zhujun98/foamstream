from foamstream import Streamer


def test_streamer():
    with Streamer(12345, lambda x: x) as streamer:
        streamer.feed("abc")
