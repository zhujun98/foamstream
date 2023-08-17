from foamclient import ZmqConsumer


if __name__ == "__main__":
    with ZmqConsumer(f"tcp://localhost:45454",
                     sock="SUB",
                     timeout=1.0) as consumer:
        print(consumer.next())
