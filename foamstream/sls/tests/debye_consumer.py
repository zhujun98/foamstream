from foamclient import ZmqConsumer


if __name__ == "__main__":
    with ZmqConsumer(f"tcp://localhost:45454",
                     sock="SUB",
                     timeout=1.0) as consumer:
        for _ in range(1000):
            print(consumer.next())
