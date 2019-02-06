# bbg_client.py
import socket
import json

def sender(hostname, port, content):
    """Connect to a TCP port and send content, then wait for reply
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((hostname, port))
        sock.sendall(content)
        print("Sent {}".format(content))
        sock.shutdown(socket.SHUT_WR)
        while 1:
            data = sock.recv(1024)
            if not data:
                break
            print("Received {}".format(data.decode("utf-8")))
        print("Done")

if __name__ == "__main__":
    query = [["XS1084818464 Corp"], ["PX_LAST"]]
    data = json.dumps(query)
    sender("localhost", 2600, data.encode("utf-8"))
