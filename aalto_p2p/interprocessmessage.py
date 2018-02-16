__author__ = 'hukkinj1'


class InterprocessMessage:
    REGISTER = 0  # Register a new connection to main process. Payload: ip and port tuple
    KILL = 1  # Kill the receiving process
    JOIN = 2  # Connect and join to another node. Payload: ip and port tuple
    QUERY = 3  # When getting this from command line, send a query message to neighbors. Payload: query string (bytes)
    PUBLISH = 4  # Payload: (key, value)

    def __init__(self, msg_type, payload=None):
        self.type = msg_type
        self.payload = payload
