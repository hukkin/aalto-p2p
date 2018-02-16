from random import randint
import select
import socket
import struct
import sys
import os
import multiprocessing
import queue
import signal
from aalto_p2p.interprocessmessage import InterprocessMessage
from timer.timer import Timer
from networking.networking import create_listen_socket, ip_string_to_int, int_to_ip_string, connect, recvall

# Official specification
VERSION = 1
HEADER_LENGTH = 16  # bytes
RESERVED_VALUE = 0
JOIN_RESPONSE_PAYLOAD_LEN = 2  # bytes
RESOURCE_VALUE_LEN = 4  # bytes
PING_INTERVAL = 5  # seconds
PORT_DEFAULT = 6346

TYPE_PING = 0
TYPE_PONG = 1
TYPE_BYE = 2
TYPE_JOIN = 3
TYPE_QUERY = 128
TYPE_QUERY_HIT = 129

JOIN_ACC = 512


# Taneli's own constants
MSG_ID_MAPPING_LIMIT = 500


class Node:

    # Taneli's own constants
    MAIN_THREAD_MAX_CYCLE_TIME = 0.1  # seconds. The maximum time that main thread accept call can block

    def __init__(self, ip, port=PORT_DEFAULT):
        self.ip = ip  # Only in string IPv4 form, e.g. "192.168.10.105". No DNS names here. No IPv6.
        self.port = port

        self.listen_socket = None
        self.commandline = None

        self.connections = []
        self.candidate_connections = []  # waiting for join response or sending join response

        # These data containers will be shared among all processes
        self.manager = multiprocessing.Manager()
        self.data = {}
        self.ip_addresses = self.manager.list()
        self.message_id_mapping = RoundRobinList(MSG_ID_MAPPING_LIMIT) # List of (msg_id, addr) tuples.

    def start(self):

        try:
            signal.signal(signal.SIGTERM, self.sigterm_handler)
            self.__start_node()
        # Handle Ctrl+C from user, sys.exit from code
        except (KeyboardInterrupt, SystemExit):
            for process in multiprocessing.active_children():
                process.terminate()
                process.join()
            print("Node stopped.")

    def __start_node(self):
        new_stdin = os.fdopen(os.dup(sys.stdin.fileno()))
        self.commandline = CommandlineReader(sys.stdin)
        self.commandline = CommandlineReader(new_stdin)
        self.commandline.start()
        new_stdin.close()

        self.listen_socket = create_listen_socket(self.ip, self.port)
        if self.listen_socket is None:
            sys.exit("Failed to open listen socket to given ip and port combination.")
        self.listen_socket.settimeout(Node.MAIN_THREAD_MAX_CYCLE_TIME)

        while True:
            self.clean_terminated_connections()
            self.handle_commandline()
            self.handle_conn_write_queues()

            try:
                sock, _ = self.listen_socket.accept()  # Blocking call (for MAIN_THREAD_MAX_CYCLE_TIME). Implement select here for improved performance
                conn = Connection(sock, (self.ip, self.port), self.ip_addresses)
                self.candidate_connections.append(conn)
                conn.start()
            except socket.timeout:
                pass

    def sigterm_handler(self, _signo, _stack_frame):
        raise SystemExit

    def handle_commandline(self):
        try:
            while True:
                message = self.commandline.write_queue.get_nowait()
                if message.type == InterprocessMessage.JOIN:
                    conn = Connection(None, (self.ip, self.port), self.ip_addresses, message.payload)
                    self.candidate_connections.append(conn)
                    conn.start()
                elif message.type == InterprocessMessage.QUERY:
                    #  TODO: if you have this key self, print the value
                    body = message.payload
                    header = Header(TYPE_QUERY, self.ip, self.port, len(body), Header.MAX_TTL)
                    message_to_deliver = Message(header, body)
                    self.message_id_mapping.add((header.msg_id, (self.ip, self.port)))
                    self.deliver_message(message_to_deliver)
                elif message.type == InterprocessMessage.PUBLISH:
                    self.data[message.payload[0]] = struct.unpack("!L", message.payload[1])[0]
                    process_print("Data published. Key: " + str(message.payload[0]) + "  Value: " + str(message.payload[1]))
        except queue.Empty:
            pass

    def clean_terminated_connections(self):

        # Clean up terminated connections
        for conn in self.connections[:]:
            if not conn.is_alive():
                self.connections.remove(conn)
                conn.join()
                self.ip_addresses.remove(conn.other_end_listen_addr)
                print("Cleaned a terminated connection")

        # Clean up terminated candidate connections
        for conn in self.candidate_connections[:]:
            if not conn.is_alive():
                self.candidate_connections.remove(conn)
                conn.join()
                print("Cleaned a terminated candidate connection")

    def handle_conn_write_queues(self):
        for conn in self.candidate_connections[:]:
            try:
                while True:
                    message = conn.write_queue.get_nowait()
                    if message.type == InterprocessMessage.REGISTER:
                        self.candidate_connections.remove(conn)
                        conn.other_end_listen_addr = message.payload
                        self.connections.append(conn)
                        self.ip_addresses.append(message.payload)
            except queue.Empty:
                pass

        for conn in self.connections:
            try:
                while True:
                    message = conn.write_queue.get_nowait()
                    if isinstance(message, Message):

                        if message.header.msg_type == TYPE_QUERY_HIT:
                            for mapping in self.message_id_mapping.get():
                                if mapping[0] == message.header.msg_id:
                                    if mapping[1] == (self.ip, self.port):
                                        entries = unpack_query_hit_body(message.body)
                                        self.print_query_hit(entries)
                                    else:
                                        try:
                                            message.header.decrement_time_to_live()
                                        except ValueError:
                                            break
                                        message.receivers = [mapping[1]]
                                        self.deliver_message(message)
                                    break
                        elif message.header.msg_type == TYPE_QUERY:

                            # Discard if msg_id has recently been seen
                            for mapping in self.message_id_mapping.get():
                                if mapping[0] == message.header.msg_id:
                                    break
                            # Store msg_id for loop avoidance and query hit redirection
                            self.message_id_mapping.add((message.header.msg_id, conn.other_end_listen_addr))

                            # Respond with query hit, if a local hit is found
                            if message.body in self.data:
                                receivers = [conn.other_end_listen_addr]
                                body = {1: self.data[message.body]}  # A dict with resource_id as key and resource_value as value (both integers)
                                bytes_body = pack_query_hit_body(body)
                                header = Header(TYPE_QUERY_HIT, self.ip, self.port, len(bytes_body), Header.MAX_TTL, message.header.msg_id)
                                query_hit = Message(header, bytes_body, receivers)
                                self.deliver_message(query_hit)

                            # Forward to others and decrement ttl (if ttl over 1)
                            if message.header.time_to_live > 1:
                                message.header.decrement_time_to_live()
                                self.deliver_message(message)

            except queue.Empty:
                pass

    def deliver_message(self, message):

        # If receivers is empty, broadcast to all (except blacklisters)
        if len(message.receivers) == 0:
            # print("self.connections: "+str(self.connections))  # DEBUG
            for conn in self.connections:
                if conn.other_end_listen_addr not in message.receivers_blacklist:
                    conn.read_queue.put(message)

        # Else send to those that are in receivers (except blacklisters)
        else:
            for addr in message.receivers:
                if addr not in message.receivers_blacklist:
                    for conn in self.connections:
                        if addr == conn.other_end_listen_addr:
                            conn.read_queue.put(message)

    def print_query_hit(self, query_hit_body_list):
        print("Results to query:")
        for entry in query_hit_body_list:
            print("ID: " + hex(entry[0]) + "   Value: " + hex(entry[1]))


class CommandlineReader(multiprocessing.Process):
    def __init__(self, stdin):
        super(CommandlineReader, self).__init__()
        self.daemon = True
        self.write_queue = multiprocessing.Queue()
        self.stdin = stdin

    def run(self):

        for line in self.stdin:
            line = line[:-1]  # strip newline from the end
            command = line.split(" ", 2)  # Max_split here should be max_parts_in_msg - 1
            command = list(filter(None, command))  # strip all empty strings from the list
            if len(command) == 0:
                continue

            if command[0] in ["join", "j"] and len(command) in [2, 3]:
                port = PORT_DEFAULT
                if len(command) == 3:
                    try:
                        port = int(command[2])
                    except ValueError:
                        continue  # no need to do anything, if port is invalid
                self.write_queue.put(InterprocessMessage(InterprocessMessage.JOIN, (command[1], port)))
            elif command[0] in ["query", "q"] and len(command) == 2:
                self.write_queue.put(InterprocessMessage(InterprocessMessage.QUERY, command[1].encode() + b"\x00"))
            elif command[0] in ["publish", "p"] and len(command) == 3:
                try:
                    value = bytes.fromhex(command[2])
                except ValueError:
                    continue
                if len(value) > RESOURCE_VALUE_LEN:
                    continue
                while len(value) != RESOURCE_VALUE_LEN:
                    value = b"\x00" + value
                interprocess_message = InterprocessMessage(InterprocessMessage.PUBLISH, (command[1].encode() + b"\x00", value))
                self.write_queue.put(interprocess_message)


class Connection(multiprocessing.Process):

    def __init__(self, sock, node_listen_addr, ip_addresses, other_end_listen_addr=None):

        super(Connection, self).__init__()
        self.daemon = True
        self.socket = sock
        self.node_listen_addr = node_listen_addr
        self.other_end_listen_addr = other_end_listen_addr
        """
        ping_status:
        Integer telling how many ping messages have not been answered. Value -1 means that an answer has been
        received during that cycle.
        """
        self.ping_status = -1
        self.ping_timer = Timer(PING_INTERVAL)

        self.read_queue = multiprocessing.Queue()
        self.write_queue = multiprocessing.Queue()
        self.ip_addresses = ip_addresses

    def run(self):
        process_print("New Connection() process running")

        # If socket is not created already, do it now.
        if self.socket is None:
            self.socket = connect(self.other_end_listen_addr[0], self.other_end_listen_addr[1])
            if self.socket is None:
                self.die()

            self.send_join_request()
            self.recv_join_response()
            self.register_connection(self.other_end_listen_addr)
        else:
            join_request = self.recv_join_request()
            self.other_end_listen_addr = (join_request.header.sender_ip, join_request.header.sender_port)
            self.send_join_response(join_request)
            self.register_connection(self.other_end_listen_addr)

        self.ping_timer.start()

        while True:
            if self.ping_timer.has_expired():
                self.ping_status += 1
                self.send_ping_a()
                self.ping_timer.start()

            read_sockets, _, _ = select.select([self.read_queue._reader, self.socket], [], [], PING_INTERVAL)
            for sock in read_sockets:
                if sock == self.socket:
                    try:
                        header = self.recv_header()
                    except ValueError:
                        continue
                    body = recvall(self.socket, header.payload_len)

                    if header.msg_type == TYPE_QUERY_HIT:
                        self.write_queue.put(Message(header, body, None))

                    elif header.msg_type == TYPE_PING:
                        if header.time_to_live == 1:
                            self.respond_to_type_a_ping(header.msg_id)
                        else:
                            pass  # TODO: receive type b pings

                    elif header.msg_type == TYPE_PONG:
                        # Type A pong
                        if len(body) == 0:
                            self.ping_status = -1
                        else:
                            pass  # TODO: receive type b pongs

                    elif header.msg_type == TYPE_QUERY:
                        self.write_queue.put(Message(header, body, [], [self.other_end_listen_addr]))

                else:
                    self.handle_read_queue()

    def handle_read_queue(self):
        message = self.read_queue.get()
        if isinstance(message, Message):
            self.send_message(message)

    def respond_to_type_a_ping(self, msg_id):
        header_obj = Header(TYPE_PONG, self.node_listen_addr[0], self.node_listen_addr[1], 0, 1, msg_id)
        byte_header = header_obj.get_bytes()
        try:
            self.socket.sendall(byte_header)
        except socket.error:
            self.die()

    def recv_join_request(self):
        """
        :return: listen address (ip, port tuple) of the sender
        """
        header = self.recv_header()
        return Message(header)
        # TODO: validation

    def send_message(self, message):
        byte_header = message.header.get_bytes()
        try:
            self.socket.sendall(byte_header)
            self.socket.sendall(message.body)
            # process_print("sent message: " + str(byte_header) + " and body : " + str(message.body))  # DEBUG
        except socket.error:
            self.die()

    def send_join_request(self):
        header_obj = Header(TYPE_JOIN, self.node_listen_addr[0], self.node_listen_addr[1], 0, 1)
        byte_header = header_obj.get_bytes()
        try:
            self.socket.sendall(byte_header)
        except socket.error:
            self.die()

    def send_ping_a(self):
        header_obj = Header(TYPE_PING, self.node_listen_addr[0], self.node_listen_addr[1], 0, 1)
        byte_header = header_obj.get_bytes()
        try:
            self.socket.sendall(byte_header)
        except socket.error:
            self.die()

    def recv_join_response(self):
        header = self.recv_header()
        bytes_payload = recvall(self.socket, JOIN_RESPONSE_PAYLOAD_LEN)
        payload = struct.unpack("!H", bytes_payload)[0]
        if payload != JOIN_ACC:
            self.die()

    def send_join_response(self, request):
        header_obj = Header(TYPE_JOIN, self.node_listen_addr[0], self.node_listen_addr[1],
                            JOIN_RESPONSE_PAYLOAD_LEN, 1, request.header.msg_id)
        payload_bytes = struct.pack("!H", JOIN_ACC)
        try:
            self.socket.sendall(header_obj.get_bytes() + payload_bytes)
        except socket.error:
            self.die()

    def recv_header(self):
        bytes_header = recvall(self.socket, HEADER_LENGTH)
        if bytes_header is None:
            self.die()
        header_obj = unpack_header(bytes_header)
        return header_obj

    def register_connection(self, other_end_listen_addr):
        registration = InterprocessMessage(InterprocessMessage.REGISTER, other_end_listen_addr)
        self.write_queue.put(registration)

    def die(self):
        if self.socket is not None:
            self.socket.close()
        process_print("Process died")
        os._exit(0)


class Message:
    def __init__(self, header, body=b"", receivers=[], receivers_blacklist=[]):
        """
        A message appointed to all receivers ((ip, port) tuple) in receivers.
        Not appointed to receivers in receivers_blacklist. Blacklist is stronger than receivers list.
        None as receivers value means that receivers are still unknown and should be resolved later.
        :param header:
        :param body:
        :param receiver:
        :param receivers_blacklist:
        :return:
        """
        self.header = header
        self.body = body
        self.receivers = receivers
        self.receivers_blacklist = receivers_blacklist


class Header:

    VALID_VERSIONS = [VERSION]
    MAX_TTL = 5

    def __init__(self, msg_type, sender_ip, sender_port, payload_len, time_to_live, msg_id=None,
                 version=VERSION):

        if version not in Header.VALID_VERSIONS:
            raise ValueError("Invalid protocol version.")
        if (time_to_live < 1) or (time_to_live > Header.MAX_TTL):
            raise ValueError("Invalid time-to-live: " + str(time_to_live))

        self.version = version
        self.msg_id = msg_id
        self.time_to_live = time_to_live
        self.payload_len = payload_len
        self.sender_port = sender_port
        self.sender_ip = sender_ip
        self.msg_type = msg_type

        if self.msg_id is None:
            self.msg_id = randint(0, 4294967295)  # [0, (2^32 - 1)]

    def get_bytes(self):
        """
        :return: header in bytes form
        """
        ip = ip_string_to_int(self.sender_ip)

        # ! means network byte order
        # B means unsigned char (1 byte)
        # H means unsigned short (2 bytes)
        # L means unsigned long (4 bytes)
        byte_header = struct.pack("!BBBBHHLL", self.version, self.time_to_live, self.msg_type, RESERVED_VALUE,
                                  self.sender_port, self.payload_len, ip, self.msg_id)
        return byte_header

    def decrement_time_to_live(self):
        if self.time_to_live > 1:
            self.time_to_live -= 1
        else:
            raise ValueError("Time-to-live must be 1 or more")


def unpack_header(byte_header):
    """
    :param byte_header: A header in bytes, in network-byte-order
    :return: A Header object
    """
    version, time_to_live, msg_type, _, sender_port, payload_len, sender_ip, msg_id = struct.unpack(
        "!BBBBHHLL", byte_header)

    sender_ip = int_to_ip_string(sender_ip)

    return Header(msg_type, sender_ip, sender_port, payload_len, time_to_live, msg_id, version)


def unpack_query_hit_body(byte_body):
    """
    :param byte_body:
    :return: a dict object with resource id as key and value as value (both are bytes objects)
    """
    entries_list = []
    entries_len = struct.unpack("!H", byte_body[:2])[0]
    entries = byte_body[4:]
    for i in range(entries_len):
        first_byte = i*8
        entry = entries[first_byte:first_byte+8]
        resource_id, _, resource_value = struct.unpack("!HHL", entry)
        entries_list.append((resource_id, resource_value))
    return entries_list


def pack_query_hit_body(dict_body):
    """
    :param dict_body: A dictionary. Keys are resource_ids as integer, values are resource_values as integers
    :return: a bytes object
    """
    entry_size = len(dict_body)
    bytes_body = struct.pack("!HH", entry_size, 0)
    for resource_id in dict_body:
        next_bytes = struct.pack("!HHL", resource_id, 0, dict_body[resource_id])
        bytes_body += next_bytes
    return bytes_body


class RoundRobinList:

    def __init__(self, capacity):
        self.capacity = capacity
        self.content = []
        self.next = 0

    def add(self, item):
        if len(self.content) < self.capacity:
            self.content.append(item)
        else:
            self.content[self.next] = item

        self.next += 1
        if self.next == self.capacity:
            self.next = 0

    def get(self):
        return self.content


def process_print(message):
    print("DEBUG:  ::::  PID: " + str(os.getpid()) + "  ::::  " + message)
    sys.stdout.flush()