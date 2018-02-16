import socket
import struct

__author__ = 'hukkinj1'


def ip_string_to_int(ip):
    """
    Convert an IPv4 string address to int. For example convert '0.0.0.1' to 1
    """
    packed_ip = socket.inet_aton(ip)
    return struct.unpack("!L", packed_ip)[0]


def int_to_ip_string(int_ip):
    packed_ip = struct.pack("!L", int_ip)
    return socket.inet_ntoa(packed_ip)


def connect(ip, port):
    """
    :param ip: Ip address. Either IP or DNS name as string
    :param port: Integer
    :return: Connected socket, or None if unsuccessful
    """
    sock = None

    try:
        addrinfo = socket.getaddrinfo(ip, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except socket.error:
        return None

    for result in addrinfo:
        family, socktype, proto, _, addr = result
        try:
            sock = socket.socket(family, socktype, proto)
        except socket.error:
            sock = None
            continue
        try:
            sock.connect(addr)
        except socket.error as e:
            log_message = "Failed to connect {}:{} due to {}".format(addr[0], addr[1], e)
            print(log_message)
            sock.close()
            sock = None
            continue
        break
    return sock


def create_listen_socket(ip, port):
    """
    :param ip:
    :param port:
    :return: Return the created blocking listen socket or None if unsuccessful
    """
    sock = None

    try:
        addrinfo = socket.getaddrinfo(ip, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except socket.error:
        return None

    for result in addrinfo:
        family, socktype, proto, _, addr = result
        try:
            sock = socket.socket(family, socktype, proto)
        except socket.error:
            sock = None
            continue
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(addr)
            sock.listen(128)  # parameter value = maximum number of queued connections
        except socket.error as e:
            log_message = "Failed to bind listen socket to address {}:{} due to {}".format(addr[0], addr[1], e)
            print(log_message)
            sock.close()
            sock = None
            continue
        break
    return sock


def recvall(sock, n):
    """
    :param sock:
    :param n:
    :return: If successful, return read n amount of bytes. Otherwise return None.
    """
    data = b""
    while len(data) < n:
        new_bytes = sock.recv(n - len(data))
        if not new_bytes:
            return None
        data += new_bytes
    return data
