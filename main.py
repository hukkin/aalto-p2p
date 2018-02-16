from aalto_p2p.node import Node
import sys

node = None

if len(sys.argv) == 1:
    node = Node("127.0.0.1")
elif len(sys.argv) == 2:
    node = Node(sys.argv[1])
elif len(sys.argv) == 3:
    node = Node(sys.argv[1], int(sys.argv[2]))

if node:
    node.start()

"""
Port:
6346

IP:
130.233.195.30
130.233.195.31
130.233.195.32
"""
