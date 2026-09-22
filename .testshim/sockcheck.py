import os
import socket

p = os.path.join(os.path.dirname(__file__), "t.sock")
try:
    s = socket.socket(socket.AF_UNIX)
    s.bind(p)
    print("unix bind ok")
    s.close()
    os.unlink(p)
except Exception as e:
    print("unix bind failed:", e)
