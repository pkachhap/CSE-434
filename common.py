#!/usr/bin/env python3
import socket
import struct
import time
from enum import IntEnum

# Protocol constants
PROTO_VERSION = 1
HEADER = struct.Struct(">BBII") 

class MsgType(IntEnum):
    REGISTER_USER_REQ  = 0x10
    REGISTER_USER_RESP = 0x11
    REGISTER_DISK_REQ  = 0x20
    REGISTER_DISK_RESP = 0x21
    CONFIGURE_DSS_REQ  = 0x30
    CONFIGURE_DSS_RESP = 0x31
    DEREGISTER_USER_REQ  = 0x40
    DEREGISTER_USER_RESP = 0x41
    DEREGISTER_DISK_REQ  = 0x50
    DEREGISTER_DISK_RESP = 0x51
    ERROR_RESP = 0x7F

class Status(IntEnum):
    OK = 0
    INVALID_ARGUMENT = 1
    ALREADY_REGISTERED = 2
    NOT_REGISTERED = 3
    INSUFFICIENT_RESOURCES = 4
    INTERNAL_ERROR = 5

# Packing helpers (big-endian) 
pack_u8  = lambda x: struct.pack(">B", x)
pack_u16 = lambda x: struct.pack(">H", x)
pack_u32 = lambda x: struct.pack(">I", x)
pack_u64 = lambda x: struct.pack(">Q", x)

def pack_str(s: str) -> bytes:
    b = s.encode("utf-8")
    if len(b) > 0xFFFF:
        raise ValueError("string too long")
    return pack_u16(len(b)) + b

def pack_u32_list(values):
    if len(values) > 0xFFFF:
        raise ValueError("list too long")
    out = [pack_u16(len(values))]
    out.extend(pack_u32(v) for v in values)
    return b"".join(out)

#  Unpacking cursor 
class Cursor:
    def __init__(self, data: bytes):
        self.data = data
        self.off = 0
    def need(self, n):
        if self.off + n > len(self.data):
            raise ValueError("truncated payload")
    def u8(self):
        self.need(1); v = self.data[self.off]; self.off += 1; return v
    def u16(self):
        self.need(2); v = struct.unpack_from(">H", self.data, self.off)[0]; self.off += 2; return v
    def u32(self):
        self.need(4); v = struct.unpack_from(">I", self.data, self.off)[0]; self.off += 4; return v
    def u64(self):
        self.need(8); v = struct.unpack_from(">Q", self.data, self.off)[0]; self.off += 8; return v
    def str(self):
        n = self.u16(); self.need(n)
        s = self.data[self.off:self.off+n].decode("utf-8")
        self.off += n
        return s
    def u32_list(self):
        n = self.u16()
        out = []
        for _ in range(n):
            out.append(self.u32())
        return out

#  Socket I/O
def recv_exact(sock: socket.socket, n: int) -> bytes:
    bufs, remaining = [], n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("socket closed")
        bufs.append(chunk)
        remaining -= len(chunk)
    return b"".join(bufs)

def send_msg(sock: socket.socket, msg_type: int, txn_id: int, payload: bytes):
    hdr = HEADER.pack(PROTO_VERSION, msg_type, txn_id, len(payload))
    sock.sendall(hdr + payload)

def recv_msg(sock: socket.socket):
    hdr = recv_exact(sock, HEADER.size)
    version, msg_type, txn_id, payload_len = HEADER.unpack(hdr)
    if version != PROTO_VERSION:
        raise ValueError(f"bad protocol version {version}")
    payload = recv_exact(sock, payload_len) if payload_len else b""
    return msg_type, txn_id, payload

#Logging 
def trace(prefix: str, **kv):
    ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    items = " ".join(f"{k}={v}" for k, v in kv.items())
    print(f"[{ts}] {prefix} {items}")

def parse_hostport(s: str):
    host, port = s.split(":", 1)
    return host, int(port)
