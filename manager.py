#!/usr/bin/env python3
import argparse
import socket
import threading
from typing import Dict, Tuple

from common import (
    MsgType, Status, Cursor, send_msg, recv_msg, trace,
    pack_u8, pack_u32, pack_str, pack_u32_list
)

class Registry:
    def __init__(self):
        self.lock = threading.Lock()
        self.next_user_id = 1
        self.next_disk_id = 1
        self.next_dss_id = 1
        self.users: Dict[int, dict] = {}
        self.disks: Dict[int, dict] = {}

    def register_user(self, name, addr, port):
        with self.lock:
            uid = self.next_user_id; self.next_user_id += 1
            self.users[uid] = {"user_id": uid, "name": name, "addr": addr, "port": port, "status": "READY"}
            return uid

    def deregister_user(self, user_id):
        with self.lock:
            return self.users.pop(user_id, None) is not None

    def register_disk(self, name, addr, port, capacity_bytes, zone):
        with self.lock:
            did = self.next_disk_id; self.next_disk_id += 1
            self.disks[did] = {
                "disk_id": did, "name": name, "addr": addr, "port": port,
                "capacity": int(capacity_bytes), "zone": zone, "status": "READY"
            }
            return did

    def deregister_disk(self, disk_id):
        with self.lock:
            return self.disks.pop(disk_id, None) is not None

    def allocate_disks(self, n):
        with self.lock:
            ready = [d["disk_id"] for d in self.disks.values()]
            if len(ready) < n:
                return None
            return ready[:n]

reg = Registry()

# RESP encoders 
def enc_register_user_resp(status: Status, user_id: int) -> bytes:
    return pack_u8(int(status)) + pack_u32(user_id)

def enc_register_disk_resp(status: Status, disk_id: int) -> bytes:
    return pack_u8(int(status)) + pack_u32(disk_id)

def enc_configure_resp(status: Status, dss_id: int, disk_ids):
    return pack_u8(int(status)) + pack_u32(dss_id) + pack_u32_list(disk_ids or [])

def enc_simple_status(status: Status) -> bytes:
    return pack_u8(int(status))

def enc_error(status: Status, msg: str) -> bytes:
    return pack_u8(int(status)) + pack_str(msg)

# REQ decoders
def dec_register_user_req(pb: bytes):
    c = Cursor(pb)
    name = c.str()
    listen_port = c.u16()
    return name, listen_port

def dec_register_disk_req(pb: bytes):
    c = Cursor(pb)
    name = c.str()
    listen_port = c.u16()
    capacity = c.u64()
    zone = c.str()
    return name, listen_port, capacity, zone

def dec_configure_req(pb: bytes):
    c = Cursor(pb)
    n = c.u16()
    policy = c.str()              
    constraints_json = c.str()    
    return n, policy, constraints_json

def dec_deregister_user_req(pb: bytes):
    c = Cursor(pb)
    return c.u32()

def dec_deregister_disk_req(pb: bytes):
    c = Cursor(pb)
    return c.u32()

def handle_conn(conn: socket.socket, addr: Tuple[str, int]):
    peer = f"{addr[0]}:{addr[1]}"
    try:
        while True:
            msg_type, txn_id, payload = recv_msg(conn)
            trace("IN ", peer=peer, type=msg_type, txn=txn_id)

            if msg_type == MsgType.REGISTER_USER_REQ:
                try:
                    name, listen_port = dec_register_user_req(payload)
                    if not name or listen_port <= 0:
                        raise ValueError("missing name/port")
                    uid = reg.register_user(name, addr[0], listen_port)
                    send_msg(conn, MsgType.REGISTER_USER_RESP, txn_id, enc_register_user_resp(Status.OK, uid))
                except Exception as e:
                    send_msg(conn, MsgType.ERROR_RESP, txn_id, enc_error(Status.INVALID_ARGUMENT, str(e)))

            elif msg_type == MsgType.DEREGISTER_USER_REQ:
                uid = dec_deregister_user_req(payload)
                ok = reg.deregister_user(uid)
                send_msg(conn, MsgType.DEREGISTER_USER_RESP, txn_id, enc_simple_status(Status.OK if ok else Status.NOT_REGISTERED))

            elif msg_type == MsgType.REGISTER_DISK_REQ:
                try:
                    name, listen_port, cap, zone = dec_register_disk_req(payload)
                    if not name or listen_port <= 0 or cap <= 0:
                        raise ValueError("bad disk args")
                    did = reg.register_disk(name, addr[0], listen_port, cap, zone)
                    send_msg(conn, MsgType.REGISTER_DISK_RESP, txn_id, enc_register_disk_resp(Status.OK, did))
                except Exception as e:
                    send_msg(conn, MsgType.ERROR_RESP, txn_id, enc_error(Status.INVALID_ARGUMENT, str(e)))

            elif msg_type == MsgType.DEREGISTER_DISK_REQ:
                did = dec_deregister_disk_req(payload)
                ok = reg.deregister_disk(did)
                send_msg(conn, MsgType.DEREGISTER_DISK_RESP, txn_id, enc_simple_status(Status.OK if ok else Status.NOT_REGISTERED))

            elif msg_type == MsgType.CONFIGURE_DSS_REQ:
                n, policy, constraints_json = dec_configure_req(payload)
                if n <= 0:
                    send_msg(conn, MsgType.CONFIGURE_DSS_RESP, txn_id, enc_configure_resp(Status.INVALID_ARGUMENT, 0, []))
                    continue
                disks = reg.allocate_disks(n)
                if not disks:
                    send_msg(conn, MsgType.CONFIGURE_DSS_RESP, txn_id, enc_configure_resp(Status.INSUFFICIENT_RESOURCES, 0, []))
                    continue
                with reg.lock:
                    dss_id = reg.next_dss_id
                    reg.next_dss_id += 1
                send_msg(conn, MsgType.CONFIGURE_DSS_RESP, txn_id, enc_configure_resp(Status.OK, dss_id, disks))

            else:
                send_msg(conn, MsgType.ERROR_RESP, txn_id, enc_error(Status.INVALID_ARGUMENT, f"unknown msg_type {msg_type}"))
    except (ConnectionError, OSError):
        trace("BYE", peer=peer)
    finally:
        conn.close()

def serve(port: int, host: str = "0.0.0.0"):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(128)
        trace("LISTEN", host=host, port=port)
        while True:
            conn, addr = s.accept()
            trace("ACCEPT", peer=f"{addr[0]}:{addr[1]}")
            threading.Thread(target=handle_conn, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    args = ap.parse_args()
    serve(args.port)
