#!/usr/bin/env python3
import argparse
import itertools
import socket
import time

from common import (
    MsgType, Status, Cursor, send_msg, recv_msg, parse_hostport,
    pack_str, pack_u16, pack_u64, pack_u32
)

_txn = itertools.count(1)

def enc_register_disk_req(name: str, listen_port: int, capacity: int, zone: str) -> bytes:
    return pack_str(name) + pack_u16(listen_port) + pack_u64(capacity) + pack_str(zone)

def dec_register_disk_resp(pb: bytes):
    c = Cursor(pb)
    status = c.u8(); disk_id = c.u32()
    return status, disk_id

def enc_deregister_disk_req(disk_id: int) -> bytes:
    return pack_u32(disk_id)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--manager", required=True)
    ap.add_argument("--capacity", type=int, required=True)
    ap.add_argument("--zone", default="")
    args = ap.parse_args()

    host, mport = parse_hostport(args.manager)
    with socket.create_connection((host, mport)) as sock:
        # register-disk
        t = next(_txn)
        send_msg(sock, MsgType.REGISTER_DISK_REQ, t, enc_register_disk_req(args.name, args.port, args.capacity, args.zone))
        mt, tid, pb = recv_msg(sock)
        if mt != MsgType.REGISTER_DISK_RESP:
            c = Cursor(pb); status = c.u8(); msg = c.str()
            raise SystemExit(f"ERROR {Status(status).name}: {msg}")
        status, disk_id = dec_register_disk_resp(pb)
        if status != Status.OK:
            raise SystemExit(f"Register failed: {Status(status).name}")
        print(f"Registered disk_id={disk_id}")

        try:
            print("Disk running; Ctrl+C to deregister")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            t = next(_txn)
            send_msg(sock, MsgType.DEREGISTER_DISK_REQ, t, enc_deregister_disk_req(disk_id))
            mt, tid, pb = recv_msg(sock)
            c = Cursor(pb); st = c.u8()
            print(f"Deregister status={Status(st).name}")

if __name__ == "__main__":
    main()
