#!/usr/bin/env python3
import argparse
import itertools
import socket

from common import (
    MsgType, Status, Cursor, send_msg, recv_msg, parse_hostport,
    pack_str, pack_u16, pack_u32
)

_txn = itertools.count(1)

# Encoders/decoders 
def enc_register_user_req(name: str, listen_port: int) -> bytes:
    return pack_str(name) + pack_u16(listen_port)

def dec_register_user_resp(pb: bytes):
    c = Cursor(pb)
    status = c.u8()
    user_id = c.u32()
    return status, user_id

def enc_configure_req(n: int, policy: str, constraints_json: str) -> bytes:
    return pack_u16(n) + pack_str(policy) + pack_str(constraints_json)

def dec_configure_resp(pb: bytes):
    c = Cursor(pb)
    status = c.u8()
    dss_id = c.u32()
    disk_ids = c.u32_list()
    return status, dss_id, disk_ids

def enc_deregister_user_req(user_id: int) -> bytes:
    return pack_u32(user_id)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--port", type=int, required=True, help="user's listen port (record only)")
    ap.add_argument("--manager", required=True, help="host:port of manager")
    args = ap.parse_args()

    host, mport = parse_hostport(args.manager)
    with socket.create_connection((host, mport)) as sock:
        # register-user
        t = next(_txn)
        send_msg(sock, MsgType.REGISTER_USER_REQ, t, enc_register_user_req(args.name, args.port))
        mt, tid, pb = recv_msg(sock)
        if mt != MsgType.REGISTER_USER_RESP:
            c = Cursor(pb); status = c.u8(); msg = c.str()
            raise SystemExit(f"ERROR {Status(status).name}: {msg}")
        status, user_id = dec_register_user_resp(pb)
        if status != Status.OK:
            raise SystemExit(f"Register failed: {Status(status).name}")
        print(f"Registered user_id={user_id}")

        try:
            while True:
                cmd = input("> ").strip()
                if not cmd:
                    continue
                if cmd == "bye":
                    t = next(_txn)
                    send_msg(sock, MsgType.DEREGISTER_USER_REQ, t, enc_deregister_user_req(user_id))
                    mt, tid, pb = recv_msg(sock)
                    c = Cursor(pb); st = c.u8()
                    print(f"Deregister status={Status(st).name}")
                    break
                if cmd.startswith("cfg "):
                    try:
                        n = int(cmd.split()[1])
                    except Exception:
                        print("usage: cfg N")
                        continue
                    t = next(_txn)
                    send_msg(sock, MsgType.CONFIGURE_DSS_REQ, t, enc_configure_req(n, "RAID0", ""))
                    mt, tid, pb = recv_msg(sock)
                    if mt != MsgType.CONFIGURE_DSS_RESP:
                        print("unexpected response")
                        continue
                    st, dss_id, disks = dec_configure_resp(pb)
                    if st == Status.OK:
                        print(f"DSS created dss_id={dss_id} disks={disks}")
                    else:
                        print(f"Configure failed: {Status(st).name}")
                else:
                    print("commands: cfg N | bye")
        except (EOFError, KeyboardInterrupt):
            try:
                t = next(_txn)
                send_msg(sock, MsgType.DEREGISTER_USER_REQ, t, enc_deregister_user_req(user_id))
                recv_msg(sock)
            except Exception:
                pass

if __name__ == "__main__":
    main()
