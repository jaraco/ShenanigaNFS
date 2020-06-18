import abc
import asyncio
import random

import struct
import xdrlib
from asyncio import StreamWriter, StreamReader
from io import BytesIO
from typing import *

from pynefs import rpchelp
from pynefs.generated.rfc1831 import *
from pynefs.generated.rfc1831 import rpc_msg


T = TypeVar("T")


class UnpackedRPCMsg(Generic[T]):
    def __init__(self, msg: v_rpc_msg, body: T):
        self.msg = msg
        self.body: Optional[T] = body

    @property
    def xid(self) -> int:
        return self.msg.xid

    @xid.setter
    def xid(self, v):
        self.msg.xid = v

    @property
    def header(self):
        return self.msg.header

    @header.setter
    def header(self, v):
        self.msg.header = v

    @property
    def success(self):
        if self.msg.header.mtype != REPLY:
            raise ValueError("Tried to check success of function message?")
        return self.msg.header.rbody.stat == MSG_ACCEPTED


SPLIT_MSG = Tuple[v_rpc_msg, bytes]


class BaseTransport(abc.ABC):
    @abc.abstractmethod
    async def write_msg_bytes(self, msg: bytes):
        pass

    @abc.abstractmethod
    async def read_msg_bytes(self) -> bytes:
        pass

    @property
    def closed(self):
        return False

    @abc.abstractmethod
    def close(self):
        pass

    async def write_msg(self, header: v_rpc_msg, body: bytes) -> None:
        p = xdrlib.Packer()
        rpc_msg.pack(p, header)
        p.pack_fstring(len(body), body)
        await self.write_msg_bytes(p.get_buffer())

    async def read_msg(self) -> SPLIT_MSG:
        msg_bytes = await self.read_msg_bytes()
        unpacker = xdrlib.Unpacker(msg_bytes)
        msg = rpc_msg.unpack(unpacker)
        return msg, unpacker.get_buffer()[unpacker.get_position():]


class TCPTransport(BaseTransport):
    # 100KB, larger than UDP would allow anyway?
    MAX_MSG_BYTES = 100_000

    def __init__(self, reader: StreamReader, writer: StreamWriter):
        self.reader = reader
        self.writer = writer

    @property
    def closed(self):
        return self.reader.at_eof() or self.writer.is_closing()

    def close(self):
        if self.writer.can_write_eof():
            self.writer.write_eof()
        if not self.writer.is_closing():
            self.writer.close()

    async def write_msg_bytes(self, msg: bytes):
        # Tack on the fragment size, mark as last frag
        msg = struct.pack("!L", len(msg) | (1 << 31)) + msg
        self.writer.write(msg)
        await self.writer.drain()

    async def read_msg_bytes(self) -> bytes:
        last_frag = False
        msg_bytes = BytesIO()
        total_len = 0
        while not last_frag:
            frag_header = struct.unpack("!L", await self.reader.readexactly(4))[0]
            last_frag = frag_header & (1 << 31)
            frag_len = frag_header & (~(1 << 31))
            total_len += frag_len
            if total_len > self.MAX_MSG_BYTES:
                raise ValueError(f"Overly large RPC message! {total_len}, {frag_len}")
            msg_bytes.write(await self.reader.readexactly(frag_len))
        return msg_bytes.getvalue()


class BaseClient(abc.ABC):
    prog: int
    vers: int
    procs: Dict[int, rpchelp.Proc]
    transport: Optional[BaseTransport]

    def __init__(self):
        self.xid_map: Dict[int, asyncio.Future] = {}

    def pack_args(self, proc_id: int, args: Sequence):
        packer = xdrlib.Packer()
        arg_specs = self.procs[proc_id].arg_types
        if len(args) != len(arg_specs):
            raise ValueError("Wrong number of arguments!")

        for spec, arg in zip(arg_specs, args):
            spec.pack(packer, arg)
        return packer.get_buffer()

    def pump_reply(self, msg: SPLIT_MSG):
        reply, reply_body = msg
        if not reply.header.mtype == msg_type.REPLY:
            # Weird. log this.
            return
        xid_future = self.xid_map.pop(reply.xid, None)
        if not xid_future:
            # Got a reply for a message we didn't send???
            return
        xid_future.set_result(msg)

    def unpack_return(self, proc_id: int, body: bytes):
        unpacker = xdrlib.Unpacker(body)
        return self.procs[proc_id].ret_type.unpack(unpacker)

    @staticmethod
    def gen_xid() -> int:
        return random.getrandbits(32)

    @abc.abstractmethod
    async def connect(self):
        pass

    async def send_call(self, proc_id: int, *args, xid: Optional[int] = None) -> UnpackedRPCMsg[T]:
        if xid is None:
            xid = self.gen_xid()
        if not self.transport:
            await self.connect()

        msg = v_rpc_msg(
            xid=xid,
            header=v_rpc_body(
                mtype=msg_type.CALL,
                cbody=v_call_body(
                    rpcvers=2,
                    prog=self.prog,
                    vers=self.vers,
                    proc=proc_id,
                    # always null auth for now
                    cred=v_opaque_auth(
                        flavor=auth_flavor.AUTH_NONE,
                        body=b""
                    ),
                    verf=v_opaque_auth(
                        flavor=auth_flavor.AUTH_NONE,
                        body=b""
                    ),
                )
            )
        )
        fut = asyncio.Future()
        self.xid_map[xid] = fut
        await self.transport.write_msg(msg, self.pack_args(proc_id, args))

        # TODO: timeout?
        reply, reply_body = await fut
        assert(reply.header.mtype == REPLY)
        if reply.header.rbody.stat != reply_stat.MSG_ACCEPTED:
            return UnpackedRPCMsg(reply, None)
        return UnpackedRPCMsg(reply, self.unpack_return(proc_id, reply_body))


class TCPClient(BaseClient):
    def __init__(self, host, port):
        super().__init__()
        self.transport = None
        self.reader_task: Optional[asyncio.Task] = None
        self.host = host
        self.port = port

    async def pump_replies(self):
        while self.transport and not self.transport.closed:
            self.pump_reply(await self.transport.read_msg())

    async def connect(self):
        self.transport = TCPTransport(*await asyncio.open_connection(self.host, self.port))
        self.reader_task = asyncio.create_task(self.pump_replies())


class ConnCtx:
    def __init__(self):
        self.state = {}


class Server:
    """Base class for rpcgen-created server classes.  Unpack arguments,
    dispatch to appropriate procedure, and pack return value.  Check,
    at instantiation time, whether there are any procedures defined in the
    IDL which are both unimplemented and whose names are missing from the
    deliberately_unimplemented member.
    As a convenience, allows creation of transport server w/
    create_transport_server.  In what every way the server is created,
    you must call register."""
    prog: int
    vers: int
    procs: Dict[int, rpchelp.Proc]

    def get_handler(self, proc_id) -> Callable:
        return getattr(self, self.procs[proc_id].name)

    def register(self, transport_server):
        transport_server.register(self.prog, self.vers, self)

    def handle_proc_call(self, proc_id, call_body: bytes) -> bytes:
        proc = self.procs[proc_id]
        if proc is None:
            raise NotImplementedError()

        unpacker = xdrlib.Unpacker(call_body)
        argl = [arg_type.unpack(unpacker)
                for arg_type in proc.arg_types]
        rv = self.get_handler(proc_id)(*argl)

        packer = xdrlib.Packer()
        proc.ret_type.pack(packer, rv)
        return packer.get_buffer()

    def make_reply(self, xid, stat: reply_stat = 0, msg_stat: Union[accept_stat, reject_stat] = 0) -> v_rpc_msg:
        return v_rpc_msg(
            xid=xid,
            header=v_rpc_body(
                mtype=msg_type.REPLY,
                rbody=v_reply_body(
                    stat=stat,
                    areply=v_accepted_reply(
                        verf=v_opaque_auth(
                            auth_flavor.AUTH_NONE,
                            body=b""
                        ),
                        data=v_reply_data(
                            stat=msg_stat,
                        )
                    )
                )
            )
        )


class TCPServer(Server):
    def __init__(self, bind_host, bind_port):
        self.bind_host, self.bind_port = bind_host, bind_port

    async def start(self) -> asyncio.AbstractServer:
        return await asyncio.start_server(self.handle_connection, self.bind_host, self.bind_port)

    async def handle_connection(self, reader, writer):
        transport = TCPTransport(reader, writer)
        ctx = ConnCtx()
        while not transport.closed:
            try:
                read_ret = await asyncio.wait_for(transport.read_msg(), 1)
            except asyncio.TimeoutError:
                continue

            call: v_rpc_msg = read_ret[0]
            call_body: bytes = read_ret[1]

            if not call.header.mtype == msg_type.CALL:
                # ???
                continue
            try:
                await self.handle_call(transport, call, call_body)
            except Exception:
                reply_header = self.make_reply(call.xid, reply_stat.MSG_ACCEPTED, accept_stat.SYSTEM_ERR)
                await transport.write_msg(reply_header, b"")
                transport.close()
                raise
        transport.close()

    async def handle_call(self, transport: BaseTransport, call: v_rpc_msg, call_body: bytes):
        reply_body = self.handle_proc_call(call.header.cbody.proc, call_body)
        reply_header = self.make_reply(call.xid)
        await transport.write_msg(reply_header, reply_body)
