# Main server, in a socket style
#
# Trying to show how to do it both correctly and "robustly", which
# means that client mess-ups (either by not following the protocol, or
# asking for too many resources, or by being malicious) don't take
# down the server.
#
# This means:
#
# - Two threads per client. This allows bi-directional communication,
#   yet with blocks on both directions so we are not spending any CPU
#   resources idle-polling. You need one to handle the blocking read,
#   then another to send out messages from other clients. This writer
#   thread should queue up whole messages so there is no weird message
#   interleaving if two clients are trying to send a message at the
#   same time.
#
# - Handling blocking threads properly. In general, you don't want to
#   just blindly use blocking calls and rely on killing threads when
#   something is failing, because the other resources the thread
#   acquired may not be freed (the sockets!) Luckily, in this case, if
#   we are blocking on a socket, we can close the socket (from another
#   thread) which causes the blocking call to end, allowing us to exit
#   the thread.
#
# - Two threads really can be running at the same time. Which means
#   shared resources need to somehow be protected so that only one can
#   access at one point in time. For us, these resources are the
#   client list and the room list. We don't want to (for example) check
#   to see if a username is available, notice that it is, then assign
#   it; but between the check and assignment, another client thread
#   was doing the same check, and we end up with two client with the
#   same username. For situations like this, we use a lock (mutex).
#   This can be surprisingly tricky to get right!
#
# - These concerns can interact, leading to confusing responsibility
#   as to where and when we actually kill clients and remove them from
#   the shared state. The way we navigate this complexity is by
#   following the idea that each client has a single "owner": its own
#   reader thread. Only that thread ever removes the client from the
#   shared state (its one cleanup path, in a finally block). Any other
#   thread that wants a client gone just calls client.kill(), which
#   closes the socket; the owner thread then wakes up and cleans
#   up. This avoids tricky lock-ordering problems (deadlocks) and
#   recursive announcements.

import logging
import threading
import queue
import select
import signal
import socket
import time

# Use logging instead of print to avoid message interleaving from multiple threads
log = logging.getLogger(__name__)

HOST = '0.0.0.0'
PORT = 12347
ENCODING = 'utf-8'
MSG_START = b'\x02'
MSG_END   = b'\x03'
MAX_LEN = 1024
BACKLOG = 32
MAX_CLIENTS = 32
HANDSHAKE_TIMEOUT_S = 10
IDLE_TIMEOUT_S = 15*60
CLOSE_CONFIRM_S = 60  # A room creator must ask twice within this time to close a room
SERVER_USER = '*DadServer*'
STOP = object()  # Unique object we use to tell a write thread to finish
DEFAULT_ROOM = 'general'
PREFIX_DIRECT = 'PRIVATE'
PREFIX_ERROR = 'ERROR'
VERSION = '2.0.6' # Semantic versioning, ideally

RESTRICTED_NAMES = {PREFIX_ERROR.lower(),
                    PREFIX_DIRECT.lower(),
                    DEFAULT_ROOM.lower(),
                    SERVER_USER.lower(),
                    SERVER_USER.lower().strip('*'),
                    'dadmin', 'admin', 'root'}


class ChatError(RuntimeError):
    """A client request that is not allowed."""


class MessageParser:
    """Convert byte stream into a series of messages."""

    def __init__(self):
        self.buf = b''

    def append_and_parse(self, new_bytes):
        """Returns a list of complete message payloads."""
        self.buf += new_bytes
        return self.parse()

    def parse(self):
        msgs = []
        while True:
            idx = self.buf.find(MSG_START)
            if idx < 0:
                # No starting character, all bytes mean nothing
                self.buf = b''
                return msgs
            elif idx > 0:
                # There is a starting character, but junk before it
                self.buf = self.buf[idx:]

            # Now starting character is at 0, find ending character
            idx = self.buf.find(MSG_END, 1)
            if idx < 0:
                if len(self.buf) > MAX_LEN:
                    # This message is too long, so it is technically junk
                    self.buf = self.buf[MAX_LEN:]
                    continue
                # Rest of message could still be pending
                return msgs
            elif idx >= MAX_LEN:
                # Found end character, but message is too long; discard
                self.buf = self.buf[MAX_LEN:]
                continue

            # Now we need to confirm that there isn't another starting character
            sidx = self.buf.find(MSG_START, 1)
            if 0 < sidx < idx:
                # Yes, another start character
                self.buf = self.buf[sidx:]
                continue

            # We found the end of a message with valid length, and
            # with no intermediate start characters.
            # Decode it, while making sure it will stay
            # less than or equal to its length if we re-encode it.
            msg = self.buf[1:idx].decode(ENCODING, errors='ignore')
            msgs.append(msg)
            self.buf = self.buf[(idx+1):]


def blocking_read_messages(sock, parser, timeout_s):
    """Read and return a non-empty list of messages.

    Returns None if no complete message arrived before the deadline,
    or if the peer closed the connection.

    The deadline is enforced with select() rather than sock.settimeout(),
    because a socket timeout is shared by the whole socket - it would also
    apply to the write thread's sendall() on this same socket, and a short
    leftover read timeout could make a healthy write spuriously fail. The
    timeout on select() affects only this call in this thread. The socket
    itself stays fully blocking.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        readable, _, _ = select.select([sock], [], [], remaining)  # Blocking
        if not readable:
            return None  # Deadline passed with no data
        chunk = sock.recv(1024)  # Won't block: socket is readable (or at EOF)
        if not chunk:
            return None  # Peer closed the connection
        msgs = parser.append_and_parse(chunk)
        if msgs:
            return msgs


def connect_to_server(host):
    """Resolve host and connect to the chat server on it (for clients).

    We ask for an IPv4 address explicitly. The default lookup asks for
    IPv4 and IPv6 together, and on .local (mDNS/Bonjour) hostnames that
    can stall for ~5 seconds per connect when the target host answers
    the IPv4 question but never answers the IPv6 one (measured against
    a macOS -> Unraid/avahi setup). If there is no IPv4 address at all,
    fall back to the default lookup.
    """
    try:
        infos = socket.getaddrinfo(host, PORT, family=socket.AF_INET,
                                   type=socket.SOCK_STREAM)
    except socket.gaierror:
        infos = socket.getaddrinfo(host, PORT, type=socket.SOCK_STREAM)
    return socket.create_connection(infos[0][4])


def format_msg(target: str, source: str, msg: str):
    """Adds header to our message."""
    return ' '.join([target, source + ':', msg])


def encode(s):
    """Encodes message into our byte format."""
    s = truncate_to_encoded_length(s, MAX_LEN - 2)
    return MSG_START + s.encode(ENCODING, errors='ignore') + MSG_END


def remove_chars(s, chars: str):
    return ''.join(c for c in s if c not in chars)


def truncate_to_encoded_length(s, n):
    s = s[:n]
    while len(s.encode(ENCODING, errors='ignore')) > n:
        s = s[:(len(s)-1)]
    return s


def valid_username(s: str):
    return valid_name(s, 'user', 14)


def valid_roomname(s: str):
    return valid_name(s, 'room', 14)


def valid_name(s: str, nametype: str, max_encoded_len: int):
    """Checks to see if s is valid name. Raises ChatError with reason if unable."""
    if s != s.strip():
        raise ChatError(f'{nametype}name has leading or trailing whitespace')
    if s != remove_chars(s, '/*#: \t\r\n'):
        raise ChatError(f'{nametype}name contains invalid characters')
    if s != truncate_to_encoded_length(s, max_encoded_len):
        raise ChatError(f'{nametype}name exceeds max encoded length ({max_encoded_len})')
    if len(s) < 3:
        raise ChatError(f'{nametype}name too short (< 3 characters)')
    if s.lower() in RESTRICTED_NAMES:
        raise ChatError(f'{nametype}name a controlled term - not allowed')
    if not any(c.isalpha() for c in s):
        raise ChatError(f'{nametype}name must include at least one letter')
    return s


class Room:
    def __init__(self, name: str, creator: str, public=False):
        self.name: str = name
        self.public: bool = public
        self.creator: str = creator
        self._admins: set[str] = set()
        self._allowed: set[str] = set() # Only used when public = False
        self.banned: set[str] = set()   # Only used when public = True
        self.present: set[str] = {self.creator} # Default to creator present
        self.close_requested_at: float | None = None  # When the creator last asked to close

    @property
    def admins(self):
        return {self.creator} | self._admins

    @property
    def allowed(self):
        return {self.creator} | self._allowed

    def kick(self, kicker: str, kickee: str):
        if kicker not in self.admins:
            raise ChatError(f'{kicker} not an admin - not allowed to kick {kickee}')
        if kickee == self.creator:
            raise ChatError(f'Not allowed to kick the room creator {kickee}')
        if kickee in self.admins and kicker != self.creator:
            raise ChatError(f'Only the room creator has permission to kick admin {kickee}')
        self._admins.discard(kickee)  # Being kicked also takes away admin status
        self.present.discard(kickee)
        if self.public:
            self.banned.add(kickee)
        else:
            self._allowed.discard(kickee)

    def invite(self, inviter: str, candidate: str):
        if inviter not in self.admins:
            raise ChatError(f'{inviter} not an admin - not allowed to invite {candidate}')
        if self.public:
            if candidate in self.banned:
                self.banned.discard(candidate)
        else:
            self._allowed.add(candidate)

    def join(self, user: str):
        if self.public:
            if user in self.banned:
                raise ChatError(f'{user} has been banned from #{self.name}')
        else:
            if user not in self.allowed:
                raise ChatError(f'{user} does not have permission to join #{self.name}')
        self.present.add(user)

    def leave(self, user: str):
        self.present.discard(user)

    def promote(self, promoter: str, candidate: str):
        if promoter != self.creator:
            raise ChatError('Only the room creator has permission to promote')
        if not self.public and candidate not in self.allowed:
            raise ChatError(
                f'{candidate} must first be allowed into #{self.name} before promotion to admin')
        self._admins.add(candidate)

    def demote(self, demoter: str, candidate: str):
        if demoter != self.creator:
            raise ChatError('Only the room creator has permission to demote')
        if candidate not in self._admins:
            raise ChatError(f'{candidate} is not an admin, so nothing to demote')
        self._admins.discard(candidate)

    def request_close(self, closer: str, now: float):
        """Returns True if the room should close now.

        To be sure, the creator has to ask twice within CLOSE_CONFIRM_S
        seconds. `now` is the current time in seconds (passed in, so tests
        don't have to wait a minute).
        """
        if closer != self.creator:
            raise ChatError('Only the room creator has permission to close the room')
        asked_recently = (self.close_requested_at is not None
                          and now - self.close_requested_at <= CLOSE_CONFIRM_S)
        self.close_requested_at = now
        return asked_recently

    def drop(self, name):
        """Remove all references to this name in this room.

        Does not change the creator - this should be done elsewhere.
        """
        self._admins.discard(name)
        self._allowed.discard(name)
        self.banned.discard(name)
        self.present.discard(name)


class Client:
    """Hold on to the client information and socket.

    Also, single spot to stop the socket (and thus stop the blocking read
    and write threads).
    """
    SENDING_QUEUE_MAX = 64

    def __init__(self, sock, peer):
        self.name = None
        self.sock = sock
        self.peer = peer # additional information about the socket
        self.messages_to_send = queue.Queue(maxsize=self.SENDING_QUEUE_MAX)
        # TODO: heartbeat timing info / state would also go here
        # connection time?

    def __repr__(self):
        return f"<{self.name or '?'} {self.peer}>"

    def send(self, msgbytes: bytes):
        """Enqueue an already encoded message to send (assumes client threads are all running).

        Raises queue.Full if queue is full.
        """
        self.messages_to_send.put_nowait(msgbytes)

    def kill_before_full_setup_with_message(self, msg):
        """Shut down the client with a message. Assumes other threads not set up yet."""
        try:
            self.sock.settimeout(2.0)
            self.sock.sendall(encode(format_msg(PREFIX_DIRECT, SERVER_USER, msg)))
            self.sock.shutdown(socket.SHUT_WR)
            # Closing with unread data in the receive buffer makes the kernel send
            # RST instead of FIN, and an RST can discard the message we just sent.
            # Consume the peer's pending bytes first.
            self.sock.recv(4096)
        except OSError:
            pass # This message sending is best effort
        finally:
            self.sock.close()

    def kill(self):
        """Initiate stopping both threads associated with the client.

        To stop blocking reads or blocking writes, we close the socket,
        which ends blocking calls (on either thread).

        On the write thread, we also send a terminal message on the
        writing queue, if that is where the write thread is blocked.

        Okay to call from any thread, and okay to call twice.
        """
        try:
            # Stop the blocking reader and writer by shutting down the socket
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            # Socket already is dead, so nothing to shut down
            pass

        try:
            # If the writer is actually blocked waiting for the queue,
            # send a terminal message
            self.messages_to_send.put_nowait(STOP)
        except queue.Full:
            # Full queue means the write thread will see the dead socket on its own
            pass

    def quit(self):
        """Gracefully stop the client: send every message already queued, then hang up.

        Unlike kill(), this does not close the socket right away. The write
        thread hangs up (by calling kill()) once it reaches the STOP message.
        """
        try:
            self.messages_to_send.put_nowait(STOP)
        except queue.Full:
            # No room to wait our turn, so hang up right away
            self.kill()


def splitmsg(msg):
    """Splits a message with a prefix character.

    Returns the prefix word with the prefix character, and the
    rest of the message after the space.
    """
    if not msg:
        return '', ''

    i_space = msg.find(' ')
    if i_space < 0:
        return msg, ''

    return msg[:i_space], msg[(i_space+1):]


def plain(s):
    """Strips off prefix character"""
    if len(s) > 0 and s[0] in '/#:':
        return s[1:]
    return s


class ChatServer:
    """Keep track of connected users and rooms."""

    def __init__(self):
        # You MUST acquire this lock before touching clients, rooms,
        # or num_connected
        self.lock = threading.Lock()
        self.clients = {}      # map of username to Client (only fully set up clients)
        self.num_connected = 0 # all open connections, including pre-handshake ones
        self.rooms = dict()    # map of room name to Room

        self.commands = Commander(self)

        self.rooms[DEFAULT_ROOM] = Room(DEFAULT_ROOM, SERVER_USER, public=True)

    def users(self):
        """Returns a list of connected users."""
        with self.lock:
            return list(self.clients.keys())

    def _get_room(self, roomname):
        # Call within a lock
        if roomname not in self.rooms:
            raise ChatError(f'No room named #{roomname}')
        return self.rooms[roomname]

    def users_in_room(self, roomname):
        with self.lock:
            return list(self._get_room(roomname).present)

    def join_room(self, roomname, clientname):
        with self.lock:
            self._get_room(roomname).join(clientname)

    def apply_to_each_user(self, roomname, action, actor, users):
        """Do a room action ('invite', 'promote', 'demote' or 'kick') to each user.

        For example, apply_to_each_user('kitchen', 'kick', 'alice', ['bob', 'carol'])
        calls room.kick('alice', 'bob'), then room.kick('alice', 'carol').
        Each user is tried separately, so one user that fails does not stop the rest.

        Returns (list of users it worked for, list of error messages).
        """
        succeeded, errors = [], []
        with self.lock:
            room = self._get_room(roomname)
            for user in users:
                try:
                    getattr(room, action)(actor, user)
                    succeeded.append(user)
                except ChatError as e:
                    errors.append(str(e))
        return succeeded, errors

    def close_room(self, roomname, closer):
        """Close a room (the creator has to ask twice within CLOSE_CONFIRM_S, to be sure).

        Returns the users who were in the room, or None if this was only the first ask.
        """
        with self.lock:
            room = self._get_room(roomname)
            if not room.request_close(closer, time.monotonic()):
                return None
            del self.rooms[roomname]
            return list(room.present)

    def room_info(self):
        "Returns (name, creator, other admins) for all rooms"
        with self.lock:
            return [(name, room.creator, sorted(room.admins - {room.creator}))
                    for (name, room) in self.rooms.items()]

    def new_room(self, roomname, creator, public=False):
        roomname = valid_roomname(roomname) # Raises ChatError
        with self.lock:
            if roomname in self.rooms:
                raise ChatError(f'Room #{roomname} already exists')
            self.rooms[roomname] = Room(roomname, creator, public=public)

    def drop_client(self, name):
        """Removes a client, and removes them from all rooms.

        Also removes rooms they created.

        Only ever called by the client's own handler thread (see the
        finally block in _handle_client) - that thread owns cleanup.

        Returns a tuple of (was dropped from client list, list of room names closed).
        """
        dropped_rooms = []
        with self.lock:
            was_dropped = self.clients.pop(name, None) is not None

            for roomname in list(self.rooms):
                room = self.rooms[roomname]
                if room.creator == name:
                    dropped_rooms.append(roomname)
                    del self.rooms[roomname]
                else:
                    room.drop(name)
        return was_dropped, dropped_rooms

    def report_drop(self, name, was_dropped, dropped_rooms):
        """Broadcast if someone left, and/or if rooms closed."""
        msg = ''
        if name and was_dropped:
            msg = f'{name} left.'
        if dropped_rooms:
            msg += f' Rooms closed: {str(dropped_rooms)}'
        msg = msg.strip()
        if msg:
            self.broadcast(msg)

    def handle_client(self, sock, peer):
        """Handle client connection - one per client connection on own thread"""
        with sock:
            # Guarantee socket close no matter what
            self._handle_client(sock, peer)

    def _handle_client(self, sock, peer):
        # Make sure to disable Nagle's algorithm - makes sockets respond immediately
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        client = Client(sock, peer)
        write_thread = threading.Thread(target=self.write_loop, args=(client,),
                                        daemon=True, name=f'send-{peer[1]}')

        log.info(f'Got a connection from {peer}')
        with self.lock:
            # With the lock, this guarantees we will never go above the max number of clients
            admit = self.num_connected < MAX_CLIENTS
            if admit:
                self.num_connected += 1
        if not admit:
            log.warning(f'Rejecting {peer} since we are at capacity')
            client.kill_before_full_setup_with_message('Server is full, try again later')
            return

        try:
            # Start the writing thread (before checking username validity) to avoid
            # race condition between when client is fully accepted (with username
            # grabbed) and initial messages trying to be broadcast.
            write_thread.start() # Can throw RuntimeError if out of threads

            # Handle the initial username retrieval
            parser = MessageParser()
            msgs = blocking_read_messages(sock, parser, HANDSHAKE_TIMEOUT_S)
            if msgs is None:
                client.kill_before_full_setup_with_message(f'No username received in time')
                return
            log.info(f'Checking if {msgs[0]} is a valid username')
            try:
                username = valid_username(msgs[0])
            except ChatError as e:
                client.kill_before_full_setup_with_message(str(e))
                return
            # Check and claim username in one go
            with self.lock:
                username_taken = username in self.clients
                if not username_taken:
                    client.name = username
                    self.clients[username] = client
                    # Also add to the general room
                    self.rooms[DEFAULT_ROOM].present.add(username)
            if username_taken:
                client.kill_before_full_setup_with_message(f'username {username} already in use')
                return
            self._send_private(client, f'Welcome {username}!')
            # Now that the client has a username, all of the normal messaging functions work.
            self.broadcast(f'{username} has joined')

            # The client may have sent more messages right behind the username
            for msg in msgs[1:]:
                self.dispatch(msg, client)

            # Carry on with the normal read loop
            while True:
                msgs = blocking_read_messages(client.sock, parser, IDLE_TIMEOUT_S)
                if msgs is None:
                    # Read timed out, or client disconnected. Let this client go.
                    return
                for msg in msgs:
                    self.dispatch(msg, client)
        except OSError as e:
            log.error(f'{client} socket error: {e}')
        except Exception:
            log.exception(f'{client} unhandled error')   # incl. failed start
        finally:
            # The single cleanup path for this client - nobody else
            # removes it from the shared state.
            client.kill()                # stops the sender thread
            if client.name is not None:
                was_dropped, dropped_rooms = self.drop_client(client.name)
                self.report_drop(client.name, was_dropped, dropped_rooms)
            with self.lock:
                self.num_connected -= 1
            log.info(f'Disconnect {client!r}')
            if write_thread.is_alive():  # False if start() never succeeded
                # Clean up the write thread
                write_thread.join(timeout=5.0)

    def write_loop(self, client):
        """Runs in its own thread so that read and write can happen simultaneously.

        Also, so that we can block on both sides to not use any CPU when idle.
        """
        try:
            while True:
                frame = client.messages_to_send.get()
                if frame is STOP:
                    return
                client.sock.sendall(frame)
        except OSError:
            pass  # Peer is gone
        finally:
            # Break the reader too: after a quit(), or if the peer is gone
            # (so the reader isn't left waiting for the idle timeout).
            client.kill()

    def dispatch(self, msg, client):
        """Dispatch a message from a client. Handle all commands."""
        # Silently drop messages of zero length
        if len(msg) == 0:
            return

        # If no prefix character - add on the default room
        if msg[0] not in '/:#':
            msg = f'#{DEFAULT_ROOM} {msg}'

        cmd_or_target, payload = splitmsg(msg)
        try:
            if msg.startswith('/'):
                # Command
                reply, do_quit = self.commands.handle(plain(cmd_or_target), payload, client.name)
                if reply:
                    # Takes care of echoing command
                    self._send_private(client, f'{cmd_or_target} {reply}')
                if do_quit:
                    client.quit()  # Let the reply above get sent first
            elif msg.startswith('#'):
                # Message to a room
                self.broadcast(payload, client.name, plain(cmd_or_target))
            elif msg.startswith(':'):
                # Direct message to a user
                self.direct_send(plain(cmd_or_target), payload, client.name)
            else:
                # Should not get here, but nothing to do but keep moving on
                log.error(f'Unexpected message format: {msg}')
        except ChatError as e:
            # Takes care of adding target prefix for error
            self._send_error(client, f'{cmd_or_target} {e}')

    def _send_error(self, client: Client, msg: str, from_user: str = SERVER_USER,
                    kill_if_full: bool = False):
        self._send_private(client, msg, from_user, prefix=PREFIX_ERROR, kill_if_full=kill_if_full)

    def _send_private(self, client: Client, msg: str, from_user: str = SERVER_USER,
                      prefix: str = PREFIX_DIRECT, kill_if_full: bool = False):
        """Send a private message to a client we already have in hand (non-blocking).

        The kill_if_full flag picks the policy for a full send queue:

        - True: signal the client to die (their own handler thread
          cleans up). Use for real chat messages, so a slow client
          reconnects rather than silently missing conversation.
        - False: quietly drop the message. Use for server replies to
          the client's own input - those can be generated faster than
          any client could possibly read them (e.g. error replies to a
          flood of bad messages), so a full queue there is not the
          client's fault and must not get them killed.
        """
        formatted_msg = format_msg(prefix, from_user, msg)
        try:
            client.send(encode(formatted_msg))
        except queue.Full:
            if kill_if_full:
                log.warning(f'{client!r} send queue full - killing slow client')
                client.kill()
            return
        log.info(f'[To {client.name}] {formatted_msg}') # Private message

    def direct_send(self, to_user: str, msg: str, from_user: str = SERVER_USER):
        """Send a private message to a user, looked up by name (non-blocking).

        Raises ChatError if the user does not exist.
        """
        # Don't send message contents that are just whitespace
        if not msg.strip():
            return

        with self.lock:
            c = self.clients.get(to_user)
        if c is None:
            raise ChatError(f'No user named {to_user}')

        self._send_private(c, msg, from_user, kill_if_full=True)

    def broadcast(self, msg: str, from_user: str = SERVER_USER, roomname: str = DEFAULT_ROOM):
        """Send a message to all users in a room.

        Raises ChatError if room does not exist, or if user is not in the room.
        """
        # Don't send message contents that are just whitespace
        if not msg.strip():
            return

        formatted_msg = format_msg('#' + roomname, from_user, msg)
        encoded_msg = encode(formatted_msg)
        slow_clients = []
        with self.lock:
            # Check post is valid: you may post to any room you are
            # allowed in, even one you have not joined (only joined
            # users receive the message, though)
            if roomname not in self.rooms:
                raise ChatError(f'is not a valid room') # Room name is prefixed
            room = self.rooms[roomname]
            if room.public:
                if from_user in room.banned:
                    raise ChatError(f'{from_user} not allowed to post to {roomname}')
            elif from_user not in room.allowed:
                raise ChatError(f'{from_user} not allowed to post to {roomname}')

            for name in room.present:
                c = self.clients.get(name)
                if c is None:
                    continue # e.g. SERVER_USER, which has no Client
                try:
                    c.send(encoded_msg)
                except queue.Full:
                    slow_clients.append(c)

        log.info(formatted_msg)

        # Signal slow clients to die (outside the lock). Each one's own
        # handler thread does the cleanup and announcements.
        for c in slow_clients:
            log.warning(f'{c!r} send queue full - killing slow client')
            c.kill()


class Commander:
    """Command structure, allowing automatic help string population.

    Command methods are named "cmd_xxx". They return a tuple
    (reply_msg, quit), where reply_msg is sent back to the caller, and
    the quit flag is set if the command should gracefully exit the
    client. Optionally, the command functions can return just reply_msg,
    and quit is assumed False.
    """
    QUIT_SIGNAL = object()

    def __init__(self, server):
        self.server = server

    def commands(self) -> list[tuple[str, str]]:
        "Returns a list of (command, helpstring) for all supported commands"
        commands = []
        # Reverse list, because dir seems to return in reverse order
        #for method in reversed(dir(self)):
        for method in self.__class__.__dict__:
            if method.startswith('cmd_'):
                commands.append((method.replace('cmd_', ''),
                                 getattr(self, method).__doc__.split('\n')[0]))
        return commands

    def handle(self, command, payload, client_name):
        """Handle command from client. Raises ChatError if command is unknown.

        Returns (reply, do_quit), where do_quit is True if client is
        requesting to quit gracefully.
        """
        # Note: command echo is handled elsewhere - just give reply
        args = payload.split()
        fn_name = 'cmd_' + command
        if hasattr(self, fn_name):
            reply = getattr(self, fn_name)(client_name, *args)
            if isinstance(reply, tuple):
                return reply
            return (reply, False)
        else:
            raise ChatError(f'Unknown command')

    def cmd_help(self, client_name, *args):
        "Lists all available commands"
        return '\n' + '\n'.join([f'/{c} {h}' for c, h in self.commands()])

    def cmd_quit(self, client_name, *args):
        "Gracefully exit"
        return (f'Goodbye {client_name}', True)

    def cmd_echo(self, client_name, *args):
        "Echo back any arguments (spaces are condensed)"
        return ' '.join(args)

    def cmd_version(self, client_name, *args):
        "Returns the version of DadChat, as x.y.z"
        return VERSION

    def cmd_users(self, client_name, *args):
        "Lists all connected users; `/users roomname` lists users in a room"
        if args:
            names = self.server.users_in_room(plain(args[0]))
        else:
            names = self.server.users()
        return ' '.join(names)

    def cmd_rooms(self, client_name, *args):
        "Lists all available rooms with room admins (creator in parentheses)"
        # Start each line with a dash, since a line starting with "#room"
        # looks like a message sent to that room
        rooms = [' '.join([f'- #{name} ({creator})'] + admins)
                 for name, creator, admins in self.server.room_info()]
        return '\n' + '\n'.join(rooms)

    def cmd_join(self, client_name, *args):
        "Join the specified room - immediately if allowed; otherwise reports error"
        if len(args) < 1:
            raise ChatError('No room specified')
        self.server.join_room(plain(args[0]), client_name)
        return f'You are now a member of {args[0]}'

    def _notify(self, users, msg):
        """Privately send msg to each user. Returns the users who are not connected."""
        absent_users = []
        for user in users:
            try:
                self.server.direct_send(user, msg)
            except ChatError:
                absent_users.append(user)
        return absent_users

    def _create_room(self, client_name, args, public):
        if len(args) < 1:
            raise ChatError('No room name specified')
        roomname = plain(args[0])
        self.server.new_room(roomname, client_name, public=public)
        reply = f'#{roomname} created and joined'
        if len(args) > 1:
            # Any users listed after the room name get invited
            reply += '. ' + self.cmd_invite(client_name, *args)
        return reply

    def cmd_newroom(self, client_name, *args):
        """Create a new private room with given name. Additional arguments are allowed users.

        Leading `#` character is optional. `roomname` must be 14 bytes encoded or fewer.
        Users listed are notified, automatically allowed into the room as non-admins, but
        not auto-joined. You (the room creator) are auto-joined to this room.
        """
        return self._create_room(client_name, args, public=False)

    def cmd_pubroom(self, client_name, *args):
        """Create a new public room with given name. Additional arguments are notified users.
        """
        return self._create_room(client_name, args, public=True)

    def _apply_to_each_user(self, client_name, args, action, done):
        """Shared by /invite, /promote, /demote and /kick, which all take
        `roomname user1 [...] [usern]` and do the room action to each user.

        `done` says what happened, e.g. 'kicked from'. Each user it worked
        for is told "You have been kicked from #kitchen by alice.", and the
        reply says "bob has been kicked from #kitchen". Some users can fail
        while others succeed (e.g., an admin can kick users, but not other
        admins), so the reply also has an error for each user it did not
        work for.
        """
        if len(args) < 2:
            raise ChatError('Need a room name and at least one user')
        roomname, users = plain(args[0]), args[1:]
        succeeded, errors = self.server.apply_to_each_user(roomname, action, client_name, users)
        if not succeeded:
            raise ChatError('. '.join(errors))
        absent_users = self._notify(succeeded, f'You have been {done} #{roomname} by {client_name}.')
        reply = '. '.join([f'{user} has been {done} #{roomname}' for user in succeeded] + errors)
        if absent_users:
            reply += f'. Users not currently present: {" ".join(absent_users)}.'
        return reply

    def cmd_invite(self, client_name, *args):
        """Invite users to a room (room admins only)

        `/invite roomname user1 [...] [usern]`. In a private room, this adds
        the users to the allowed list. In a public room, it lifts any ban.
        """
        return self._apply_to_each_user(client_name, args, 'invite', 'invited to join')

    def cmd_promote(self, client_name, *args):
        """Make users room admins (room creator only)

        `/promote roomname user1 [...] [usern]`. Room admins can invite
        users, and kick users who are not admins.
        """
        return self._apply_to_each_user(client_name, args, 'promote', 'promoted to admin of')

    def cmd_demote(self, client_name, *args):
        """Make room admins normal users again (room creator only)

        `/demote roomname user1 [...] [usern]`
        """
        return self._apply_to_each_user(client_name, args, 'demote', 'demoted to a normal user of')

    def cmd_kick(self, client_name, *args):
        """Remove users from a room (room admins only)

        `/kick roomname user1 [...] [usern]`. Only the room creator can kick
        an admin, which also takes away their admin status. Users kicked
        from a public room are banned from it until invited back.
        """
        return self._apply_to_each_user(client_name, args, 'kick', 'kicked from')

    def cmd_closeroom(self, client_name, *args):
        """Close a room you created (send twice, to be sure)

        `/closeroom roomname`. The second request has to come within
        CLOSE_CONFIRM_S seconds of the first. Everyone in the room is told it closed.
        """
        if len(args) < 1:
            raise ChatError('No room specified')
        roomname = plain(args[0])
        members = self.server.close_room(roomname, client_name)
        if members is None:
            return (f'Are you sure? Send `/closeroom #{roomname}` again within '
                    f'{CLOSE_CONFIRM_S} seconds to close it')
        others = [m for m in members if m != client_name]
        self._notify(others, f'#{roomname} was closed by {client_name}.')
        return f'#{roomname} closed'


def main():
    # We report through logging rather than print() because print() calls
    # from different threads can interleave mid-line (clobbered output),
    # while logging writes each line atomically. We also get a timestamp
    # and the emitting thread's name (which we set to recv-<port> /
    # send-<port> per client) on every line for free.
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(threadName)s %(levelname)s %(message)s')

    # Ctrl-C sends SIGINT, which Python turns into KeyboardInterrupt for
    # us. But `docker stop` (and most service managers) send SIGTERM
    # instead, which by default just kills the process. Turn SIGTERM
    # into the same KeyboardInterrupt so both shut down the same way.
    def request_shutdown(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, request_shutdown)

    chat = ChatServer()

    # TODO: heartbeat: one thread here would walk through chat.clients
    # and queue pings. Note: once that thread exists, it can also track
    # each client's last-activity time and kill() stale clients itself
    # (both idle clients and ones that never sent a username). That
    # would replace the deadline logic in blocking_read_messages
    # entirely - the reader would just block with no timeout at all.

    # Spawn a handler thread for each incoming connection.
    # Sets SO_REUSEADDR = 1 for you.
    with socket.create_server((HOST, PORT), backlog=BACKLOG) as srv:
        log.info(f'Started DadChat v{VERSION} server on {HOST}:{PORT}')
        while True:
            try:
                sock, peer = srv.accept()
            except OSError:
                # Most likely out of file descriptors during a
                # connection flood - pause briefly instead of crashing
                time.sleep(0.1)
                continue
            try:
                threading.Thread(target=chat.handle_client, args=(sock, peer),
                                 daemon=True, name=f'recv-{peer[1]}').start()
            except RuntimeError:
                # Out of threads - drop this connection instead of crashing
                sock.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info('Shutting down')
