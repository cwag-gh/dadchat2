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
#
# TODO:
# - add ping/pong keepalive
# - add message rate limiting

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
SERVER_USER = '*DadServer*'
STOP = object()  # Unique object we use to tell a write thread to finish
DEFAULT_ROOM = 'general'
DIRECT_PREFIX = 'PRIVATE'


class ChatError(RuntimeError):
    """A client request that is not allowed. """


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
    if s.lower() in {DIRECT_PREFIX.lower(),
                     DEFAULT_ROOM.lower(),
                     SERVER_USER.lower(),
                     SERVER_USER.lower().strip('*')}:
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
        self.present: set[str] = set(self.creator) # Default to creator present

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
            self.sock.sendall(encode(format_msg(DIRECT_PREFIX, SERVER_USER, msg)))
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


def splitmsg(msg):
    """Splits a message with a prefix character.

    Returns the prefix word _without_ the prefix character, and the
    rest of the message after the space.
    """
    if not msg:
        return '', ''

    i_space = msg.find(' ')
    if i_space < 0:
        return msg[1:], ''

    return msg[1:i_space], msg[(i_space+1):]


class ChatServer:
    """Keep track of connected users and rooms."""

    def __init__(self):
        # You MUST acquire this lock before touching clients, rooms,
        # or num_connected
        self.lock = threading.Lock()
        self.clients = {}      # map of username to Client (only fully set up clients)
        self.num_connected = 0 # all open connections, including pre-handshake ones
        self.rooms = dict()    # map of room name to Room

        self.rooms[DEFAULT_ROOM] = Room(DEFAULT_ROOM, SERVER_USER, public=True)

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

    def add_room(self, room_name, creator, public=False):
        room_name = valid_roomname(room_name) # Raises ChatError
        room = Room(room_name, creator, public)
        with self.lock:
            allowed = room_name not in self.rooms
            if allowed:
                self.rooms[room_name] = room
        if not allowed:
            raise ChatError(f'Room {room_name} already created')

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
            # Peer is gone. Break the reader too, so its thread stops waiting
            # for the idle timeout while this queue silently fills.
            client.kill()

    def dispatch(self, msg, client):
        """Dispatch a message from a client. Handle all commands."""
        # Silently drop messages of zero length
        if len(msg) == 0:
            return

        try:
            if msg.startswith('/'):
                # TODO: handle commands - /help /quit /join /newroom /pubroom
                # /invite /promote /kick /closeroom /rooms /users
                cmd, payload = splitmsg(msg)
                self._send_private(client, f'Unknown command {cmd}')
            elif msg.startswith('#'):
                # Message to a room
                target_room, payload = splitmsg(msg)
                self.broadcast(payload, client.name, target_room)
            elif msg.startswith(':'):
                # Direct message to a user
                target, payload = splitmsg(msg)
                self.direct_send(target, payload, client.name)
            else:
                # Message to default room
                self.broadcast(msg, client.name)
        except ChatError as e:
            self._send_private(client, str(e))

    def _send_private(self, client: Client, msg: str, from_user: str = SERVER_USER,
                      kill_if_full: bool = False):
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
        formatted_msg = format_msg(DIRECT_PREFIX, from_user, msg)
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
                raise ChatError(f'{roomname} is not valid room')
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
        log.info(f'Started DadChat v2 server on {HOST}:{PORT}')
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
