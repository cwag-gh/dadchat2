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
#   client list and the room list. We don't want to check
#   to see if a username is available, notice that it is, then assign
#   it; but between the check and assignment, another client thread
#   was doing the same check, and we end up with two client with the
#   same username. For situations like this, we use a lock (mutex).
#   This can be surprisingly tricky to get right!
#
# TODO:
# - add ping/pong keepalive
# - add message rate limiting

import threading
import queue
import socket

HOST = '0.0.0.0'
PORT = 12347
ENCODING = 'utf-8'
MSG_START = b'\x02'
MSG_END   = b'\x03'
MAX_LEN = 1024
BACKLOG = 32
HANDSHAKE_TIMEOUT_S = 10
IDLE_TIMEOUT_S = 15*60
SERVER_USER = '*DadServer*'
STOP = object()  # Unique object we use to tell a write thread to finish
DEFAULT_ROOM = 'general'
DIRECT_PREFIX = 'PRIVATE'



class MessageParser:
    """Convert byte stream into a series of messages."""

    def __init__(self):
        self.buf = b''

    def append_and_parse(self, new_bytes, n=-1):
        """Returns a list of complete message payloads. Returns max n messages."""
        self.buf += new_bytes
        return self.parse(n=n)

    def parse(self, n=-1):
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
            if 1 < sidx < idx:
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
            if n > 0 and len(msgs) >= n:
                return msgs


def blocking_read_pending_messages(sock, parser, timeout_s, n=-1):
    """Read and return up to n messages, or None if timed out"""
    t_start = time.perf_counter()
    sock.settimeout(timeout_s / 2.0)
    msgs = []
    while (time.perf_counter() - t_start) < timeout_s:
        try:
            chunk = sock.read(1024) # Blocking
        except socket.timeout:
            pass # Handled at the while loop
        if not chunk:
            break
        msgs.extend(parser.append_and_parse(chunk, n))
        if (n > 0 and len(msgs) >= n) or (n < 0 and msgs):
            return msgs
    return None


def blocking_read_pending_message(sock, parser, timeout_s):
    """Read and return a single message, or None if timed out."""
    msgs = blocking_read_pending_messages(sock, parser, timeout, 1)
    if msgs is not None:
        assert len(msgs) == 1
        return msgs[0]
    return None


def format_msg(target: str, source: str, msg: str):
    """Adds header to our message."""
    return ' '.join([target, from_user + ':', msg])


def encode(s):
    """Encodes message into our byte format."""
    s = s[:(MAX_LEN-2)]
    return MSG_START + s.encode(ENCODING, errors='ignore') + MSG_END


def remove_chars(s, chars: str):
    return ''.join(c for c in s if c not in chars)


def truncate_to_encoded_length(s, n):
    s = s[:n]
    while len(s.encode(ENCODING, errors='ignore')) > n:
        s = s[:(len(s)-1)]
    return s


class PermissionError(RuntimeError): pass


def valid_username(s: str):
    return valid_name(s, 'user', 14)


def valid_roomname(s: str):
    return valid_name(s, 'room', 14)


def valid_name(s: str, nametype: str, max_encoded_len: int):
    """Checks to see if s is valid name. Raises PermissionError with reason if unable."""
    if s != s.strip():
        raise PermissionError(f'{nametype}name has leading or trailing whitespace')
    if s != remove_chars(s, '/*#: \t\r\n'):
        raise PermissionError(f'{nametype}name contains invalid characters')
    if s != truncate_to_encoded_length(s, max_encoded_len):
        raise PermissionError(f'{nametype}name exceeds max encoded length ({max_encoded_len})')
    if len(s) < 3:
        raise PermissionError(f'{nametype}name too short (< 3 characters)')
    if s.lower() in {'private',
                     DEFAULT_ROOM.lower(),
                     SERVER_USER.lower()}:
        raise PermissionError(f'{nametype}name a controlled term - not allowed')
    if not any(c.isalpha() for c in s):
        raise PermissionError(f'{nametype}name must include at least one letter')
    return s


class Room:
    def __init__(self, name: str, creator: str, public=False):
        self.name: str = name
        self.public: bool = public
        self.creator: str = creator
        self.admins: set[str] = {self.creator}
        self.allowed: set[str] = set(self.admins) # Only used when public = False
        self.banned: set[str] = set()             # Only used when public = True
        self.present: set[str] = set(self.admins)

    def kick(self, kicker: str, kickee: str):
        if kicker not in self.admins:
            raise PermissionError(f'{kicker} not an admin - not allowed to kick {kickee}')
        self.present.discard(kickee)
        if self.public:
            self.banned.add(kickee)
        else:
            self.allowed.discard(kickee)

    def invite(self, inviter: str, candidate: str):
        if inviter not in self.admins:
            raise PermissionError(f'{inviter} not an admin - not allowed to invite {candidate}')
        if self.public:
            if candidate in self.banned:
                self.banned.discard(candidate)
        else:
            self.allowed.add(candidate)

    def join(self, user: str):
        if self.public:
            if user in self.banned:
                raise PermissionError(f'{user} has been banned from #{self.name}')
        else:
            if user not in self.allowed:
                raise PermissionError(f'{user} does not have permission to join #{self.name}')
        self.present.add(user)

    def leave(self, user: str):
        self.present.discard(user)

    def promote(self, promoter: str, candidate: str):
        if promoter != self.creator:
            raise PermissionError(f'Only the room creator has permission to promote')
        if not self.public and candidate not in self.allowed:
            raise PermissionError(
                f'{candidate} must first be allowed into #{self.name} before promotion to admin')
        self.admins.add(candidate)

    def demote(self, demoter: str, candidate: str):
        if demoter != self.creator:
            raise PermissionError(f'Only the room creator has permission to demote')
        self.admins.discard(candidate)

    def drop(self, name):
        """Remove all references to this name in this room.

        Does not change the creator - this should be done elsewhere.
        """
        self.admins.discard(name)
        self.allowed.discard(name)
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

    def ready_to_accept_messages(self):
        return self.name is not None  # This is the definition of ready

    def send(self, msgbytes: bytes):
        """Enqueue an already encoded message to send (assumes client threads are all running).

        Raises queue.Full if queue is full.
        """
        self.messages_to_send.put_nowait(msgbytes)

    def kill_before_full_setup_with_message(self, msg):
        """Shut down the client with a message. Assumes other threads not set up yet."""
        msg = ' '.join([DIRECT_PREFIX, SERVER_USER, msg])
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
        """Stop both threads associated with the client.

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
        # You MUST acquire this lock before changing information in clients or rooms
        self.lock = threading.Lock()
        self.clients = set() # Set of Clients
        self.rooms = dict() # map of room name to Room

        self.rooms[DEFAULT_ROOM] = Room(DEFAULT_ROOM, SERVER_USER, public=True)

    def get_client(self, name):
        with self.lock:
            for c in self.clients:
                if c.name == name:
                    return client
        return None

    def drop_client(self, name):
        """Removes a client, and removes them from all rooms.

        Also removes rooms they created.

        Returns a tuple of (was dropped from client list, list of room names closed).
        """
        was_dropped = False
        dropped_rooms = []
        with self.lock:
            client = self.get_client(name)
            if client is not None:
                self.clients.discard(client)
                was_dropped = True

            for roomname, room in rooms.iter():
                if room.creator == name:
                    dropped_rooms.append(roomname)
                    del self.rooms[roomname]
                else:
                    room.drop(name)
            return was_dropped, dropped_rooms

    def report_drop(self, name, was_dropped, dropped_rooms):
        """Broadcast if someone left, and/or if rooms closed.

        Note that is can be recursive, since broadcast can cause drops.
        """
        msg = ''
        if name and was_dropped:
            msg = f'{name} left.'
        if dropped_rooms:
            msg += f' Rooms closed: {str(dropped_rooms)}'
        msg = msg.strip()
        if msg:
            self.broadcast(msg)

    def add_room(self, room_name, creator, public=False):
        room_name = valid_roomname(room_name) # Raises permission error
        room = Room(room_name, creator, public)
        with self.lock:
            allowed = room_name not in self.rooms
            if allowed:
                self.rooms[room_name] = room
        if not allowed:
            raise PermissionError(f'Room {room_name} already created')

    def handle_client(self, sock, peer):
        """Handle client connection - one per client connection on own thread"""
        with sock:
            # Guarantee socket close no matter what
            self._handle_client(sock, peer)

    def _handle_client(self, sock, peer):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        client = Client(sock, peer)
        write_thread = threading.Thread(target=self.write_loop, args=(client,),
                                        daemon=True, name=f'send-{peer[1]}')

        print(f'Got a connection from {peer}')
        with self.lock:
            # With the lock, this guarantees we will never go above the max number of clients
            admit = len(self.clients) < MAX_CLIENTS
            if admit:
                self.clients.add(client)
            count = len(self.clients)
        if not admit:
            print(f'Rejecting {peer} since we are at capacity')
            client.kill_before_full_setup_with_message('Server is full, try again later')

        try:
            # Start the writing thread (before checking username validity) to avoid
            # race condition between when client is fully accepted (with username
            # grabbed) and initial messages trying to be broadcast.
            write_thread.start() # Can throw RuntimeError if out of threads

            # Handle the initial username retrieval
            parser = MessageParser()
            msg = blocking_read_pending_message(sock, parser, HANDSHAKE_TIMEOUT_S)
            if msg is None:
                client.kill_before_full_setup_with_message(f'No username received in time')
                return
            print(f'Checking if {msg} is a valid username')
            try:
                msg = valid_username(msg)
            except PermissionError as e:
                client.kill_before_full_setup_with_message(str(e))
                return
            # Check and claim username in one go
            with self.lock:
                username_taken = any(c.name == username for c in self.clients)
                if not username_taken:
                    client.name = username
                    # Also add to the general room
                    self.rooms[DEFAULT_ROOM].present.add(username)
            if username_taken:
                client.kill_before_full_setup_with_message(f'username {username} already in use')
                return
            self._server_send(client, f'Welcome {username}!')
            # Now that the client has a username, all of the normal messaging functions work.
            self.broadcast(f'{username} has joined')

            # Carry on with the normal read loop
            while True:
                msgs = blocking_read_pending_message(client.sock, parser, IDLE_TIMEOUT_S)
                if msgs is None:
                    # Read timed out. So, let this client go.
                    return
                for msg in msgs:
                    self.dispatch(msg, client)
        except OSError as e:
            print(f'ERROR: {client} socket error: {e}')
        except Exception as e:
            print(f'ERROR: {client} unhandled error: {e}')   # incl. failed start
        finally:
            username = client.username
            if not username:
                # Never assigned a name, just discard the client
                with self.lock:
                    self.clients.discard(client)
            else:
                was_dropped, dropped_rooms = self.drop_client(username)
                self.report_drop(username, was_dropped, dropped_rooms)
            print('Disconnect %r', client)
            client.kill()                # stops the sender thread
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

        if msg.startswith('/'):
            # TODO: handle command
            cmd, payload = splitmsg(msg)
            self.direct_send(client.name, f'Unknown command {cmd}')
        elif msg.startswith('#'):
            # Message to a room
            target_room, payload = splitmsg(msg)
            try:
                self.broadcast(target_room, payload, client.name)
            except PermissionError as e:
                self._server_send(client, str(e))
        elif msg.startswith(':'):
            # Direct message to a user
            target, payload = splitmsg(msg)
            try:
                self.direct_send(target, payload, client.name)
            except PermissionError as e:
                self._server_send(client, str(e))
        else:
            # Message to default room
            self.broadcast(msg, client.name)

    def _server_send(self, client: Client, msg: str):
        """Send message to existing client, who may already be removed from rooms.

        Silently eats queue.Full errors - use this for error responses. Does not
        try to kill clients, so does not set off recursive chain of drop announcements.
        """
        try:
            client.send(encode(format_msg(DIRECT_PREFIX, SERVER_USER, msg)))
        except queue.Full:
            pass

    def direct_send(self, to_user: str, msg: str, from_user: str = SERVER_USER):
        """Directly send a message to a user (non-blocking).

        Can raise permission error (TODO).

        Use this instead of messaging client directly so we can manage
        culling clients if their queue is full.
        """
        # Don't send message contents that are just whitespace
        if not msg.strip():
            return

        formatted_msg = format_msg(DIRECT_PREFIX, from_user, msg)
        encoded_msg = encode(formatted_msg)

        drop_info = None
        with self.lock:
            # TODO: Check send is valid
            # raise PermissionError(f'{from_user} not allowed to post to {to_user}')

            # Send the message
            c = self.get_client(to_user)
            try:
                c.send(encoded_message)
            except queue.Full:
                # Drop slow client
                was_dropped, rooms_closed = self.drop_client(c.name)
                drop_info = (c, was_dropped, rooms_closed)

        print(f'[To {to_user}] {formatted_msg}') # Private message

        # Actually kill client and report
        if drop_info:
            c, was_dropped, rooms_closed = drop_info
            c.kill()
            self.report_drop(c.name, was_dropped, dropped_rooms)

    def broadcast(self, msg: str, from_user: str = SERVER_USER, roomname: str = DEFAULT_ROOM):
        """Send a message to all users in a room.

        Raises permission error if room does not exist, or if user is not allowed to
        post to room.
        """
        # Don't send message contents that are just whitespace
        if not msg.strip():
            return

        formatted_msg = format_msg('#' + roomname, from_user, msg)
        encoded_msg = encode(formatted_msg)
        clients_to_drop = []
        dropped_info = []
        with self.lock:
            # Check post is valid
            if roomname not in self.rooms:
                raise PermissionError(f'{roomname} is not valid room')
            room = self.rooms[roomname]
            if not room.public and from_user not in room.allowed:
                raise PermissionError(f'{from_user} not allowed to post to {roomname}')

            # Try posting and accumulate any clients that need to be dropped
            for c in self.clients:
                if c.name in room.present:
                    try:
                        c.send(encoded_message)
                    except queue.Full:
                        clients_to_drop.append(c)

            # Drop clients that need to be dropped, accumulating info
            for c in clients_to_drop:
                was_dropped, rooms_closed = self.drop_client(c.name)
                dropped_info.append((c.name, was_dropped, rooms_closed))

        print(formatted_msg)

        # Actually kill clients
        for c in clients_to_drop:
            c.kill()

        # Report cullings
        for name, was_dropped, dropped_rooms in dropped_info:
            self.report_drop(name, was_dropped, dropped_rooms)


def main():
    chat = ChatServer()

    # TODO: heartbeat: one thread here would walk through chat.clients
    # and queue pings.

    # Spawn a handler thread for each incoming connection.
    # Sets SO_REUSEADDR = 1 for you.
    with socket.create_server((HOST, PORT), backlog=BACKLOG) as srv:
        print(f'Started DadChat v2 server on {HOST}:{PORT}')
        while True:
            sock, peer = srv.accept()
            threading.Thread(target=chat.handle_client, args=(sock, peer),
                             daemon=True, name=f'recv-{peer[1]}').start()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('\nShutting down')
