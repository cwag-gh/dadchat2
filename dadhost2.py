# Main server, in an async style

import asyncio


ENCODING = 'utf-8'
MSG_START = b'\x02'
MSG_END   = b'\x03'
MAX_LEN = 1024


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


def encode(s):
    """Encodes message into our format."""
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


def valid_username(s: str, existing_users: set[str]):
    """Tries to create a valid username from s. Raises PermissionError with reason if unable."""
    s = s.strip()
    s = remove_chars(s, '/*#: \t\r\n')
    s = truncate_to_encoded_length(s, 14)

    if len(s) < 3:
        raise PermissionError('username too short (< 3 characters)')
    if s.lower() in {'private', 'general', 'dadserver'}:
        raise PermissionError('username a controlled term - not allowed')
    if not any(c.isalpha() for c in s):
        raise PermissionError('username must include at least one letter')
    if s in existing_users:
        raise PermissionError('username already taken')

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
        self.present.remove(kickee)
        if self.public:
            self.banned.add(kickee)
        else:
            self.allowed.remove(kickee)

    def invite(self, inviter: str, candidate: str):
        if inviter not in self.admins:
            raise PermissionError(f'{inviter} not an admin - not allowed to invite {candidate}')
        if self.public:
            if candidate in self.banned:
                self.banned.remove(candidate)
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
        self.present.remove(user)

    def promote(self, promoter: str, candidate: str):
        if promoter != self.creator:
            raise PermissionError(f'Only the room creator has permission to promote')
        if not self.public and candidate not in self.allowed:
            raise PermissionError(
                f'{candidate} must first be allowed into #{self.name} before promotion to admin')
        self.admins.add(candidate)


async def close_connection(writer: asyncio.StreamWriter) -> None:
    """Close a socket that may be broken."""
    # Safe way to close a socket no matter the socket state
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), CLOSE_TIMEOUT)
    except (OSError, asyncio.TimeoutError):
        pass


class ChatServer:
    """Keep track of connected users and rooms."""

    def __init__(self):
        self.users = set() # Set of users
        self.rooms = dict() # map of room name to Room

    async def handle_client(self, reader, writer):
        parser = MessageParser()
        username = None
        addr = writer.get_extra_info('peername')

        print(f'Got a connection from {addr}')

        def send(msg):
            encoded_msg = encode(msg)
            writer.write(encoded_msg)

        while True:
            data = await reader.read(1024)
            msgs = parser.append_and_parse(data)
            for msg in msgs:
                if username is None:
                    # First message is the username
                    print(f'Checking if {msg} is a valid username')
                    try:
                        username = valid_username(msg)
                    except PermissionError as e:
                        send(f'PRIVATE *DadServer*: {str(e)}')
                        break
                    print(f'{addr} is now user {username}')
                    self.users.add(username)
                else:
                    # Handle normal message
                    # Just echo for now
                    send(msg)

            # Send all queued messages
            await writer.drain()

            if msgs and username is None:
                break

        print(f'Closing connection to {addr} ({username})')
        if username is not None:
            self.users.remove(username)
        writer.close()
        await writer.wait_closed()


async def main():
    chat = ChatServer()
    server = await asyncio.start_server(
        chat.handle_client, '0.0.0.0', 12347)

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        await server.serve_forever()


asyncio.run(main())
