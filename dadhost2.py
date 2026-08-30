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


def valid_username(s: str):
    """Creates a valid username from s. Returns None if invalid."""
    s = s.strip()
    s = remove_chars(s, '/*#: \t\r\n')
    s = truncate_to_encoded_length(s, 14)

    if len(s) < 3:
        return None
    if s.lower() in {'private', 'general', 'dadserver'}:
        return None
    if not any(c.isalpha() for c in s):
        return None

    return s


class Server:
    """Keep track of connected users and rooms."""
    def __init__(self):
        self.users = set() # Set of users

    async def handle(self, reader, writer):
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
                    username = valid_username(msg)
                    if username is None:
                        send('PRIVATE *DadServer*: Invalid username')
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
    server = Server()
    server_stream = await asyncio.start_server(
        server.handle, '0.0.0.0', 12347)

    addrs = ', '.join(str(sock.getsockname()) for sock in server_stream.sockets)
    print(f'Serving on {addrs}')

    async with server_stream:
        await server_stream.serve_forever()


asyncio.run(main())
