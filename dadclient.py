# Simple command-line client for the DadChat v2 server.
#
# Mirrors the server's two-threads-per-connection design, from the
# other side of the socket:
# - the main thread reads lines you type and sends them
# - a reader thread receives messages and prints them
#
# (Messages that arrive while you are mid-typing will interleave with
# your typed text on screen - that's normal for a minimal client.)
#
# Run it from this directory (it imports from dadhost2_new.py):
#
#     python3 dadclient.py <username> [host]

import socket
import sys
import threading

from dadhost2 import (MessageParser, encode, valid_username, ChatError,
                      connect_to_server)


def read_loop(sock, dead):
    """Receive and print messages until the connection dies."""
    parser = MessageParser()
    while True:
        try:
            chunk = sock.recv(1024)  # Blocking
        except OSError:
            break  # Socket closed under us (e.g. main thread quit)
        if not chunk:
            break  # Server closed the connection
        for msg in parser.append_and_parse(chunk):
            print(msg)
    dead.set()
    print('-- disconnected (press Enter to exit) --')


def main():
    if len(sys.argv) < 2:
        print(f'usage: {sys.argv[0]} <username> [host]')
        return 1
    username = sys.argv[1]
    host = sys.argv[2] if len(sys.argv) > 2 else '127.0.0.1'

    try:
        valid_username(username)  # Same check the server runs - fail fast
    except ChatError as e:
        print(e)
        return 1

    with connect_to_server(host) as sock:
        # First message must be our username (see README)
        sock.sendall(encode(username))

        # The reader gets its own thread so incoming messages print
        # without waiting for you to finish typing. daemon=True means
        # it dies with the program.
        dead = threading.Event()
        threading.Thread(target=read_loop, args=(sock, dead),
                         daemon=True).start()

        print('Type a message, ":user msg" for a DM, "#room msg" for a room. /quit exits.')
        while not dead.is_set():
            try:
                line = input()
            except (EOFError, KeyboardInterrupt):
                break  # Ctrl-D / Ctrl-C also quit
            if line == '/quit':
                # TODO: send this to the server once it implements commands
                break
            if line:
                try:
                    sock.sendall(encode(line))
                except OSError:
                    break  # Connection died mid-send
    return 0


if __name__ == '__main__':
    sys.exit(main())
