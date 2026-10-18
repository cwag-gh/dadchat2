# dadclient_advanced.py - a fancier chat client that works on Mac AND Windows.
#
# WHAT IT DOES
# The basic client (dadclient.py) has an annoyance: if a message
# arrives while you are halfway through typing, it gets printed right
# into the middle of your half-typed line. This client fixes that by
# taking over the whole terminal window using `curses` (a
# screen-drawing library): messages fill the top of the screen, and
# the bottom line belongs to your typing. They can never collide.
#
# THE HARD PART: WAITING ON TWO THINGS AT ONCE
# This program waits for two completely different things:
#   1. the server (a message could arrive at any moment)
#   2. your keyboard (you could press a key at any moment)
# and it should SLEEP until one of them happens. No checking over and
# over in a loop - that is called polling, and it wastes battery and
# adds lag.
#
# The tool for "sleep until one of these has something for me" is
# called select(). Problem: on Mac and Linux, select() can watch
# sockets AND the keyboard. On Windows, select() can ONLY watch
# sockets. It flat out refuses to watch a keyboard.
#
# THE TRICK: TURN THE KEYBOARD INTO A SOCKET
# If Windows select() only understands sockets, fine - we make the
# keyboard LOOK like a socket. socketpair() gives us two sockets that
# are connected to each other, like a phone line between two rooms of
# our own program. We start one tiny helper thread whose entire job
# is: wait for a keypress, push it into one end of the phone line.
# Now our main loop asks select() to watch two SOCKETS - the server,
# and the other end of the phone line - which is allowed everywhere.
#
# This is a real professional trick (event systems use it under the
# hood), and even asyncio, if you ask it to read the keyboard on
# Windows, quietly starts a helper thread just like ours. Also notice:
# only the main loop ever draws on the screen. The helper thread just
# moves keys, so the two threads never fight over anything shared.
#
# ON WINDOWS, ONE-TIME SETUP:  pip install windows-curses
#
# Run it from this folder:  python3 dadclient_advanced.py <username> [host]
#
# Ideas to add yourself: colors, scrolling back through old messages,
# arrow keys to edit your line, nicer handling of window resizing.

import codecs
import locale
import select
import socket
import sys
import threading

try:
    import curses
except ModuleNotFoundError:
    print('curses is missing. On Windows, run this first:  pip install windows-curses')
    raise SystemExit(1)

from dadhost2 import (ENCODING, MessageParser, encode, valid_username,
                      ChatError, connect_to_server)

WINDOWS = (sys.platform == 'win32')


def keyboard_to_socket(out_sock):
    """Helper thread: read keypresses, push them into our 'phone line' socket."""
    try:
        if WINDOWS:
            import msvcrt
            while True:
                ch = msvcrt.getwch()       # Wait for one keypress
                if ch in ('\x00', '\xe0'):
                    msvcrt.getwch()        # Arrow/function keys come in 2 parts; skip both
                    continue
                out_sock.sendall(ch.encode(ENCODING))
        else:
            import os
            while True:
                data = os.read(0, 1024)    # 0 means "the keyboard", as raw bytes
                if not data:
                    return                 # Keyboard input was closed
                out_sock.sendall(data)
    except OSError:
        pass  # The main loop closed things down; quietly stop
    finally:
        out_sock.close()


class ChatUI:
    """Draws the screen: messages on top, your typing on the bottom line."""

    def __init__(self, stdscr):
        self.stdscr = stdscr
        self.messages = []
        self.typed = ''
        self.redraw()

    def redraw(self):
        try:
            self.stdscr.erase()
            height, width = self.stdscr.getmaxyx()
            # Show the newest messages that fit above the typing line
            first = max(0, len(self.messages) - (height - 1))
            for row, message in enumerate(self.messages[first:]):
                self.stdscr.addnstr(row, 0, message, width - 1)
            self.stdscr.addnstr(height - 1, 0, '> ' + self.typed, width - 1)
            self.stdscr.refresh()
        except curses.error:
            pass  # Screen can hiccup mid-resize; we'll redraw on the next event

    def add_message(self, message):
        self.messages.append(message)
        self.redraw()

    def handle_char(self, ch):
        """Take one typed character. Returns the whole line when Enter is hit."""
        if ch in ('\n', '\r'):             # Enter (Mac sends \n, Windows \r)
            line, self.typed = self.typed, ''
            self.redraw()
            return line
        if ch in ('\x7f', '\x08'):         # Backspace (Mac sends \x7f, Windows \x08)
            self.typed = self.typed[:-1]
        elif ch.isprintable():
            self.typed += ch
        self.redraw()
        return None


def run(stdscr, server_sock, keys_sock):
    ui = ChatUI(stdscr)
    parser = MessageParser()
    # A typed character can arrive split across two chunks; this
    # decoder remembers the first half until the rest shows up
    decoder = codecs.getincrementaldecoder(ENCODING)('ignore')
    ui.add_message('Type a message. ":user msg" = private, "#room msg" = room, /quit = exit.')

    while True:
        # Sleep until the server or the keyboard has something for us
        readable, _, _ = select.select([server_sock, keys_sock], [], [])

        if server_sock in readable:
            try:
                chunk = server_sock.recv(1024)
            except OSError:
                chunk = b''                # Connection broke - same as hanging up
            if not chunk:                  # The server hung up on us
                ui.add_message('-- disconnected (press any key to exit) --')
                select.select([keys_sock], [], [])
                return
            for message in parser.append_and_parse(chunk):
                ui.add_message(message)

        if keys_sock in readable:
            chunk = keys_sock.recv(1024)
            if not chunk:
                return                     # Keyboard helper ended
            for ch in decoder.decode(chunk):
                if ch == '\x03':           # Ctrl-C (Windows delivers it as a key)
                    return
                line = ui.handle_char(ch)
                if line == '/quit':
                    # TODO: send to the server once it implements commands
                    return
                if line:
                    try:
                        server_sock.sendall(encode(line))
                    except OSError:
                        pass  # Server is gone; the recv side will tell us


def main():
    if len(sys.argv) < 2:
        print(f'usage: {sys.argv[0]} <username> [host]')
        return 1
    username = sys.argv[1]
    host = sys.argv[2] if len(sys.argv) > 2 else '127.0.0.1'

    try:
        valid_username(username)  # Same check the server does - fail fast
    except ChatError as e:
        print(e)
        return 1

    locale.setlocale(locale.LC_ALL, '')  # Lets the screen show emoji etc.

    with connect_to_server(host) as server_sock:
        server_sock.sendall(encode(username))  # First message = our username

        # The "phone line": two sockets connected to each other
        keys_recv, keys_send = socket.socketpair()
        threading.Thread(target=keyboard_to_socket, args=(keys_send,),
                         daemon=True).start()  # daemon = dies with the program

        try:
            # wrapper() sets up the screen, and puts the terminal back
            # to normal when we're done - even if we crash
            curses.wrapper(run, server_sock, keys_recv)
        except KeyboardInterrupt:
            pass  # Ctrl-C also quits
    return 0


if __name__ == '__main__':
    sys.exit(main())
