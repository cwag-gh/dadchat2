"""Tests for the DadChat server.

Run them all from the project folder with:

    uv run python -m unittest
"""
import socket
import threading
import unittest

from dadhost2 import (ChatError, ChatServer, MessageParser, VERSION, encode,
                      valid_roomname, valid_username)


class TestValidNames(unittest.TestCase):

    def test_good_names_are_accepted(self):
        good_names = [
            'zeke',            # ordinary name
            'abc',             # shortest allowed
            'a' * 14,          # longest allowed
            'kid_42',          # numbers and underscores are fine
            'rex🦖',            # emoji are fine too
        ]
        for name in good_names:
            self.assertEqual(valid_username(name), name)
            self.assertEqual(valid_roomname(name), name)

    def test_bad_names_are_rejected(self):
        bad_names = [
            'ab',              # too short
            'a' * 15,          # too long
            'abc🦖🦖🦖',        # too long: only 6 characters, but 15 bytes
            ' zeke',           # leading whitespace
            'zeke ',           # trailing whitespace
            'dad man',         # space in the middle
            'dad/man',         # forbidden character
            'ADMIN',           # reserved name (in any case)
            '123',             # no letters
        ]
        for name in bad_names:
            with self.assertRaises(ChatError, msg=name):
                valid_username(name)
            with self.assertRaises(ChatError, msg=name):
                valid_roomname(name)


def start_server():
    """Run a chat server in the background, like main() does. Returns its port."""
    chat = ChatServer()
    listener = socket.create_server(('127.0.0.1', 0))  # Port 0 means "pick any free port"

    def accept_loop():
        while True:
            sock, peer = listener.accept()
            threading.Thread(target=chat.handle_client, args=(sock, peer), daemon=True).start()

    threading.Thread(target=accept_loop, daemon=True).start()
    return listener.getsockname()[1]


class ChatClient:
    """Connects to the server and sends our username, just like dadclient.py."""

    def __init__(self, port, username):
        self.sock = socket.create_connection(('127.0.0.1', port))
        self.sock.settimeout(2)  # So a broken test fails instead of hanging forever
        self.parser = MessageParser()
        self.inbox = []
        self.send(username)

    def send(self, text):
        self.sock.sendall(encode(text))

    def receive(self):
        """Wait for the next message. Returns None if the server hung up."""
        while not self.inbox:
            chunk = self.sock.recv(1024)
            if not chunk:
                return None
            self.inbox = self.parser.append_and_parse(chunk)
        return self.inbox.pop(0)


class TestChatServer(unittest.TestCase):

    def test_commands(self):
        alice = ChatClient(start_server(), 'alice')
        self.addCleanup(alice.sock.close)
        self.assertEqual(alice.receive(), 'PRIVATE *DadServer*: Welcome alice!')
        self.assertEqual(alice.receive(), '#general *DadServer*: alice has joined')

        alice.send('/help')
        self.assertIn('/quit', alice.receive())  # Help is long, so just spot-check it

        conversation = [
            # alice sends        the server replies
            ('/version',         f'PRIVATE *DadServer*: /version {VERSION}'),
            ('/echo hi   there', 'PRIVATE *DadServer*: /echo hi there'),
            ('/users',           'PRIVATE *DadServer*: /users alice'),
            ('/newroom kitchen', 'PRIVATE *DadServer*: /newroom #kitchen created and joined'),
            ('/pubroom park',    'PRIVATE *DadServer*: /pubroom #park created and joined'),
            ('/users #kitchen',  'PRIVATE *DadServer*: /users alice'),
            ('/join #park',      'PRIVATE *DadServer*: /join You are now a member of #park'),
            ('/rooms',           'PRIVATE *DadServer*: /rooms \n#general *DadServer*\n#kitchen alice\n#park alice'),
            ('/nope',            'ERROR *DadServer*: /nope Unknown command'),
            ('/quit',            'PRIVATE *DadServer*: /quit Goodbye alice'),
        ]
        for request, reply in conversation:
            alice.send(request)
            self.assertEqual(alice.receive(), reply)

        self.assertIsNone(alice.receive())  # After /quit, the server hangs up


if __name__ == '__main__':
    unittest.main()
