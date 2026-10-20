"""Tests for the DadChat server.

Run them all from the project folder with:

    uv run python -m unittest
"""
import logging
import socket
import threading
import unittest

from dadhost2 import (ChatError, ChatServer, MessageParser, Room, VERSION, encode,
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


class TestRoom(unittest.TestCase):

    def test_creator_starts_in_the_room_as_admin(self):
        room = Room('kitchen', 'alice')
        self.assertIn('alice', room.present)
        self.assertIn('alice', room.admins)

    def test_private_room_is_invite_only(self):
        room = Room('kitchen', 'alice')  # Rooms are private unless public=True
        with self.assertRaises(ChatError):
            room.join('bob')
        room.invite('alice', 'bob')
        room.join('bob')
        self.assertIn('bob', room.present)

    def test_kicked_user_is_banned_from_public_room(self):
        room = Room('park', 'alice', public=True)
        room.join('bob')
        room.kick('alice', 'bob')
        self.assertNotIn('bob', room.present)
        with self.assertRaises(ChatError):
            room.join('bob')
        room.invite('alice', 'bob')  # An invite lifts the ban
        room.join('bob')

    def test_only_admins_can_invite_and_kick(self):
        room = Room('kitchen', 'alice')
        room.invite('alice', 'bob')
        with self.assertRaises(ChatError):
            room.invite('bob', 'carol')
        with self.assertRaises(ChatError):
            room.kick('bob', 'alice')

    def test_creator_can_promote_and_demote_admins(self):
        room = Room('kitchen', 'alice')
        room.invite('alice', 'bob')
        room.promote('alice', 'bob')
        room.invite('bob', 'carol')  # bob is an admin now, so this works
        room.demote('alice', 'bob')
        with self.assertRaises(ChatError):
            room.invite('bob', 'dave')  # ...but not anymore

    def test_nobody_can_kick_the_creator(self):
        room = Room('kitchen', 'alice')
        room.invite('alice', 'bob')
        room.promote('alice', 'bob')
        with self.assertRaises(ChatError):
            room.kick('bob', 'alice')

    def test_only_the_creator_can_kick_an_admin(self):
        room = Room('kitchen', 'alice')
        room.invite('alice', 'bob')
        room.invite('alice', 'carol')
        room.promote('alice', 'bob')
        room.promote('alice', 'carol')
        with self.assertRaises(ChatError):
            room.kick('carol', 'bob')  # Admins can't kick other admins
        room.kick('alice', 'bob')
        self.assertNotIn('bob', room.admins)  # Kicking takes away admin status too

    def test_creator_must_ask_twice_within_a_minute_to_close(self):
        room = Room('kitchen', 'alice')
        with self.assertRaises(ChatError):
            room.request_close('bob', now=0)                    # Only the creator can close
        self.assertFalse(room.request_close('alice', now=0))    # "Are you sure?"
        self.assertFalse(room.request_close('alice', now=100))  # Too late, so: "Are you sure?"
        self.assertTrue(room.request_close('alice', now=130))   # "Yes!"

    def test_drop_forgets_everything_about_a_user(self):
        # So a new user who later takes the name 'bob' gets none of these powers
        room = Room('kitchen', 'alice')
        room.invite('alice', 'bob')
        room.promote('alice', 'bob')
        room.join('bob')
        room.drop('bob')
        self.assertNotIn('bob', room.present)
        self.assertNotIn('bob', room.allowed)
        self.assertNotIn('bob', room.admins)


logging.disable()  # Hide the server's logs. Comment this out to see what the server is doing.


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

    def setUp(self):
        """Runs before each test: start a fresh server, and connect alice and bob."""
        self.chat = ChatServer()
        self.listener = socket.create_server(('127.0.0.1', 0))  # Port 0 means "pick any free port"
        self.addCleanup(self.listener.close)
        self.alice = self.connect('alice')
        self.bob = self.connect('bob')
        self.assertEqual(self.alice.receive(), '#general *DadServer*: bob has joined')

    def connect(self, username):
        client = ChatClient(self.listener.getsockname()[1], username)
        self.addCleanup(client.sock.close)
        # The new connection waits in the listener until it is accepted. Then we
        # do what main() does: hand it to its own thread to handle.
        sock, peer = self.listener.accept()
        threading.Thread(target=self.chat.handle_client, args=(sock, peer), daemon=True).start()
        self.assertEqual(client.receive(), f'PRIVATE *DadServer*: Welcome {username}!')
        self.assertEqual(client.receive(), f'#general *DadServer*: {username} has joined')
        return client

    def test_commands(self):
        self.alice.send('/help')
        self.assertIn('/closeroom', self.alice.receive())  # The last command, so we know help fit

        conversation = [
            # alice sends            the server replies
            ('/version',             f'PRIVATE *DadServer*: /version {VERSION}'),
            ('/echo hi   there',     'PRIVATE *DadServer*: /echo hi there'),
            ('/users',               'PRIVATE *DadServer*: /users alice bob'),
            ('/newroom kitchen',     'PRIVATE *DadServer*: /newroom #kitchen created and joined'),
            ('/pubroom park',        'PRIVATE *DadServer*: /pubroom #park created and joined'),
            ('/users #kitchen',      'PRIVATE *DadServer*: /users alice'),
            ('/join #park',          'PRIVATE *DadServer*: /join You are now a member of #park'),
            ('/rooms',               'PRIVATE *DadServer*: /rooms \n- #general (*DadServer*)\n- #kitchen (alice)\n- #park (alice)'),
            ('/invite kitchen bob',  'PRIVATE *DadServer*: /invite bob has been invited to join #kitchen'),
            ('/promote kitchen bob', 'PRIVATE *DadServer*: /promote bob has been promoted to admin of #kitchen'),
            ('/rooms',               'PRIVATE *DadServer*: /rooms \n- #general (*DadServer*)\n- #kitchen (alice) bob\n- #park (alice)'),
            ('/demote kitchen bob',  'PRIVATE *DadServer*: /demote bob has been demoted to a normal user of #kitchen'),
            ('/kick kitchen bob',    'PRIVATE *DadServer*: /kick bob has been kicked from #kitchen'),
            ('/kick kitchen alice',  'ERROR *DadServer*: /kick Not allowed to kick the room creator alice'),
            ('/closeroom park',      'PRIVATE *DadServer*: /closeroom Are you sure? Send `/closeroom #park` again within 60 seconds to close it'),
            ('/closeroom park',      'PRIVATE *DadServer*: /closeroom #park closed'),
            ('/nope',                'ERROR *DadServer*: /nope Unknown command'),
            ('/quit',                'PRIVATE *DadServer*: /quit Goodbye alice'),
        ]
        for request, reply in conversation:
            self.alice.send(request)
            self.assertEqual(self.alice.receive(), reply)

        self.assertIsNone(self.alice.receive())  # After /quit, the server hangs up

    def test_rooms_reply_does_not_look_like_a_room_message(self):
        # Regression test: /rooms used to put each room on its own line, like
        # "#general *DadServer*", which looked just like a message sent to #general
        self.alice.send('/rooms')
        self.assertNotIn('\n#', self.alice.receive())

    def test_room_message(self):
        self.alice.send('Hi all!')  # No target means #general
        self.assertEqual(self.alice.receive(), '#general alice: Hi all!')
        self.assertEqual(self.bob.receive(), '#general alice: Hi all!')

    def test_direct_message(self):
        self.alice.send(':bob psst')
        self.assertEqual(self.bob.receive(), 'PRIVATE alice: psst')

        self.alice.send(':nobody hello?')
        self.assertEqual(self.alice.receive(), 'ERROR *DadServer*: :nobody No user named nobody')

    def test_private_room(self):
        self.alice.send('/newroom kitchen bob')  # alice invites bob
        self.assertEqual(self.alice.receive(), 'PRIVATE *DadServer*: /newroom #kitchen created and joined. '
                                               'bob has been invited to join #kitchen')
        self.assertEqual(self.bob.receive(), 'PRIVATE *DadServer*: You have been invited to join #kitchen by alice.')

        self.bob.send('/join kitchen')
        self.assertEqual(self.bob.receive(), 'PRIVATE *DadServer*: /join You are now a member of kitchen')

        self.alice.send('#kitchen Dinner is ready')
        self.assertEqual(self.bob.receive(), '#kitchen alice: Dinner is ready')

    def test_kicked_user_is_told_and_locked_out(self):
        self.alice.send('/newroom kitchen bob')
        self.assertEqual(self.alice.receive(), 'PRIVATE *DadServer*: /newroom #kitchen created and joined. '
                                               'bob has been invited to join #kitchen')
        self.assertEqual(self.bob.receive(), 'PRIVATE *DadServer*: You have been invited to join #kitchen by alice.')

        self.alice.send('/kick kitchen bob')
        self.assertEqual(self.bob.receive(), 'PRIVATE *DadServer*: You have been kicked from #kitchen by alice.')

        self.bob.send('#kitchen Let me back in!')
        self.assertEqual(self.bob.receive(), 'ERROR *DadServer*: #kitchen bob not allowed to post to kitchen')

    def test_leaving_is_announced(self):
        self.bob.sock.close()
        self.assertEqual(self.alice.receive(), '#general *DadServer*: bob left.')


if __name__ == '__main__':
    unittest.main()
