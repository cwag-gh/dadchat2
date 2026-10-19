"""Tests for the DadChat server.

Run them all from the project folder with:

    uv run python -m unittest
"""
import unittest

from dadhost2 import ChatError, valid_roomname, valid_username


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


if __name__ == '__main__':
    unittest.main()
