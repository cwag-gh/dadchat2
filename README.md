# DadChat chat server v2

A fun experiment in learning about sockets.


## DadChat protocol v2

### Message framing

One message is encoded as:

    STX|payload bytes|ETX

where:

- STX is the "Start Text" ascii byte (0x02)
- ETX is the "End Text" ascii byte (0x03)
- `payload bytes` is a UTF-8 encoded string
  - which does not include STX or ETX
  - has a maximum length of 1022 bytes

This means that a STX followed by more than 1022 non-ETX characters
are just discarded, and receivers should look for the start of a new
message (STX). Similarly, any bytes after ETX that are not STX are
just discarded.


### Server connection

Server awaits connection on port 12347.

Upon a client connect, the client is expected to send their username
as the first message. Their actual username will be the following:

    first_msg.decode('utf-8', errors='ignore')
             .strip()
             .replace_any('/*#: \t\r\n', '')
             .truncate_to_encoded_bytes(14)

Note that no whitespace is allowed in a username.

Also:
- Usernames must be at least 3 characters long
- username.lower() must not be any of the following:
  `private`, `general`, `dadserver`
- Usernames must include at least one letter

If the first message does not meet these requirements, the server
will send an error message then close the connection.


### Server messages

Payloads from the server to the client are of the form:

    target_name|space|from_username|colon|space|message

where:

- `target_name` is either `PRIVATE`, which means the message was sent
  directly to you from someone, or `#roomname` where `roomname` is
  the name of a room
  - `#general` is the name of the main room
  - `target_name` is limited to 15 encoded bytes (meaning, room names are
    limited to 14 bytes)
- `space` and `colon` are just a single space " " and colon ":"
  character respectively
- `from_username` is the username of the person who sent the message
  - usernames that are bracketed by the `*` character are plugins, like
    `*ScrabChat*`
  - Messages from the server come from `*DadServer*`
  - `from_username` is limited to 14 bytes, which means that
    plugin names are limited to 12 bytes
- Both usernames and plugin names will never include whitespace
- Thus, all header information (everything but the message) will take
  a maximum of 32 bytes. This leaves 1022 - 32 = 990 bytes for a
  message from a client.

Examples:

    #general dadman: Hi all!

is a message to the `#general` room from user `dadman`.

    PRIVATE zeke: Hey dude...

is a private message from user `zeke` to the client.

    PRIVATE *ScrabBot*: Your move.

is a private message from the plugin `*ScrabBot*` to you.

### Client messages

Client messages start with an optional target, and then send the
message content:

    [target|space]|contents

where:

- `target` is either of the form `#roomname` where the client is
  sending a message to a room, `:username` where the client is
  sending a private message to a user, or `/command` where the client
  is requesting a command action from the server
- You can only send messages to rooms you are a member of.
- `contents` is limited to 990 encoded bytes

Examples:

    Hi everyone!

is a message to `#general`.

    :zeke What's up?

is a message directly to user `zeke`.

    #thekitchen What's cooking?

is a message to the room `#thekitchen`. This will only broadcast to
`#thekitchen` if you are a member of `#thekitchen`.

    /help

is a command message, in this case asking to list
all available commands.

### Commands

- `/help` lists all available commands
- `/quit` leaves the server gracefully
- `/join roomname` Joins a room that must exist. Leading `#` character is optional.
- `/newroom roomname [user1] [...] [usern]` Creates a new private room with the given name.
  Leading `#` character is optional. `roomname` must be 14 bytes encoded or fewer.
  Users listed are notified, automatically allowed into the room as non-admins, but
  not auto-joined. You (the room creator) are auto-joined to this room.
- `/pubroom roomname [user1] [...] [usern]` Creates a new public room. Any user can join with `/join`.
  Listed users are sent an invite request message.
- `/join` Joins the conversation for a room. If you already allowed as part of the room,
  you join immediately. If not, a request is sent to all of the admins of a room.
- `/invite roomname [user1] [...] [usern]` Add users to the allowed list of the room.
  Also sends a joining request message to all users.
- `/promote roomname [user1] [...] [usern]` Promote room member(s) to room admins of `roomname`.
  Room admins can invite and kick non-admin users to/from the room.
- `/kick roomname [user1] [...] [usern]` As a room admin, remove non-admin user(s).
  As a room creator, remove any room member.
- `/closeroom roomname` As the room creator, closes the room. Asks for a confirmation, so need to send
  this message twice.
- '/rooms' Lists all available rooms, room creators, and room admins
- '/users [room]' List all users. If optional room argument is provided, lists
  all users in that room. You must be a member of that room to list the
  users in that room.
