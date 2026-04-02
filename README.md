![Test](https://github.com/zotonic/mqtt_sessions/workflows/Test/badge.svg)

# MQTT v5 server side sessions and routing

MQTT v5 session and topic routing - embeddable in Erlang projects.

This library handles pools of MQTT sessions.
Transports (connections) attach to MQTT sessions to relay packets.

The sessions handles packets, queues, and a user-context state.

Access control and authentication is handled with a runtime module.
This module is configured in the `mqtt_sessions` application env key `runtime`.

The default and example runtime is `src/mqtt_sessions_runtime.erl`.

Note that this library does not handle TCP/IP connections. It handles the
complete MQTT session logic. Other libraries are used for transporting the
MQTT packets to/from external clients.

When a subscription is made to a $SYS topic the subscriber is mapped to
the default pool. This makes it possible to share system information
between different pools.

## How connects work

`mqtt_sessions` keeps the MQTT session state separate from the transport that
carries packets.

An incoming MQTT `CONNECT` does not always create a brand new session. It can:

- create a new session for a new `client_id`
- reattach to an existing session for the same `client_id`
- replace the currently attached transport for that session

This means that the same MQTT session can survive disconnects and later be
reattached by another connection source, as long as the session has not expired
and the protocol flags allow the reconnect.

The attached transport can come from different sources. For example, one
session can be driven by a websocket bridge used by a browser, while another
session can be driven by a native MQTT listener connection. The session process
only manages MQTT state; the actual transport process is external and can vary
per reconnect.

## Disconnect timeout behaviour

Sessions normally keep the negotiated `session_expiry_interval` after a disconnect.

Runtimes can mark a session as:

- websocket-originated
- anonymous
- crawler

When a session is both websocket-originated and anonymous, `mqtt_sessions`
uses a shorter timeout after the first disconnect:

- anonymous websocket session: 30 seconds
- anonymous websocket crawler session: 10 seconds

This is intended to reduce memory use for one-page anonymous visits, such as
headless crawler traffic that creates many short-lived browser sessions without
reusing them.

Authenticated websocket sessions and non-websocket sessions keep their normal
session expiry behaviour.

## Runtime callbacks

The runtime module is responsible for authentication, authorization, and for
describing some session characteristics derived from the opaque `user_context()`.

The runtime callbacks are:

- `vhost_pool/1`
  Map a virtual host to a session pool.
- `pool_default/0`
  Return the default pool when no host-specific pool can be resolved.
- `new_user_context/3`
  Create the initial runtime-specific `user_context()` for a session.
- `connect/4`
  Handle MQTT `CONNECT` and return `CONNACK` plus an updated user context.
- `reauth/2`
  Handle MQTT v5 re-authentication.
- `is_allowed/4`
  Check if a publish or subscribe action is allowed for a topic.
- `is_valid_message/3`
  Validate an incoming message before it is processed.
- `control_message/3`
  Handle runtime-specific control messages and update the user context.
- `is_websocket_origin/1`
  Return `true` when the user context represents a websocket-originated session.
- `is_crawler/1`
  Return `true` when the user context represents a crawler session.
- `is_anonymous_user/1`
  Return `true` when the user context represents an anonymous user.

The last three callbacks are used by the session process to decide whether a
disconnected session should get the shortened timeout described above, without
inspecting the runtime-specific `user_context()` directly.

## Packet size protection

`mqtt_sessions` distinguishes between incoming and outgoing MQTT packet limits.

The incoming limit is controlled with the application env key
`max_incoming_packet_size`.

The outgoing limit is controlled with the application env key
`max_outgoing_packet_size`.

Incoming packets:

- incoming `CONNECT` packets larger than the incoming limit are refused
- incoming packets on established sessions are disconnected with
  `packet_too_large`
- successful MQTT v5 `CONNACK` packets advertise the configured incoming
  `maximum_packet_size` to the client

Outgoing packets:

- are unlimited by default from the server side
- can be capped with `max_outgoing_packet_size`
- always honor the client-advertised MQTT v5 `maximum_packet_size` from
  `CONNECT`, if present

Defaults:

- `max_incoming_packet_size`: 20 MB
- `max_outgoing_packet_size`: unlimited

## Retained memory protection

Retained messages can be bounded with the application env key
`max_retained_memory`, expressed in bytes.

When configured, `mqtt_sessions`:

- removes expired retained messages first
- then evicts the oldest retained messages until the ETS memory used by the
  retained-topic and retained-message tables is back within the configured limit

If `max_retained_memory` is not configured, then retained-message storage is not
explicitly configured, `mqtt_sessions` defaults it to 500 MB.

## Incoming publish rate limiting

`mqtt_sessions` can rate-limit incoming client `PUBLISH` packets per session
using:

- `max_incoming_messages_rate`
- `max_incoming_messages_burst`

`max_incoming_messages_rate` is the sustained number of incoming publish
messages per second. The period for this limit is `1` second.

`max_incoming_messages_burst` is the extra burst capacity on top of the regular
rate. This is not a separate time window; it is additional token-bucket
capacity that can be spent immediately and then refills at the regular per-
second rate.

The limiter uses a token-bucket model per session. This means a client can send
messages at the configured steady rate, while also consuming a limited burst
budget for short spikes.

When the bucket is empty, `mqtt_sessions` does not block inside the session
process. This is important because blocking there would let unrelated Erlang
messages pile up in the mailbox. Instead it fails fast to keep memory use
bounded:

- QoS 0 incoming publishes are dropped
- QoS 1 and QoS 2 incoming publishes are rejected with MQTT reason code
  `Quota Exceeded`

Defaults:

- `max_incoming_messages_rate`: `1000`
- `max_incoming_messages_burst`: `5000`

### TODO

Current status of the list below:

- partially implemented:
  `Connect rate`,
  `Sessions with biggest queues`,
  `Sessions with largest number of packets`
- not implemented:
  `Number of connected sessions`,
  `Number of packets sent / received`,
  `Memory consumption of retained tables`

1. Add instrumentation

 - Number of connected sessions
 - Number of packets sent / received
 - Connect rate
 - Memory consumption of retained tables
 - Sessions with biggest queues
 - Sessions with largest number of packets
