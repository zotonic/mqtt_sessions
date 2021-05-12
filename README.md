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

### TODO

1. Add protections

 - Max packet size
 - Max memory voor retained
 - Max number of pending messages (drop QoS 0)
 - Max pending acks (flow control)
 - Rate limiting

 2. Add instrumentation

 - Number of sessions
 - Number of connected sessions
 - Number of packets sent / received
 - Connect rate
 - Memory consumption of retained tables
 - Sessions with biggest queues
 - Sessions with largest number of packets

