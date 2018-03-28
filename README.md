# mqtt_sessions

MQTT session and topic routing - embeddable in Erlang projects.


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

