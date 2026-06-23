"""
Adapts a pika delivery to the txamqp-style message interface Jasmin's consumers were written against,
so the consumer bodies (which read message.content.body / message.content.properties['...'] /
message.delivery_tag / message.routing_key) move over with minimal churn.

pika delivers a ReceivedMessage namedtuple (channel, method, properties, body). The key addition over the
old txamqp message is `.channel`: acks/rejects MUST go back on the channel that delivered the message
(per-channel delivery-tag scope), which is the discipline that fixes the 'unknown delivery tag' bug.
"""


class _ContentView:
    __slots__ = ('body', 'properties')

    def __init__(self, body, properties):
        self.body = body
        self.properties = properties


class DeliveryMessage:
    __slots__ = ('channel', 'delivery_tag', 'routing_key', 'consumer_tag', 'redelivered', 'content')

    def __init__(self, received):
        # received is pika.adapters.twisted_connection.ReceivedMessage(channel, method, properties, body)
        self.channel = received.channel
        self.delivery_tag = received.method.delivery_tag
        self.routing_key = received.method.routing_key
        self.consumer_tag = received.method.consumer_tag
        self.redelivered = received.method.redelivered

        props = received.properties
        properties = {
            'message-id': props.message_id,
            'reply-to': props.reply_to,
            'priority': props.priority,
            'headers': props.headers if props.headers is not None else {},
        }
        self.content = _ContentView(received.body, properties)
