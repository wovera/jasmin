"""
pika replacement for txamqp.content.Content.

Jasmin's message classes (jasmin/managers/content.py, jasmin/routing/content.py) subclass this and set
`self.body` (bytes, usually a pickled PDU) plus a txamqp-style `self.properties` dict. This base keeps that
exact interface and derives the pika publish pair (body bytes + pika.BasicProperties) the AmqpFactory needs,
so the subclasses move over unchanged apart from their import.
"""
import pika


class Content:
    def __init__(self, body=b'', children=None, properties=None):
        self.body = body
        self.properties = properties if properties is not None else {}

    # txamqp.content.Content was dict-like over its properties; preserve that so existing code/tests
    # that index a content (content['message-id']) keep working.
    def __getitem__(self, key):
        return self.properties[key]

    def __setitem__(self, key, value):
        self.properties[key] = value

    def __contains__(self, key):
        return key in self.properties

    def __delitem__(self, key):
        del self.properties[key]

    @property
    def pika_body(self):
        """The body as bytes (str bodies are utf-8 encoded; pickled PDU bodies are already bytes)."""
        body = self.body
        if isinstance(body, str):
            return body.encode()
        return body

    @property
    def pika_properties(self):
        """Map the txamqp-style properties dict onto pika.BasicProperties.

        Keys mirror what Jasmin's Content subclasses set: 'message-id', 'reply-to', 'priority',
        'delivery-mode', 'content-type', and a nested 'headers' dict."""
        p = self.properties
        priority = p.get('priority')
        return pika.BasicProperties(
            message_id=p.get('message-id'),
            reply_to=p.get('reply-to'),
            priority=int(priority) if priority is not None else None,
            delivery_mode=p.get('delivery-mode'),
            content_type=p.get('content-type'),
            headers=p.get('headers'),
        )
