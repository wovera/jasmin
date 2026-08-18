# Copyright (c) Jookies LTD <jasmin@jookies.net>
# See LICENSE for details.

"""Jasmin SMS Gateway by Jookies LTD <jasmin@jookies.net>"""
import binascii

import messaging.sms.gsm0338  # noqa: F401  (registers the 'gsm0338' codec encode_short_message asks for)

from jasmin.protocols.http.errors import UrlArgsValidationError, AuthenticationError


def hex2bin(hex_content):
    """Convert hex-content back to binary data, raise a UrlArgsValidationError on failure"""

    try:
        return binascii.unhexlify(hex_content)
    except Exception as e:
        raise UrlArgsValidationError("Invalid hex-content data: '%s'" % hex_content)


def encode_short_message(request):
    """The bytes the splitter and the wire see, from the request's content or hex-content.

    Every endpoint that builds a submit_sm shares this: one that transcoded differently would split, price or count
    a different message than the one /send puts on the wire.
    """

    if b'hex-content' in request.args:
        return hex2bin(request.args[b'hex-content'][0])

    content = request.args[b'content'][0]
    if request.args[b'coding'][0] != b'0':
        return content

    # 7 bit coding is counted and split in septets, so utf8 is transcoded here rather than carried as its bytes.
    short_message = (content.decode() if isinstance(content, bytes) else content).encode('gsm0338', 'replace')
    request.args[b'content'][0] = short_message

    return short_message

def authenticate_user(username, password, routerpb, stats, log):
    if isinstance(username, bytes):
        username = username.decode()
    if isinstance(password, bytes):
        password = password.decode()

    user = routerpb.authenticateUser(
        username=username,
        password=password)
    if user is None:
        stats.inc('auth_error_count')

        log.debug(
            "Authentication failure for username:%s and password:%s",
            username, password)
        log.error(
            "Authentication failure for username:%s",
            username)
        raise AuthenticationError(
            'Authentication failure for username:%s' % username)
    return user