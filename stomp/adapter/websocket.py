"""Websocket transport for stomp.py.

Obviously not a typical message broker, but convenient if you don't have a broker, but still want to use stomp.py
methods.
"""

from urllib.parse import urlparse
import websocket
import socket
import struct
import logging
import time
import errno

from stomp.connect import BaseConnection
from stomp.protocol import *
from stomp.transport import *
from stomp.utils import *

_LOGGER = logging.getLogger(__name__)

class WebsocketTransport(Transport):
    """
    Transport over websocket connections rather than using a broker.
    """
    def __init__(self, url):
        Transport.__init__(self, [], False, False, 0.0, 0.0, 0.0, 0.0, 0, False, None, None, None, None, False,
                           DEFAULT_SSL_VERSION, None, None, None)
        self.subscriptions = {}
        self.url = url
        self.current_host_and_port = (urlparse(url).hostname, 0)
        self.connected = False

    def is_connected(self):
        """
        Return true if the socket managed by this connection is connected

        :rtype: bool
        """

    def attempt_connection(self):
        """
        Establish a websocket connection
        """
        websocket.enableTrace(True)
#        websocket.setdefaulttimeout(3600)
#        self.ws = websocket.create_connection(self.url, timeout=3600)
#        self.ws = websocket.create_connection(self.url, sockopt=((socket.SOL_TCP, socket.TCP_KEEPALIVE, 30),))
        self.ws = websocket.create_connection(self.url, sockopt=(
             (socket.SOL_TCP, socket.TCP_KEEPIDLE, 10),))

        if not self.ws:
            _LOGGER.error("websocket: unable to connect!")
            raise exception.ConnectFailedException()

        self.connected = True

    def send(self, encoded_frame):
        """
        Send an encoded frame through the webocket.

        :param bytes encoded_frame:
        """
        self.ws.send(encoded_frame)

    def receive(self):
        """
        Receive x bytes from the websocket.

        :rtype: bytes
        """

        while True:
            try:
                _LOGGER.debug("websocket: rx")
                data = self.ws.recv()
                _LOGGER.debug("websocket: rxed")
                if type(data) == str:
                    return str.encode(data)
                return data
            except websocket.WebSocketConnectionClosedException:
                _LOGGER.error("websocket: connection closed!")
                self.connected = False
                raise exception.ConnectionClosedException()
            except socket.error as e:
                time.sleep(5)
                _LOGGER.error("websocket: reconnecting! not really...")
                raise exception.ConnectionClosedException()
#                self.attempt_connection()
#                continoue
 
        try:
            frame = self.ws.recv_frame()
        except websocket.WebSocketException:
            _LOGGER.error("websocket.WebSocketException")
            raise exception.ConnectionClosedException()
#            raise
#            return websocket.ABNF.OPCODE_CLOSE, None
        if not frame:
            _LOGGER.error("websocket.WebSocketException, not a valid frame: %s" % frame)
            raise websocket.WebSocketException("websocket: Not a valid frame: %s" % frame)
        elif frame.opcode in OPCODE_DATA:
            _LOGGER.error("websocket got OPCODE_DATA")
            if type(frame.data) == str:
                return str.encode(frame.data)
            return frame.data
#            return str(frame.data)
        elif frame.opcode in OPCODE_TEXT:
            _LOGGER.error("websocket got OPCODE_TEXT")
            if type(frame.data) == str:
                return str.encode(frame.data)
            return frame.data
#            return str(frame.data, "utf-8")
        elif frame.opcode == websocket.ABNF.OPCODE_CLOSE:
            _LOGGER.error("websocket got OPCODE_CLOSE")
            self.ws.send_close()
            raise exception.ConnectFailedException()
#            return None
        elif frame.opcode == websocket.ABNF.OPCODE_PING:
            _LOGGER.error("websocket got OPCODE_PING")
            self.ws.pong(frame.data)
            return None

        if type(frame.data) == str:
            return str.encode(frame.data)
        return frame.data

    def process_frame(self, f, frame_str):
        """
        :param Frame f: Frame object
        :param bytes frame_str: Raw frame content
        """
        frame_type = f.cmd.lower()

        if frame_type in ['disconnect']:
            return

        if frame_type == 'send':
            frame_type = 'message'
            f.cmd = 'MESSAGE'

        if frame_type in ['connected', 'message', 'receipt', 'error', 'heartbeat']:
            if frame_type == 'message':
                if f.headers['destination'] not in self.subscriptions.values():
                    return
                (f.headers, f.body) = self.notify('before_message', f.headers, f.body)
            self.notify(frame_type, f.headers, f.body)
        if 'receipt' in f.headers:
            receipt_frame = Frame('RECEIPT', {'receipt-id': f.headers['receipt']})
            lines = convert_frame_to_lines(receipt_frame)
            self.send(encode(pack(lines)))
        log.debug("Received frame: %r, headers=%r, body=%r", f.cmd, f.headers, f.body)

    def stop(self):
        log.error("websocket stop!")
        self.running = False
        self.ws.close()
        Transport.stop(self)

#    def notify(self, frame_type, headers=None, body=None):
#        Transport.notify(self, frame_type, headers, body)

class WebsocketConnection(BaseConnection, Protocol12):
    def __init__(self, url, wait_on_receipt=False):
        """
        :param bool wait_on_receipt: deprecated, ignored
        """
        self.transport = WebsocketTransport(url)
        self.transport.set_listener('websocket-listener', self)
        self.transactions = {}
        Protocol12.__init__(self, self.transport, (0, 0))

    def connect(self, username=None, passcode=None, wait=False, timeout=None, headers=None, **keyword_headers):
        """
        :param str username:
        :param str passcode:
        :param bool wait:
        :param dict headers:
        :param keyword_headers:
        """
        Protocol12.connect(self, username, passcode, wait, timeout, headers, **keyword_headers)

    def subscribe(self, destination, id, ack='auto', headers=None, **keyword_headers):
        """
        :param str destination:
        :param str id:
        :param str ack:
        :param dict headers:
        :param keyword_headers:
        """
        self.transport.subscriptions[id] = destination
        Protocol12.subscribe(self, destination, id, ack, headers, **keyword_headers)

    def unsubscribe(self, id, headers=None, **keyword_headers):
        """
        :param str id:
        :param dict headers:
        :param keyword_headers:
        """
        del self.transport.subscriptions[id]

    def disconnect(self, receipt=None, headers=None, **keyword_headers):
        """
        :param str receipt:
        :param dict headers:
        :param keyword_headers:
        """
        log.error("websocket->disconnect")
        Protocol12.disconnect(self, receipt, headers, **keyword_headers)
        self.transport.stop()

    def send_frame(self, cmd, headers=None, body=''):
        """
        :param str cmd:
        :param dict headers:
        :param body:
        """
        if headers is None:
            headers = {}
        frame = utils.Frame(cmd, headers, body)

        if cmd == CMD_BEGIN:
            trans = headers[HDR_TRANSACTION]
            if trans in self.transactions:
                self.notify('error', {}, 'Transaction %s already started' % trans)
            else:
                self.transactions[trans] = []
        elif cmd == CMD_COMMIT:
            trans = headers[HDR_TRANSACTION]
            if trans not in self.transactions:
                self.notify('error', {}, 'Transaction %s not started' % trans)
            else:
                for f in self.transactions[trans]:
                    self.transport.transmit(f)
                del self.transactions[trans]
        elif cmd == CMD_ABORT:
            trans = headers['transaction']
            del self.transactions[trans]
        else:
            if 'transaction' in headers:
                trans = headers['transaction']
                if trans not in self.transactions:
                    self.transport.notify('error', {}, 'Transaction %s not started' % trans)
                    return
                else:
                    self.transactions[trans].append(frame)
            else:
                self.transport.transmit(frame)
