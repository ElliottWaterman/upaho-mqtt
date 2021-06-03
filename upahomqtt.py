# This is a stripped down version of paho-mqtt intended for MicroPython.
# https://github.com/ElliottWaterman/upaho-mqtt

import time # utime as time
from time import sleep  #, sleep_ms
import threading
from sys import platform as sys_platform, implementation as sys_implementation
import struct # ustruct as struct
import errno
import select
import socket

ssl = None
try:
    import ssl
except ImportError:
    print("Failed to import ssl")
    pass

try:
    # Use monotonic clock if available
    time_func = time.monotonic
except AttributeError:
    time_func = time.time

if sys_platform == 'win32': # or sys_platform == 'linux' or sys_platform == 'darwin':
    EAGAIN = errno.WSAEWOULDBLOCK
else:  # elif sys_platform == 'GPy'
    EAGAIN = errno.EAGAIN

if sys_implementation.name == 'micropython':
    DEBUG_PC = False  # MicroPython
else:
    DEBUG_PC = True  # Python

print("DEBUG_PC: " + str(DEBUG_PC))

MQTTv31 = 3
MQTTv311 = 4

PROTOCOL_NAMEv31 = b"MQIsdp"
PROTOCOL_NAMEv311 = b"MQTT"

PROTOCOL_VERSION = 3

# Message types
CONNECT = 0x10
CONNACK = 0x20
PUBLISH = 0x30
PUBACK = 0x40
PUBREC = 0x50
PUBREL = 0x60
PUBCOMP = 0x70
SUBSCRIBE = 0x80
SUBACK = 0x90
UNSUBSCRIBE = 0xA0
UNSUBACK = 0xB0
PINGREQ = 0xC0
PINGRESP = 0xD0
DISCONNECT = 0xE0

# Log levels
MQTT_LOG_INFO = 0x01
MQTT_LOG_NOTICE = 0x02
MQTT_LOG_WARNING = 0x04
MQTT_LOG_ERR = 0x08
MQTT_LOG_DEBUG = 0x10

# CONNACK codes
CONNACK_ACCEPTED = 0
CONNACK_REFUSED_PROTOCOL_VERSION = 1
CONNACK_REFUSED_IDENTIFIER_REJECTED = 2
CONNACK_REFUSED_SERVER_UNAVAILABLE = 3
CONNACK_REFUSED_BAD_USERNAME_PASSWORD = 4
CONNACK_REFUSED_NOT_AUTHORIZED = 5

# Connection state
mqtt_cs_new = 0
mqtt_cs_connected = 1
mqtt_cs_disconnecting = 2
mqtt_cs_connect_async = 3

# Message state
mqtt_ms_invalid = 0
mqtt_ms_publish = 1
mqtt_ms_wait_for_puback = 2
mqtt_ms_wait_for_pubrec = 3
mqtt_ms_resend_pubrel = 4
mqtt_ms_wait_for_pubrel = 5
mqtt_ms_resend_pubcomp = 6
mqtt_ms_wait_for_pubcomp = 7
mqtt_ms_send_pubrec = 8
mqtt_ms_queued = 9

# Error values
MQTT_ERR_AGAIN = -1
MQTT_ERR_SUCCESS = 0
MQTT_ERR_NOMEM = 1
MQTT_ERR_PROTOCOL = 2
MQTT_ERR_INVAL = 3
MQTT_ERR_NO_CONN = 4
MQTT_ERR_CONN_REFUSED = 5
MQTT_ERR_NOT_FOUND = 6
MQTT_ERR_CONN_LOST = 7
MQTT_ERR_TLS = 8
MQTT_ERR_PAYLOAD_SIZE = 9
MQTT_ERR_NOT_SUPPORTED = 10
MQTT_ERR_AUTH = 11
MQTT_ERR_ACL_DENIED = 12
MQTT_ERR_UNKNOWN = 13
MQTT_ERR_ERRNO = 14

sockpair_data = b"0"


class WouldBlockError(Exception):
    pass


def error_string(mqtt_errno):
    """Return the error string associated with an mqtt error number."""
    if mqtt_errno == MQTT_ERR_SUCCESS:
        return "No error."
    elif mqtt_errno == MQTT_ERR_NOMEM:
        return "Out of memory."
    elif mqtt_errno == MQTT_ERR_PROTOCOL:
        return "A network protocol error occurred when communicating with the broker."
    elif mqtt_errno == MQTT_ERR_INVAL:
        return "Invalid function arguments provided."
    elif mqtt_errno == MQTT_ERR_NO_CONN:
        return "The client is not currently connected."
    elif mqtt_errno == MQTT_ERR_CONN_REFUSED:
        return "The connection was refused."
    elif mqtt_errno == MQTT_ERR_NOT_FOUND:
        return "Message not found (internal error)."
    elif mqtt_errno == MQTT_ERR_CONN_LOST:
        return "The connection was lost."
    elif mqtt_errno == MQTT_ERR_TLS:
        return "A TLS error occurred."
    elif mqtt_errno == MQTT_ERR_PAYLOAD_SIZE:
        return "Payload too large."
    elif mqtt_errno == MQTT_ERR_NOT_SUPPORTED:
        return "This feature is not supported."
    elif mqtt_errno == MQTT_ERR_AUTH:
        return "Authorisation failed."
    elif mqtt_errno == MQTT_ERR_ACL_DENIED:
        return "Access denied by ACL."
    elif mqtt_errno == MQTT_ERR_UNKNOWN:
        return "Unknown error."
    elif mqtt_errno == MQTT_ERR_ERRNO:
        return "Error defined by errno."
    else:
        return "Unknown error."


def connack_string(connack_code):
    """Return the string associated with a CONNACK result."""
    if connack_code == CONNACK_ACCEPTED:
        return "Connection Accepted."
    elif connack_code == CONNACK_REFUSED_PROTOCOL_VERSION:
        return "Connection Refused: unacceptable protocol version."
    elif connack_code == CONNACK_REFUSED_IDENTIFIER_REJECTED:
        return "Connection Refused: identifier rejected."
    elif connack_code == CONNACK_REFUSED_SERVER_UNAVAILABLE:
        return "Connection Refused: broker unavailable."
    elif connack_code == CONNACK_REFUSED_BAD_USERNAME_PASSWORD:
        return "Connection Refused: bad user name or password."
    elif connack_code == CONNACK_REFUSED_NOT_AUTHORIZED:
        return "Connection Refused: not authorised."
    else:
        return "Connection Refused: unknown reason."


def topic_matches_sub(sub, topic):
    """Check whether a topic matches a subscription.

    For example:

    foo/bar would match the subscription foo/# or +/bar
    non/matching would not match the subscription non/+/+
    """
    result = True
    multilevel_wildcard = False

    slen = len(sub)
    tlen = len(topic)

    if slen > 0 and tlen > 0:
        if (sub[0] == '$' and topic[0] != '$') or (topic[0] == '$' and sub[0] != '$'):
            return False

    spos = 0
    tpos = 0

    while spos < slen and tpos < tlen:
        if sub[spos] == topic[tpos]:
            if tpos == tlen-1:
                # Check for e.g. foo matching foo/#
                if spos == slen-3 and sub[spos+1] == '/' and sub[spos+2] == '#':
                    result = True
                    multilevel_wildcard = True
                    break

            spos += 1
            tpos += 1

            if tpos == tlen and spos == slen-1 and sub[spos] == '+':
                spos += 1
                result = True
                break
        else:
            if sub[spos] == '+':
                spos += 1
                while tpos < tlen and topic[tpos] != '/':
                    tpos += 1
                if tpos == tlen and spos == slen:
                    result = True
                    break

            elif sub[spos] == '#':
                multilevel_wildcard = True
                if spos+1 != slen:
                    result = False
                    break
                else:
                    result = True
                    break

            else:
                result = False
                break

    if not multilevel_wildcard and (tpos < tlen or spos < slen):
        result = False

    return result

class MQTTMessage(object):
    """ This is a class that describes an incoming message. It is passed to the
    on_message callback as the message parameter.
    """
    def __init__(self, mid=0, topic=b""):
        self.timestamp = 0
        self.state = mqtt_ms_invalid
        self.dup = False
        self.mid = mid
        self._topic = topic
        self.payload = b""
        self.qos = 0
        self.retain = False
    
    def __eq__(self, other):
        """Override the default Equals behavior"""
        if isinstance(other, self.__class__):
            return self.mid == other.mid
        return False
    
    def __ne__(self, other):
        """Define a non-equality test"""
        return not self.__eq__(other)
    
    @property
    def topic(self):
        return self._topic  # Returns bytes  # .decode('utf-8') = str

    @topic.setter
    def topic(self, value):
        self._topic = value


class Client(object):
    """MQTT version 3.1/3.1.1 client class.

    This is the main class for use communicating with an MQTT broker.
    """
    def __init__(self, client_id="", clean_session=True, userdata=None, protocol=MQTTv31):
        if not clean_session and (client_id == "" or client_id is None):
            raise ValueError('A client id must be provided if clean session is False.')
        self._clean_session = clean_session

        self._protocol = protocol
        self._userdata = userdata
        self._sock = None
        self._keepalive = 60
        self._message_retry = 20
        self._last_retry_check = 0
        # [MQTT-3.1.3-4] Client Id must be UTF-8 encoded string.
        if client_id == "" or client_id is None:
            if protocol == MQTTv31:
                pass # self._client_id = base62(uuid.uuid4().int, padding=22)
            else:
                self._client_id = b""
        else:
            self._client_id = client_id
        if isinstance(self._client_id, str):
            self._client_id = self._client_id.encode('utf-8')

        self._username = ""
        self._password = ""
        self._in_packet = {
            "command": 0,
            "have_remaining": 0,
            "remaining_count": [],
            "remaining_mult": 1,
            "remaining_length": 0,
            "packet": b"",
            "to_process": 0,
            "pos": 0}
        self._out_packet = []
        self._current_out_packet = None
        self._last_msg_in = time_func()
        self._last_msg_out = time_func()
        self._reconnect_min_delay = 1
        self._reconnect_max_delay = 120
        self._reconnect_delay = None
        self._ping_t = 0
        self._last_mid = 0
        self._state = mqtt_cs_new
        self._out_messages = []
        self._in_messages = []
        self._max_inflight_messages = 20
        self._inflight_messages = 0
        self._will = False
        self._will_topic = ""
        self._will_payload = None
        self._will_qos = 0
        self._will_retain = False
        self._on_message_filtered = []
        self._host = ""
        self._port = 1883
        self._bind_address = ""

        self._in_callback_mutex = threading.Lock()
        self._out_packet_mutex = threading.Lock()
        self._current_out_packet_mutex = threading.Lock() # RLock
        self._msgtime_mutex = threading.Lock()
        self._out_message_mutex = threading.Lock() # RLock
        self._in_message_mutex = threading.Lock()
        # Other mutexes:
        # self._callback_mutex = threading.RLock() # Changing callback func and checking func exists - not necessary
        # self._reconnect_delay_mutex = threading.Lock() # Changing and using reconnect delay - not necessary
        # self._mid_generate_mutex = threading.Lock() # Creating next mid - good, but necessary?

        self._thread = None
        self._thread_terminate = False
        self._ssl = False
        # No default callbacks
        self.on_log = None
        self.on_connect = None
        self.on_subscribe = None
        self.on_message = None
        self.on_publish = None
        self.on_unsubscribe = None
        self.on_disconnect = None
        self._strict_protocol = False

    def __del__(self):
        pass

    def _sock_recv(self, bufsize):
        # sleep_ms(20)
        try:
            return self._sock.recv(bufsize)
        except socket.error as err:
            self._easy_log(MQTT_LOG_DEBUG, "_sock_recv err: " + str(err))
            if DEBUG_PC:
                if self._ssl and err.errno == ssl.SSL_ERROR_WANT_READ:
                    raise WouldBlockError()
                if self._ssl and err.errno == ssl.SSL_ERROR_WANT_WRITE:
                    # self._call_socket_register_write()
                    raise WouldBlockError()
            if err.errno == EAGAIN:
                raise WouldBlockError()
            raise

    def _sock_send(self, buf):
        try:
            return self._sock.send(buf)
        except socket.error as err:
            self._easy_log(MQTT_LOG_DEBUG, "_sock_send err: " + str(err))
            if DEBUG_PC:
                if self._ssl and err.errno == ssl.SSL_ERROR_WANT_READ:
                    raise WouldBlockError()
                if self._ssl and err.errno == ssl.SSL_ERROR_WANT_WRITE:
                    # self._call_socket_register_write()
                    raise WouldBlockError()
            if err.errno == EAGAIN:
                # self._call_socket_register_write()
                raise WouldBlockError()
            raise

    def _sock_close(self):
        """Close the connection to the server."""
        if not self._sock:
            return

        try:
            sock = self._sock
            self._sock = None
            # self._call_socket_unregister_write(sock)
            # self._call_socket_close(sock)
        finally:
            # In case a callback fails, still close the socket to avoid leaking the file descriptor.
            sock.close()

    def reinitialise(self, client_id="", clean_session=True, userdata=None):
        if self._sock:
            self._sock.close()
            self._sock = None

        self.__init__(client_id, clean_session, userdata)

    def tls_set(self, ca_certs=None, certfile=None, keyfile=None, cert_reqs=None, tls_version=None, ciphers=None):
        if ssl is None:
            raise ValueError('This platform has no SSL/TLS.')

        # TODO: Add ca_certs support if required, used in ssl.wrap_socket()
        self._ssl = True

    def tls_unset(self):
        if ssl is None:
            raise ValueError('This platform has no SSL/TLS.')

        self._ssl = False

    def connect(self, host, port=1883, keepalive=60, bind_address=""):
        """Connect to a remote broker.
        """
        # print("connect")
        self.connect_async(host, port, keepalive, bind_address)
        return self.reconnect()

    def connect_async(self, host, port=1883, keepalive=60, bind_address=""):
        """Connect to a remote broker asynchronously. This is a non-blocking
        connect call that can be used with loop_start() to provide very quick
        start.
        """
        # print("connect_async")
        if host is None or len(host) == 0:
            raise ValueError('Invalid host.')
        if port <= 0:
            raise ValueError('Invalid port number.')
        if keepalive < 0:
            raise ValueError('Keepalive must be >=0.')

        self._host = host
        self._port = port
        self._keepalive = keepalive
        self._bind_address = bind_address

        self._state = mqtt_cs_connect_async

    def reconnect(self):
        """Reconnect the client after a disconnect. Can only be called after
        connect()/connect_async()."""
        # print("reconnect")
        if len(self._host) == 0:
            raise ValueError('Invalid host.')
        if self._port <= 0:
            raise ValueError('Invalid port number.')

        self._in_packet = {
            "command": 0,
            "have_remaining": 0,
            "remaining_count": [],
            "remaining_mult": 1,
            "remaining_length": 0,
            "packet": b"",
            "to_process": 0,
            "pos": 0}

        with self._out_packet_mutex:
            self._out_packet = []

        with self._current_out_packet_mutex:
            self._current_out_packet = None

        with self._msgtime_mutex:
            self._last_msg_in = time_func()
            self._last_msg_out = time_func()

        self._ping_t = 0
        self._state = mqtt_cs_new

        if self._sock:
            self._sock.close()
            self._sock = None
            # print("self._sock = None")

        # Put messages in progress in a valid state.
        self._messages_reconnect_reset()

        self._easy_log(MQTT_LOG_DEBUG, ">> sock.create_connection")
        sock = self._create_socket_connection()

        if self._ssl:
            # TODO: Add ca_certs support if required
            print(">> wrapping socket")
            try:
                sock = ssl.wrap_socket(sock, server_hostname=self._host) # , do_handshake_on_connect=False
            except ssl.SSLError as e:
                print(e)
            except ValueError as e:
                print(e)
                sock = ssl.wrap_socket(sock)
                # sock = ssl.wrap_socket(sock, do_handshake_on_connect=False)
            except TypeError as e:
                # Difference in Micropython (socket has internal SSLContext for "server_hostname") and Python (SSLContext)
                print(e)
                sock = ssl.wrap_socket(sock)

            # sock = ssl.wrap_socket(sock)
            sock.settimeout(self._keepalive)
            print(">> socket handshake")
            sock.do_handshake()

        self._sock = sock
        self._sock.setblocking(False)
        if not DEBUG_PC:
            self._poll = select.poll()
            # self.fileno = self._sock.fileno()
            self._poll.register(self._sock, select.POLLIN | select.POLLOUT | select.POLLERR | select.POLLHUP)  #self.fileno)

        return self._send_connect(self._keepalive)

    def loop_forever(self, timeout=1000, max_packets=1, retry_first_connection=False):
        run = True

        while run:
            if self._thread_terminate is True:
                break

            # if self._state == mqtt_cs_connect_async:
            if not (self._state == mqtt_cs_new or self._state == mqtt_cs_connected):
                try:
                    print(">> loop forever - reconnect top")
                    self.reconnect()
                except (socket.error, OSError):
                    self._easy_log(MQTT_LOG_DEBUG, "Connection failed, retrying")
                    self._reconnect_wait()
            else:
                break

        while run:
            rc = MQTT_ERR_SUCCESS
            while rc == MQTT_ERR_SUCCESS:
                rc = self.loop(timeout, max_packets)

                if (self._thread_terminate is True
                    and self._current_out_packet is None
                    and len(self._out_packet) == 0
                        and len(self._out_messages) == 0):
                    rc = 1
                    run = False

            def should_exit():
                return self._state == mqtt_cs_disconnecting or run is False or self._thread_terminate is True

            if should_exit():
                run = False
            else:
                self._reconnect_wait()

                if should_exit():
                    run = False
                else:
                    try:
                        print(">> loop forever - reconnect bottom")
                        self.reconnect()
                    except (socket.error, OSError):
                        self._easy_log(MQTT_LOG_DEBUG, "Connection failed, retrying")

        print("<< Breaking loop forever")
        return rc

    def loop_start(self):
        if self._thread is not None:
            return MQTT_ERR_INVAL
        
        self._thread_terminate = False
        self._thread = threading.Thread(target=self._thread_main)
        threading.stack_size(12288 if not DEBUG_PC else 32768)  # Increase stack size to allow for more stack frames (deeper nested functions)
        self._thread.start()
    
    def loop_stop(self):
        if self._thread is None:
            return MQTT_ERR_INVAL
        
        self._thread_terminate = True
        # if threading.current_thread() != self._thread:
        #     self._thread.join()
        #     self._thread = None

    def loop(self, timeout=1000, max_packets=1):
        """Process network events.
        """
        # print("loop")
        if timeout < 0:
            raise ValueError('Invalid timeout.')

        with self._current_out_packet_mutex:
            # print("self._current_out_packet =", self._current_out_packet)
            with self._out_packet_mutex:
                # print("self._out_packet =", self._out_packet)
                if self._current_out_packet is None and len(self._out_packet) > 0:
                    self._current_out_packet = self._out_packet.pop(0)
            # print("self._current_out_packet =", self._current_out_packet)
                if self._current_out_packet:
                    if DEBUG_PC:
                        wlist = [self._sock]
                    elif self._sock:
                        self._poll.modify(self._sock, select.POLLIN | select.POLLOUT | select.POLLERR | select.POLLHUP)
                else:
                    if DEBUG_PC:
                        wlist = []
                    elif self._sock:
                        self._poll.modify(self._sock, select.POLLIN | select.POLLERR | select.POLLHUP)

        if DEBUG_PC:
            rlist = [self._sock]
            xlist = [self._sock]
            try:
                socklist = select.select(rlist, wlist, xlist, (timeout / 1000))  # timeout in seconds not ms
            except TypeError:
                # Socket isn't correct type, in likelihood connection is lost
                return MQTT_ERR_CONN_LOST
            except ValueError:
                # Can occur if we just reconnected but rlist/wlist contain a -1 for some reason.
                return MQTT_ERR_CONN_LOST
            except Exception:
                # Note that KeyboardInterrupt, etc. can still terminate since they are not derived from Exception
                return MQTT_ERR_UNKNOWN

            if self._sock in socklist[2]:
                print("select/socket xlist error found!")

            if self._sock in socklist[0]:
                rc = self.loop_read(max_packets)
                if rc or (self._sock is None):
                    return rc
            
            if self._sock in socklist[1]:
                rc = self.loop_write(max_packets)
                if rc or (self._sock is None):
                    return rc

        else:
            events = self._poll.poll(timeout)
            # print("events =", events)
            for poll_obj, ev in events:
                # print("event =", "pHUP" if ev & select.POLLHUP else "", "pERR" if ev & select.POLLERR else "", "pIN" if ev & select.POLLIN else "", "pOUT" if ev & select.POLLOUT else "")
                if ev & select.POLLHUP or ev & select.POLLERR:
                    self._easy_log("poll/socket event error found! " + str(ev))

                if ev & select.POLLIN:
                    rc = self.loop_read(max_packets)
                    if rc or (self._sock is None):
                        return rc

                if ev & select.POLLOUT:
                    rc = self.loop_write(max_packets)
                    if rc or (self._sock is None):
                        return rc

        return self.loop_misc()

    def _publish_is_free(self):
        # if self._current_out_packet_mutex.acquire(False) and self._out_packet_mutex.acquire(False):
        return (self._current_out_packet is None and len(self._out_packet) == 0 and len(self._out_messages) == 0)
        # return True

    def publish(self, topic, payload=None, qos=0, retain=False):
        """Publish a message on a topic.

        This causes a message to be sent to the broker and subsequently from
        the broker to any clients subscribing to matching topics.

        topic: The topic that the message should be published on.
        payload: The actual message to send. If not given, or set to None a
        zero length message will be used. Passing an int or float will result
        in the payload being converted to a string representing that number. If
        you wish to send a true int/float, use struct.pack() to create the
        payload you require.
        qos: The quality of service level to use.
        retain: If set to true, the message will be set as the "last known
        good"/retained message for the topic.

        Returns a tuple (result, mid), where result is MQTT_ERR_SUCCESS to
        indicate success or MQTT_ERR_NO_CONN if the client is not currently
        connected.  mid is the message ID for the publish request. The mid
        value can be used to track the publish request by checking against the
        mid argument in the on_publish() callback if it is defined.

        A ValueError will be raised if topic is None, has zero length or is
        invalid (contains a wildcard), if qos is not one of 0, 1 or 2, or if
        the length of the payload is greater than 268435455 bytes."""
        if topic is None or len(topic) == 0:
            raise ValueError('Invalid topic.')

        topic = topic.encode('utf-8')

        if self._topic_wildcard_len_check(topic) != MQTT_ERR_SUCCESS:
            raise ValueError('Publish topic cannot contain wildcards.')

        if qos < 0 or qos > 2:
            raise ValueError('Invalid QoS level.')

        if isinstance(payload, str):
            local_payload = payload.encode('utf-8')
        elif isinstance(payload, (bytes, bytearray)):
            local_payload = payload
        elif isinstance(payload, (int, float)):
            local_payload = str(payload).encode('ascii')
        elif payload is None:
            local_payload = b''
        else:
            raise TypeError('payload must be a string, bytes, bytearray, int, float or None.')

        if len(local_payload) > 268435455:
            raise ValueError('Payload too large.')

        local_mid = self._mid_generate()

        if qos == 0:
            rc = self._send_publish(local_mid, topic, local_payload, qos, retain, False)
            return (rc, local_mid)
        else:
            message = MQTTMessage(local_mid, topic)
            message.timestamp = time_func()
            message.payload = local_payload
            message.qos = qos
            message.retain = retain
            message.dup = False

            with self._out_message_mutex:  # .acquire()
                self._out_messages.append(message)
                if self._max_inflight_messages == 0 or self._inflight_messages < self._max_inflight_messages:
                    self._inflight_messages += 1
                    if qos == 1:
                        message.state = mqtt_ms_wait_for_puback
                    elif qos == 2:
                        message.state = mqtt_ms_wait_for_pubrec
                    # .release()

                    rc = self._send_publish(message.mid, topic, message.payload, message.qos, message.retain, message.dup)

                    # remove from inflight messages so it will be sent after a connection is made
                    if rc is MQTT_ERR_NO_CONN:
                        self._inflight_messages -= 1
                        message.state = mqtt_ms_publish

                    return (rc, local_mid)
                else:
                    message.state = mqtt_ms_queued
                    # .release()
                    return (MQTT_ERR_SUCCESS, local_mid)

    def username_pw_set(self, username, password=None):
        """Set a username and optionally a password for broker authentication.

        Must be called before connect() to have any effect.
        Requires a broker that supports MQTT v3.1.

        username: The username to authenticate with. Need have no relationship to the client id. Must be unicode [MQTT-3.1.3-11].
            Set to None to reset client back to not using username/password for broker authentication.
        password: The password to authenticate with. Optional, set to None if not required. If it is unicode, then it 
            will be encoded as UTF-8.
        """

        # [MQTT-3.1.3-11] User name must be UTF-8 encoded string
        self._username = None if username is None else username.encode('utf-8')
        self._password = password
        if isinstance(self._password, str):
            self._password = self._password.encode('utf-8')

    def disconnect(self):
        """Disconnect a connected client from the broker."""
        self._state = mqtt_cs_disconnecting

        if self._sock is None:
            return MQTT_ERR_NO_CONN

        return self._send_disconnect()

    def subscribe(self, topic, qos=0):
        """Subscribe the client to one or more topics."""
        # print("subscribe")
        topic_qos_list = None
        if isinstance(topic, str):
            if qos<0 or qos>2:
                raise ValueError('Invalid QoS level.')
            if topic is None or len(topic) == 0:
                raise ValueError('Invalid topic.')
            topic_qos_list = [(topic.encode('utf-8'), qos)]
        elif isinstance(topic, tuple):
            if topic[1]<0 or topic[1]>2:
                raise ValueError('Invalid QoS level.')
            if topic[0] is None or len(topic[0]) == 0 or not isinstance(topic[0], str):
                raise ValueError('Invalid topic.')
            topic_qos_list = [(topic[0].encode('utf-8'), topic[1])]
        elif isinstance(topic, list):
            topic_qos_list = []
            for t in topic:
                if t[1]<0 or t[1]>2:
                    raise ValueError('Invalid QoS level.')
                if t[0] is None or len(t[0]) == 0 or not isinstance(t[0], str):
                    raise ValueError('Invalid topic.')
                topic_qos_list.append((t[0].encode('utf-8'), t[1]))

        if topic_qos_list is None:
            raise ValueError("No topic specified, or incorrect topic type.")

        if self._sock is None: 
            return (MQTT_ERR_NO_CONN, None)

        return self._send_subscribe(False, topic_qos_list)

    def unsubscribe(self, topic):
        """Unsubscribe the client from one or more topics."""
        topic_list = None
        if topic is None:
            raise ValueError('Invalid topic.')
        if isinstance(topic, str):
            if len(topic) == 0:
                raise ValueError('Invalid topic.')
            topic_list = [topic.encode('utf-8')]
        elif isinstance(topic, list):
            topic_list = []
            for t in topic:
                if len(t) == 0 or not isinstance(t, str):
                    raise ValueError('Invalid topic.')
                topic_list.append(t.encode('utf-8'))

        if topic_list is None:
            raise ValueError("No topic specified, or incorrect topic type.")

        if self._sock is None:
            return (MQTT_ERR_NO_CONN, None)

        return self._send_unsubscribe(False, topic_list)

    def loop_read(self, max_packets=1):
        """Process read network events. """
        # print("loop_read")
        if self._sock is None:
            return MQTT_ERR_NO_CONN

        max_packets = len(self._out_messages) + len(self._in_messages)
        print("max_packets =", max_packets)
        max_packets = 1
        if max_packets < 1:
            max_packets = 1

        for i in range(0, max_packets):
            rc = self._packet_read()
            if rc > 0:
                return self._loop_rc_handle(rc)
            elif rc == MQTT_ERR_AGAIN:
                return MQTT_ERR_SUCCESS
        return MQTT_ERR_SUCCESS

    def loop_write(self, max_packets=1):
        """Process write network events.""" 
        # print("loop_write")
        if self._sock is None:
            return MQTT_ERR_NO_CONN

        max_packets = len(self._out_packet) + 1
        if max_packets < 1:
            max_packets = 1

        for i in range(0, max_packets):
            rc = self._packet_write()
            if rc > 0:
                return self._loop_rc_handle(rc)
            elif rc == MQTT_ERR_AGAIN:
                return MQTT_ERR_SUCCESS
        return MQTT_ERR_SUCCESS

    def loop_misc(self):
        """Process miscellaneous network events.""" 
        # print("loop_misc")
        if self._sock is None:
            return MQTT_ERR_NO_CONN

        now = time_func()
        self._check_keepalive()
        if self._last_retry_check + 1 < now:
            # Only check once a second at most
            self._message_retry_check()
            self._last_retry_check = now

        if self._ping_t > 0 and now - self._ping_t >= self._keepalive:
            # client->ping_t != 0 means we are waiting for a pingresp.
            # This hasn't happened in the keepalive time so we should disconnect.
            self._sock_close()

            if self._state == mqtt_cs_disconnecting:
                rc = MQTT_ERR_SUCCESS
            else:
                # rc = 1 # offical library
                rc = MQTT_ERR_CONN_LOST

            if self.on_disconnect:
                with self._in_callback_mutex:
                    self.user_data_set("dc in loop_misc")
                    self.on_disconnect(self, self._userdata, rc)
            return MQTT_ERR_CONN_LOST

        return MQTT_ERR_SUCCESS

    def max_inflight_messages_set(self, inflight):
        """Set the maximum number of messages with QoS>0 that can be part way
        through their network flow at once. Defaults to 20."""
        if inflight < 0:
            raise ValueError('Invalid inflight.')
        self._max_inflight_messages = inflight

    def message_retry_set(self, retry):
        """Set the timeout in seconds before a message with QoS>0 is retried.
        20 seconds by default."""
        if retry < 0:
            raise ValueError('Invalid retry.')

        self._message_retry = retry

    def user_data_set(self, userdata):
        """Set the user data variable passed to callbacks. May be any data type."""
        self._userdata = userdata

    def will_set(self, topic, payload=None, qos=0, retain=False):
        """Set a Will to be sent by the broker in case the client disconnects unexpectedly.

        This must be called before connect() to have any effect.

        topic: The topic that the will message should be published on.
        payload: The message to send as a will. If not given, or set to None a
        zero length message will be used as the will. Passing an int or float
        will result in the payload being converted to a string representing
        that number. If you wish to send a true int/float, use struct.pack() to
        create the payload you require.
        qos: The quality of service level to use for the will.
        retain: If set to true, the will message will be set as the "last known
        good"/retained message for the topic.

        Raises a ValueError if qos is not 0, 1 or 2, or if topic is None or has
        zero string length.
        """
        if topic is None or len(topic) == 0:
            raise ValueError('Invalid topic.')
        if qos<0 or qos>2:
            raise ValueError('Invalid QoS level.')
        if isinstance(payload, bytes):
            self._will_payload = payload
        elif isinstance(payload, str):
            self._will_payload = payload.encode('utf-8')
        elif isinstance(payload, bytearray):
            self._will_payload = payload
        elif isinstance(payload, int) or isinstance(payload, float):
            self._will_payload = str(payload)
        elif payload is None:
            self._will_payload = None
        else:
            raise TypeError('payload must be a utf-8 bytes, string, bytearray, int, float or None.')

        self._will = True
        self._will_topic = topic.encode('utf-8')
        self._will_qos = qos
        self._will_retain = retain

    def will_clear(self):
        """ Removes a will that was previously configured with will_set().

        Must be called before connect() to have any effect."""
        self._will = False
        self._will_topic = ""
        self._will_payload = None
        self._will_qos = 0
        self._will_retain = False

    def socket(self):
        """Return the socket or ssl object for this client."""
        return self._sock

    def message_callback_add(self, sub, callback):
        """Register a message callback for a specific topic.
        Messages that match 'sub' will be passed to 'callback'. Any
        non-matching messages will be passed to the default on_message
        callback.
        
        Call multiple times with different 'sub' to define multiple topic
        specific callbacks.
        
        Topic specific callbacks may be removed with
        message_callback_remove()."""
        if callback is None or sub is None:
            raise ValueError("sub and callback must both be defined.")

        for i in range(0, len(self._on_message_filtered)):
            if self._on_message_filtered[i][0] == sub:
                self._on_message_filtered[i] = (sub, callback)
                return

        self._on_message_filtered.append((sub, callback))

    def message_callback_remove(self, sub):
        """Remove a message callback previously registered with
        message_callback_add()."""
        if sub is None:
            raise ValueError("sub must defined.")

        for i in range(0, len(self._on_message_filtered)):
            if self._on_message_filtered[i][0] == sub:
                self._on_message_filtered.pop(i)
                return

    # ============================================================
    # Private functions
    # ============================================================

    def _loop_rc_handle(self, rc):
        if rc:
            self._easy_log(MQTT_LOG_WARNING, "Disconnecting because of Reason Code {}".format(rc))

            self._sock_close()

            if self._state == mqtt_cs_disconnecting:
                rc = MQTT_ERR_SUCCESS
            if self.on_disconnect:
                with self._in_callback_mutex:
                    self.user_data_set("dc in _loop_rc_handle")
                    self.on_disconnect(self, self._userdata, rc)

        return rc

    def _packet_read(self):
        # This gets called if pselect() indicates that there is network data
        # available - ie. at least one byte.  What we do depends on what data we
        # already have.
        # If we've not got a command, attempt to read one and save it. This should
        # always work because it's only a single byte.
        # Then try to read the remaining length. This may fail because it is may
        # be more than one byte - will need to save data pending next read if it
        # does fail.
        # Then try to read the remaining payload, where 'payload' here means the
        # combined variable header and actual payload. This is the most likely to
        # fail due to longer length, so save current data and current position.
        # After all data is read, send to _mqtt_handle_packet() to deal with.
        # Finally, free the memory and reset everything to starting conditions.
        # print("_packet_read")
        if self._in_packet['command'] == 0:
            try:
                command = self._sock_recv(1)
            except WouldBlockError:
                return MQTT_ERR_AGAIN
            except socket.error as err:
                self._easy_log(MQTT_LOG_ERR, "failed to receive on socket1: " + str(err))
                return 1
            else:
                if len(command) == 0:
                    print("_packet_read 1 error: cmd len is 0")
                    return 1
                command, = struct.unpack("!B", command)
                self._in_packet['command'] = command

        if self._in_packet['have_remaining'] == 0:
            # Read remaining
            # Algorithm for decoding taken from pseudo code at
            # http://publib.boulder.ibm.com/infocenter/wmbhelp/v6r0m0/topic/com.ibm.etools.mft.doc/ac10870_.htm
            while True:
                try:
                    byte = self._sock_recv(1)
                except WouldBlockError:
                    return MQTT_ERR_AGAIN
                except socket.error as err:
                    self._easy_log(MQTT_LOG_ERR, "failed to receive on socket2: " + str(err))
                    return 1
                else:
                    if len(byte) == 0:
                        print("_packet_read 2 error: byte len is 0")
                        return 1
                    byte, = struct.unpack("!B", byte)
                    self._in_packet['remaining_count'].append(byte)
                    # Max 4 bytes length for remaining length as defined by protocol.
                    # Anything more likely means a broken/malicious client.
                    if len(self._in_packet['remaining_count']) > 4:
                        return MQTT_ERR_PROTOCOL

                    self._in_packet['remaining_length'] += (byte & 127) * self._in_packet['remaining_mult']
                    self._in_packet['remaining_mult'] = self._in_packet['remaining_mult'] * 128

                if (byte & 128) == 0:
                    break

            self._in_packet['have_remaining'] = 1
            self._in_packet['to_process'] = self._in_packet['remaining_length']

        while self._in_packet['to_process'] > 0:
            try:
                data = self._sock_recv(self._in_packet['to_process'])
            except WouldBlockError:
                return MQTT_ERR_AGAIN
            except socket.error as err:
                self._easy_log(MQTT_LOG_ERR, "failed to receive on socket3: " + str(err))
                return 1
            else:
                if len(data) == 0:
                    print("_packet_read 3 error: data len is 0")
                    return 1
                self._in_packet['to_process'] -= len(data)
                self._in_packet['packet'] += data

        # All data for this packet is read.
        self._in_packet['pos'] = 0
        rc = self._packet_handle()

        # Free data and reset values
        self._in_packet = dict(
            command=0,
            have_remaining=0,
            remaining_count=[],
            remaining_mult=1,
            remaining_length=0,
            packet=b"",
            to_process=0,
            pos=0)

        with self._msgtime_mutex:
            self._last_msg_in = time_func()
        return rc

    def _packet_write(self):
        # print("_packet_write")
        self._current_out_packet_mutex.acquire()

        while self._current_out_packet:
            packet = self._current_out_packet

            try:
                write_length = self._sock_send(packet['packet'][packet['pos']:])
            except (AttributeError, ValueError):
                self._current_out_packet_mutex.release()
                return MQTT_ERR_SUCCESS
            except WouldBlockError:
                self._current_out_packet_mutex.release()
                return MQTT_ERR_AGAIN
            except socket.error as err:
                self._current_out_packet_mutex.release()
                self._easy_log(MQTT_LOG_ERR, "failed to receive on socket: " + str(err))
                return 1

            if write_length > 0:
                packet['to_process'] -= write_length
                packet['pos'] += write_length

                if packet['to_process'] == 0:
                    if (packet['command'] & 0xF0) == PUBLISH and packet['qos'] == 0:
                        if self.on_publish:
                            with self._in_callback_mutex:
                                self.on_publish(self, self._userdata, packet['mid'])

                    if (packet['command'] & 0xF0) == DISCONNECT:
                        self._current_out_packet_mutex.release()

                        with self._msgtime_mutex:
                            self._last_msg_out = time_func()

                        if self.on_disconnect:
                            with self._in_callback_mutex:
                                self.user_data_set("dc in _packet_write")
                                self.on_disconnect(self, self._userdata, MQTT_ERR_SUCCESS)

                        if self._sock:
                            self._sock.close()
                            self._sock = None
                        return MQTT_ERR_SUCCESS

                    with self._out_packet_mutex:
                        if len(self._out_packet) > 0:
                            self._current_out_packet = self._out_packet.pop(0)
                        else:
                            self._current_out_packet = None
            else:
                break

        self._current_out_packet_mutex.release()

        with self._msgtime_mutex:
            self._last_msg_out = time_func()

        # print("_p_w _current_out_packet =", self._current_out_packet)
        return MQTT_ERR_SUCCESS

    def _easy_log(self, level, fmt, *args):
        if self.on_log is not None:
            buf = fmt % args
            try:
                self.on_log(self, self._userdata, level, buf)
            except Exception:
                # Can't _easy_log this, as we'll recurse until we break
                pass  # self._logger will pick this up, so we're fine

    def _check_keepalive(self):
        if self._keepalive == 0:
            return MQTT_ERR_SUCCESS

        now = time_func()

        with self._msgtime_mutex:
            last_msg_out = self._last_msg_out
            last_msg_in = self._last_msg_in

        if self._sock is not None and (now - last_msg_out >= self._keepalive or now - last_msg_in >= self._keepalive):
            if self._state == mqtt_cs_connected and self._ping_t == 0:
                self._send_pingreq()
                with self._msgtime_mutex:
                    self._last_msg_out = now
                    self._last_msg_in = now
            else:
                self._sock_close()

                print("ping expired: msg_out %ds, msg_in %ds", (now - last_msg_out - self._keepalive), (now - last_msg_in - self._keepalive))

                if self._state == mqtt_cs_disconnecting:
                    rc = MQTT_ERR_SUCCESS
                else:
                    # rc = 1 # offical library
                    rc = MQTT_ERR_CONN_LOST

                if self.on_disconnect:
                    with self._in_callback_mutex:
                        self.user_data_set("dc in _check_keepalive")
                        self.on_disconnect(self, self._userdata, rc)

    def _mid_generate(self):
        self._last_mid = self._last_mid + 1
        if self._last_mid == 65536:
            self._last_mid = 1
        return self._last_mid

    @staticmethod
    def _topic_wildcard_len_check(topic):
        # Search for + or # in a topic. Return MQTT_ERR_INVAL if found.
         # Also returns MQTT_ERR_INVAL if the topic string is too long.
         # Returns MQTT_ERR_SUCCESS if everything is fine.
        if b'+' in topic or b'#' in topic or len(topic) == 0 or len(topic) > 65535:
            return MQTT_ERR_INVAL
        else:
            return MQTT_ERR_SUCCESS

    def _send_pingreq(self):
        self._easy_log(MQTT_LOG_DEBUG, "Sending PINGREQ")
        rc = self._send_simple_command(PINGREQ)
        if rc == MQTT_ERR_SUCCESS:
            self._ping_t = time_func()
        return rc

    def _send_pingresp(self):
        self._easy_log(MQTT_LOG_DEBUG, "Sending PINGRESP")
        return self._send_simple_command(PINGRESP)

    def _send_puback(self, mid):
        self._easy_log(MQTT_LOG_DEBUG, "Sending PUBACK (Mid: "+str(mid)+")")
        return self._send_command_with_mid(PUBACK, mid, False)

    def _send_pubcomp(self, mid):
        self._easy_log(MQTT_LOG_DEBUG, "Sending PUBCOMP (Mid: "+str(mid)+")")
        return self._send_command_with_mid(PUBCOMP, mid, False)

    def _pack_remaining_length(self, packet, remaining_length):
        remaining_bytes = []
        while True:
            byte = remaining_length % 128
            remaining_length = remaining_length // 128
            # If there are more digits to encode, set the top bit of this digit
            if remaining_length > 0:
                byte = byte | 0x80

            remaining_bytes.append(byte)
            # packet.append(byte)
            packet.extend(struct.pack("!B", byte))
            if remaining_length == 0:
                # FIXME - this doesn't deal with incorrectly large payloads
                return packet

    def _pack_str16(self, packet, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        packet.extend(struct.pack("!H", len(data)))
        packet.extend(data)

    def _send_publish(self, mid, topic, payload=b'', qos=0, retain=False, dup=False):
        # we assume that topic and payload are already properly encoded
        assert not isinstance(topic, str) and not isinstance(payload, str) and payload is not None

        if self._sock is None:
            return MQTT_ERR_NO_CONN

        # utopic = topic.encode('utf-8')
        command = PUBLISH | ((dup & 0x1) << 3) | (qos << 1) | retain
        packet = bytearray()
        packet.append(command)  # packet.extend(struct.pack("!B", command))

        payloadlen = len(payload)
        remaining_length = 2 + len(topic) + payloadlen

        if payloadlen == 0:
            self._easy_log(MQTT_LOG_DEBUG, "Sending PUBLISH (d"+str(dup)+", q"+str(qos)+", r"+str(int(retain))+", m"+str(mid)+", '"+str(topic)+"' (NULL payload)")
        else:
            self._easy_log(MQTT_LOG_DEBUG, "Sending PUBLISH (d"+str(dup)+", q"+str(qos)+", r"+str(int(retain))+", m"+str(mid)+", '"+str(topic)+"', ... ("+str(payloadlen)+" bytes)")

        if qos > 0:
            # For message id
            remaining_length += 2

        self._pack_remaining_length(packet, remaining_length)
        self._pack_str16(packet, topic)

        if qos > 0:
            # For message id
            packet.extend(struct.pack("!H", mid))

        packet.extend(payload)

        return self._packet_queue(PUBLISH, packet, mid, qos)

    def _send_pubrec(self, mid):
        self._easy_log(MQTT_LOG_DEBUG, "Sending PUBREC (Mid: "+str(mid)+")")
        return self._send_command_with_mid(PUBREC, mid, False)

    def _send_pubrel(self, mid):  #, dup=False
        self._easy_log(MQTT_LOG_DEBUG, "Sending PUBREL (Mid: "+str(mid)+")")
        return self._send_command_with_mid(PUBREL | 2, mid, False)  #, dup)

    def _send_command_with_mid(self, command, mid, dup):
        # For PUBACK, PUBCOMP, PUBREC, and PUBREL
        if dup:
            command = command | 0x8

        remaining_length = 2
        packet = struct.pack('!BBH', command, remaining_length, mid)
        return self._packet_queue(command, packet, mid, 1)

    def _send_simple_command(self, command):
        # For DISCONNECT, PINGREQ and PINGRESP
        remaining_length = 0
        packet = struct.pack('!BB', command, remaining_length)
        return self._packet_queue(command, packet, 0, 0)

    def _send_connect(self, keepalive):
        # print("_send_connect")
        if self._protocol == MQTTv31:
            protocol = PROTOCOL_NAMEv31
            proto_ver = 3
        else:
            protocol = PROTOCOL_NAMEv311
            proto_ver = 4

        remaining_length = 2 + len(protocol) + 1 + 1 + 2 + 2 + len(self._client_id)

        connect_flags = 0
        if self._clean_session:
            connect_flags = connect_flags | 0x02

        if self._will:
            remaining_length = remaining_length + 2 + len(self._will_topic) + 2 + (0 if self._will_payload is None else len(self._will_payload))
            connect_flags = connect_flags | 0x04 | ((self._will_qos & 0x03) << 3) | ((self._will_retain & 0x01) << 5)

        if self._username:
            remaining_length = remaining_length + 2 + len(self._username)
            connect_flags = connect_flags | 0x80
            if self._password:
                connect_flags = connect_flags | 0x40
                remaining_length = remaining_length + 2 + len(self._password)

        command = CONNECT
        packet = bytearray()
        packet.extend(struct.pack("!B", command))  # packet.append(command)

        self._pack_remaining_length(packet, remaining_length)
        packet.extend(struct.pack("!H" + str(len(protocol)) + "sBBH", len(protocol), protocol, proto_ver, connect_flags, keepalive))

        self._pack_str16(packet, self._client_id)

        if self._will:
            self._pack_str16(packet, self._will_topic)
            if self._will_payload is None or len(self._will_payload) == 0:
                packet.extend(struct.pack("!H", 0))
            else:
                self._pack_str16(packet, self._will_payload)

        if self._username:
            self._pack_str16(packet, self._username)

            if self._password:
                self._pack_str16(packet, self._password)

        self._keepalive = keepalive
        self._easy_log(MQTT_LOG_DEBUG, "Sending CONNECT (u{}, p{}, wr{}, wq{}, wf{}, c{}, k{}) client_id={}".format(
            (connect_flags & 0x80) >> 7,
            (connect_flags & 0x40) >> 6,
            (connect_flags & 0x20) >> 5,
            (connect_flags & 0x18) >> 3,
            (connect_flags & 0x4) >> 2,
            (connect_flags & 0x2) >> 1,
            keepalive,
            self._client_id
        ))
        return self._packet_queue(command, packet, 0, 0)

    def _send_disconnect(self):
        self._easy_log(MQTT_LOG_DEBUG, "Sending DISCONNECT")
        return self._send_simple_command(DISCONNECT)

    def _send_subscribe(self, dup, topics):
        # print("_send_subscribe")
        remaining_length = 2
        for t in topics:
            remaining_length = remaining_length + 2 + len(t[0]) + 1

        command = SUBSCRIBE | (dup << 3) | 0x2  # (1<<1)
        packet = bytearray()
        packet.extend(struct.pack("!B", command))  # packet.append(command)
        self._pack_remaining_length(packet, remaining_length)
        local_mid = self._mid_generate()
        packet.extend(struct.pack("!H", local_mid))
        for t in topics:
            self._pack_str16(packet, t[0])
            packet.extend(struct.pack("B", t[1]))  # packet.append(q)
        self._easy_log(MQTT_LOG_DEBUG, "Sending SUBSCRIBE (d"+str(dup)+", m"+str(local_mid)+") ["+ ", ".join(map(lambda t: t[0].decode(), topics)) +"]")
        return (self._packet_queue(command, packet, local_mid, 1), local_mid)

    def _send_unsubscribe(self, dup, topics):
        remaining_length = 2
        for t in topics:
            remaining_length = remaining_length + 2+len(t)

        command = UNSUBSCRIBE | (dup << 3) | 0x2  # (1<<1)
        packet = bytearray()
        packet.extend(struct.pack("!B", command))  # packet.append(command)
        self._pack_remaining_length(packet, remaining_length)
        local_mid = self._mid_generate()
        packet.extend(struct.pack("!H", local_mid))
        for t in topics:
            self._pack_str16(packet, t)
        
        self._easy_log(MQTT_LOG_DEBUG, "Sending UNSUBSCRIBE (d"+str(dup)+", m"+str(local_mid)+") ["+ ", ".join(map(lambda t: t[0].decode(), topics)) +"]")
        return (self._packet_queue(command, packet, local_mid, 1), local_mid)

    def _message_retry_check_actual(self, messages, mutex):
        with mutex:
            # print("_message_retry_check_actual {}".format(len(messages))) #needed
            now = time_func()
            for m in messages:
                if m.timestamp + self._message_retry < now:
                    # print("m.state ==", m.state)
                    if m.state == mqtt_ms_wait_for_puback or m.state == mqtt_ms_wait_for_pubrec:
                        m.timestamp = now
                        m.dup = True
                        self._send_publish(m.mid, m.topic, m.payload, m.qos, m.retain, m.dup)
                    elif m.state == mqtt_ms_wait_for_pubrel:
                        m.timestamp = now
                        # m.dup = True
                        self._send_pubrec(m.mid)
                    elif m.state == mqtt_ms_wait_for_pubcomp:
                        m.timestamp = now
                        # m.dup = True
                        self._send_pubrel(m.mid) #, True)

    def _message_retry_check(self):
        # print("_message_retry_check") #needed
        self._message_retry_check_actual(self._out_messages, self._out_message_mutex)
        self._message_retry_check_actual(self._in_messages, self._in_message_mutex)

    def _messages_reconnect_reset_out(self):
        # print("_messages_reconnect_reset_out") #needed
        with self._out_message_mutex:
            self._inflight_messages = 0
            for m in self._out_messages:
                m.timestamp = 0
                if self._max_inflight_messages == 0 or self._inflight_messages < self._max_inflight_messages:
                    if m.qos == 0:
                        m.state = mqtt_ms_publish
                    elif m.qos == 1:
                        # self._inflight_messages = self._inflight_messages + 1
                        if m.state == mqtt_ms_wait_for_puback:
                            m.dup = True
                        m.state = mqtt_ms_publish
                    elif m.qos == 2:
                        # self._inflight_messages = self._inflight_messages + 1
                        if self._clean_session:
                            if m.state != mqtt_ms_publish:
                                m.dup = True
                            m.state = mqtt_ms_publish
                        else:
                            if m.state == mqtt_ms_wait_for_pubcomp:
                                m.state = mqtt_ms_resend_pubrel
                                # m.dup = True
                            else:
                                if m.state == mqtt_ms_wait_for_pubrec:
                                    m.dup = True
                                m.state = mqtt_ms_publish
                else:
                    m.state = mqtt_ms_queued

    def _messages_reconnect_reset_in(self):
        # print("_messages_reconnect_reset_in") #needed
        with self._in_message_mutex:
            if self._clean_session:
                self._in_messages = []
                return
            for m in self._in_messages:
                m.timestamp = 0
                if m.qos != 2:
                    self._in_messages.pop(self._in_messages.index(m))
                else:
                    # Preserve current state
                    pass

    def _messages_reconnect_reset(self):
        # print("_messages_reconnect_reset") #needed
        self._messages_reconnect_reset_out()
        self._messages_reconnect_reset_in()

    def _packet_queue(self, command, packet, mid, qos):
        # print("_packet_queue") #needed
        mpkt = dict(
            command = command,
            mid = mid,
            qos = qos,
            pos = 0,
            to_process = len(packet),
            packet = packet)

        with self._out_packet_mutex:
            self._out_packet.append(mpkt)
            if self._current_out_packet_mutex.acquire(False):
                if self._current_out_packet is None and len(self._out_packet) > 0:
                    self._current_out_packet = self._out_packet.pop(0)
                    # print("_p_q _current_out_packet =", self._current_out_packet)
                self._current_out_packet_mutex.release()
            # print("self._out_packet =", self._out_packet)

        # Write a single byte to sockpairW (connected to sockpairR) to break
        # out of select() if in threaded mode.
        #try:
        #    self._sockpairW.send(sockpair_data)
        #except socket.error as err:
        #    if err != EAGAIN:
        #        raise

        if self._thread is None:
            if self._in_callback_mutex.acquire(False):
                self._in_callback_mutex.release()
                return self.loop_write()

        return MQTT_ERR_SUCCESS

    def _packet_handle(self):
        # print("_packet_handle")
        cmd = self._in_packet['command'] & 0xF0
        if cmd == PINGREQ:
            return self._handle_pingreq()
        elif cmd == PINGRESP:
            return self._handle_pingresp()
        elif cmd == PUBACK:
            return self._handle_pubackcomp("PUBACK")
        elif cmd == PUBCOMP:
            return self._handle_pubackcomp("PUBCOMP")
        elif cmd == PUBLISH:
            return self._handle_publish()
        elif cmd == PUBREC:
            return self._handle_pubrec()
        elif cmd == PUBREL:
            return self._handle_pubrel()
        elif cmd == CONNACK:
            return self._handle_connack()
        elif cmd == SUBACK:
            return self._handle_suback()
        elif cmd == UNSUBACK:
            return self._handle_unsuback()
        else:
            # If we don't recognise the command, return an error straight away.
            self._easy_log(MQTT_LOG_ERR, "Error: Unrecognised command "+str(cmd))
            return MQTT_ERR_PROTOCOL

    def _handle_pingreq(self):
        if self._in_packet['remaining_length'] != 0:
            return MQTT_ERR_PROTOCOL

        self._easy_log(MQTT_LOG_DEBUG, "Received PINGREQ")
        return self._send_pingresp()

    def _handle_pingresp(self):
        if self._in_packet['remaining_length'] != 0:
            return MQTT_ERR_PROTOCOL

        # No longer waiting for a PINGRESP.
        self._ping_t = 0
        self._easy_log(MQTT_LOG_DEBUG, "Received PINGRESP")
        return MQTT_ERR_SUCCESS

    def _handle_connack(self):
        # print("_handle_connack") #needed
        # print(self._in_packet)
        if self._strict_protocol:
            if self._in_packet['remaining_length'] != 2:
                return MQTT_ERR_PROTOCOL

        if len(self._in_packet['packet']) != 2:
            return MQTT_ERR_PROTOCOL

        (flags, result) = struct.unpack("!BB", self._in_packet['packet'])
        if self._protocol == MQTTv311:
            if result == CONNACK_REFUSED_PROTOCOL_VERSION:
                self._easy_log(MQTT_LOG_DEBUG, "Received CONNACK ("+str(flags)+", "+str(result)+"), attempting downgrade to MQTT v3.1.")
                # Downgrade to MQTT v3.1
                self._protocol = MQTTv31
                return self.reconnect()
            elif (result == CONNACK_REFUSED_IDENTIFIER_REJECTED and self._client_id == b''):
                    self._easy_log(MQTT_LOG_DEBUG, "NOT IMPLEMENTED - Received CONNACK ("+str(flags)+", "+str(result)+"), attempting to use non-empty CID")
                    # self._client_id = base62(uuid.uuid4().int, padding=22)
                    # return self.reconnect()
                    return MQTT_ERR_CONN_REFUSED

        if result == 0:
            self._state = mqtt_cs_connected
            self._reconnect_delay = None

        self._easy_log(MQTT_LOG_DEBUG, "Received CONNACK ("+str(flags)+", "+str(result)+")")

        if self.on_connect:
            flags_dict = dict()
            flags_dict['session present'] = flags & 0x01
            with self._in_callback_mutex:
                self.on_connect(self, self._userdata, flags_dict, result)

        if result == 0:
            rc = 0
            print("_out_messages length: " + str(len(self._out_messages)))
            # print("_out_messages: " + str(self._out_messages))
            print("_out_messages: " + str([ "m"+str(m.mid)+" "+str(m.topic)+" : "+str(m.payload) for m in self._out_messages ]))
            with self._out_message_mutex:
                for m in self._out_messages:
                    m.timestamp = time_func()
                    if m.state == mqtt_ms_queued:
                        print("1577 mqtt_ms_queued loop_write()")
                        self.loop_write()  # Process outgoing messages that have just been queued up
                        return MQTT_ERR_SUCCESS

                    if m.qos == 0:
                        with self._in_callback_mutex:  # Don't call loop_write after _send_publish()
                            rc = self._send_publish(m.mid, m.topic, m.payload, m.qos, m.retain, m.dup)
                        if rc != 0:
                            return rc
                    elif m.qos == 1:
                        if m.state == mqtt_ms_publish:
                            self._inflight_messages += 1
                            m.state = mqtt_ms_wait_for_puback
                            with self._in_callback_mutex:  # Don't call loop_write after _send_publish()
                                print("1591 mqtt_ms_wait_for_puback _send_publish()")
                                rc = self._send_publish(m.mid, m.topic, m.payload, m.qos, m.retain, m.dup)
                            if rc != 0:
                                return rc
                    elif m.qos == 2:
                        if m.state == mqtt_ms_publish:
                            self._inflight_messages += 1
                            m.state = mqtt_ms_wait_for_pubrec
                            with self._in_callback_mutex:  # Don't call loop_write after _send_publish()
                                print("1600 mqtt_ms_wait_for_pubrec _send_publish()")
                                rc = self._send_publish(m.mid, m.topic, m.payload, m.qos, m.retain, m.dup)
                            if rc != 0:
                                return rc
                        elif m.state == mqtt_ms_resend_pubrel:
                            self._inflight_messages += 1
                            m.state = mqtt_ms_wait_for_pubcomp
                            with self._in_callback_mutex:  # Don't call loop_write after _send_publish()
                                print("1608 mqtt_ms_wait_for_pubcomp _send_pubrel()")
                                rc = self._send_pubrel(m.mid) #y, m.dup)
                            if rc != 0:
                                return rc
                    print("1612 loop_write()")
                    self.loop_write() # Process outgoing messages that have just been queued up

            return rc
        elif result > 0 and result < 6:
            return MQTT_ERR_CONN_REFUSED
        else:
            return MQTT_ERR_PROTOCOL

    def _handle_suback(self):
        # print("_handle_suback") #needed after _packet_handle
        self._easy_log(MQTT_LOG_DEBUG, "Received SUBACK")
        pack_format = "!H" + str(len(self._in_packet['packet']) - 2) + 's'
        (mid, packet) = struct.unpack(pack_format, self._in_packet['packet'])

        pack_format = "!" + "B" * len(packet)
        granted_qos = struct.unpack(pack_format, packet)

        if self.on_subscribe:
            with self._in_callback_mutex:  # Don't call loop_write after _send_publish()
                self.on_subscribe(self, self._userdata, mid, granted_qos)

        return MQTT_ERR_SUCCESS

    def _handle_publish(self):
        rc = 0
        # print("_handle_publish") #needed after packet_handle
        header = self._in_packet['command']
        message = MQTTMessage()
        message.dup = (header & 0x08) >> 3
        message.qos = (header & 0x06) >> 1
        message.retain = (header & 0x01)

        pack_format = "!H" + str(len(self._in_packet['packet']) - 2) + 's'
        (slen, packet) = struct.unpack(pack_format, self._in_packet['packet'])
        pack_format = '!' + str(slen) + 's' + str(len(packet) - slen) + 's'
        (topic, packet) = struct.unpack(pack_format, packet)

        if len(topic) == 0:
            return MQTT_ERR_PROTOCOL

        try:
            print_topic = topic.decode('utf-8')
        except UnicodeDecodeError:
            print_topic = "TOPIC WITH INVALID UTF-8: " + str(topic)

        message.topic = topic

        if message.qos > 0:
            pack_format = "!H" + str(len(packet) - 2) + 's'
            (message.mid, packet) = struct.unpack(pack_format, packet)

        message.payload = packet

        self._easy_log(MQTT_LOG_DEBUG, "Received PUBLISH (d"+str(message.dup)+", q"+str(message.qos)+", r"+str(message.retain)+", m"+str(message.mid)+", '"+print_topic+"', ... ("+str(len(message.payload))+" bytes)")

        message.timestamp = time_func()
        if message.qos == 0:
            self._handle_on_message(message)
            return MQTT_ERR_SUCCESS
        elif message.qos == 1:
            rc = self._send_puback(message.mid)
            self._handle_on_message(message)
            return rc
        elif message.qos == 2:
            rc = self._send_pubrec(message.mid)
            message.state = mqtt_ms_wait_for_pubrel
            with self._in_message_mutex:  # .acquire()
                self._in_messages.append(message)
            return rc
        else:
            return MQTT_ERR_PROTOCOL

    def _handle_pubrel(self):
        if self._strict_protocol:
            if self._in_packet['remaining_length'] != 2:
                return MQTT_ERR_PROTOCOL

        if len(self._in_packet['packet']) != 2:
            return MQTT_ERR_PROTOCOL

        # mid, = struct.unpack("!H", self._in_packet['packet'])
        mid = struct.unpack("!H", self._in_packet['packet'])
        mid = mid[0]
        
        self._easy_log(MQTT_LOG_DEBUG, "Received PUBREL (Mid: "+str(mid)+")")

        with self._in_message_mutex: # .acquire()
            for i in range(len(self._in_messages)):
                if self._in_messages[i].mid == mid:
                    # Only pass the message on if we have removed it from the queue - this
                    # prevents multiple callbacks for the same message.
                    message = self._in_messages.pop(i)
                    self._handle_on_message(message)  # self._in_messages[i]
                    self._inflight_messages -= 1
                    if self._max_inflight_messages > 0:
                        with self._out_message_mutex:
                            rc = self._update_inflight()
                        if rc != MQTT_ERR_SUCCESS:
                            return rc

        return self._send_pubcomp(mid)
        # return MQTT_ERR_SUCCESS

    def _update_inflight(self):
        # Dont lock message_mutex here
        for m in self._out_messages:
            if self._inflight_messages < self._max_inflight_messages:
                if m.qos > 0 and m.state == mqtt_ms_queued:
                    self._inflight_messages += 1
                    if m.qos == 1:
                        m.state = mqtt_ms_wait_for_puback
                    elif m.qos == 2:
                        m.state = mqtt_ms_wait_for_pubrec
                    rc = self._send_publish(m.mid, m.topic, m.payload, m.qos, m.retain, m.dup)
                    if rc != 0:
                        return rc
            else:
                return MQTT_ERR_SUCCESS
        return MQTT_ERR_SUCCESS

    def _handle_pubrec(self):
        if self._strict_protocol:
            if self._in_packet['remaining_length'] != 2:
                return MQTT_ERR_PROTOCOL

        # mid, = struct.unpack("!H", self._in_packet['packet'][:2])
        mid = struct.unpack("!H", self._in_packet['packet'])
        mid = mid[0]
        
        self._easy_log(MQTT_LOG_DEBUG, "Received PUBREC (Mid: "+str(mid)+")")

        with self._out_message_mutex:
            for m in self._out_messages:
                if m.mid == mid:
                    m.state = mqtt_ms_wait_for_pubcomp
                    m.timestamp = time_func()
                    return self._send_pubrel(mid) #, False)

        return MQTT_ERR_SUCCESS

    def _handle_unsuback(self):
        if self._strict_protocol:
            if self._in_packet['remaining_length'] != 2:
                return MQTT_ERR_PROTOCOL

        # mid, = struct.unpack("!H", self._in_packet['packet'][:2])
        mid = struct.unpack("!H", self._in_packet['packet'])
        mid = mid[0]

        self._easy_log(MQTT_LOG_DEBUG, "Received UNSUBACK (Mid: "+str(mid)+")")

        if self.on_unsubscribe:
            with self._in_callback_mutex:
                self.on_unsubscribe(self, self._userdata, mid)
        return MQTT_ERR_SUCCESS
    
    def _do_on_disconnect(self, rc, properties=None):
        # with self._callback_mutex:
        if self.on_disconnect:
            with self._in_callback_mutex:
                try:
                    self.user_data_set("dc in _do_on_disconnect")
                    if properties:
                        self.on_disconnect(self, self._userdata, rc, properties)
                    else:
                        self.on_disconnect(self, self._userdata, rc)
                except Exception as err:
                    self._easy_log(MQTT_LOG_ERR, 'Caught exception in on_disconnect: %s', err)
                    # if not self.suppress_exceptions:
                    #     raise

    def _do_on_publish(self, mid):
        # with self._callback_mutex:
        if self.on_publish:
            with self._in_callback_mutex:
                try:
                    self.on_publish(self, self._userdata, mid)
                except Exception as err:
                    self._easy_log(MQTT_LOG_ERR, 'Caught exception in on_publish: %s', err)
                    # if not self.suppress_exceptions:
                    #     raise

        msg = self._out_messages.pop(mid)
        # msg.info._set_as_published()
        if msg.qos > 0:
            self._inflight_messages -= 1
            if self._max_inflight_messages > 0:
                rc = self._update_inflight()
                if rc != MQTT_ERR_SUCCESS:
                    return rc
        return MQTT_ERR_SUCCESS

    def _handle_pubackcomp(self, cmd):
        if self._strict_protocol:
            if self._in_packet['remaining_length'] != 2:
                return MQTT_ERR_PROTOCOL

        mid, = struct.unpack("!H", self._in_packet['packet'][:2])
        # mid = struct.unpack("!H", self._in_packet['packet'])
        # mid = mid[0]

        self._easy_log(MQTT_LOG_DEBUG, "Received "+cmd+" (Mid: "+str(mid)+")")

        with self._out_message_mutex:
            for i in range(len(self._out_messages)):
                try:
                    if self._out_messages[i].mid == mid:
                        # Only inform the client the message has been sent once.
                        if self.on_publish:
                            with self._in_callback_mutex:
                                self.on_publish(self, self._userdata, mid)

                        msg = self._out_messages.pop(i)
                        if msg.qos > 0:
                            self._inflight_messages = self._inflight_messages - 1
                            if self._max_inflight_messages > 0:
                                rc = self._update_inflight()
                                if rc != MQTT_ERR_SUCCESS:
                                    return rc
                        return MQTT_ERR_SUCCESS
                except IndexError:
                    # Have removed item so i>count.
                    # Not really an error.
                    pass

        return MQTT_ERR_SUCCESS

        # with self._out_message_mutex:
        #     try:
        #         if mid in self._out_messages:
        #             # Only inform the client the message has been sent once.
        #             if self.on_publish:
        #                 with self._in_callback_mutex:
        #                     self.on_publish(self, self._userdata, mid)

        #             msg = self._out_messages.pop(mid)
        #             # msg.info._set_as_published()
        #             if msg.qos > 0:
        #                 self._inflight_messages -= 1
        #                 if self._max_inflight_messages > 0:
        #                     rc = self._update_inflight()
        #                     if rc != MQTT_ERR_SUCCESS:
        #                         return rc
        #             return MQTT_ERR_SUCCESS
        #     except IndexError:
        #         # Have removed item so i>count.
        #         # Not really an error.
        #         print("_handle_pubackcomp IndexError")
        #         pass

        # return MQTT_ERR_SUCCESS

    def _handle_on_message(self, message):
        matched = False

        try:
            topic = message.topic.decode('utf-8')
        except UnicodeDecodeError:
            topic = None

        if topic is not None:
            for t in self._on_message_filtered:
                if topic_matches_sub(t[0], topic):
                    with self._in_callback_mutex:
                        t[1](self, self._userdata, message)
                    matched = True

        if matched == False and self.on_message:
            with self._in_callback_mutex:
                self.on_message(self, self._userdata, message)
    
    def _thread_main(self):
        self.loop_forever(retry_first_connection=True)

    def _reconnect_wait(self):
        # See reconnect_delay_set for details
        now = time_func()
        # with self._reconnect_delay_mutex:
        if self._reconnect_delay is None:
            self._reconnect_delay = self._reconnect_min_delay
        else:
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                self._reconnect_max_delay,
            )

        target_time = now + self._reconnect_delay

        remaining = target_time - now
        while (self._state != mqtt_cs_disconnecting
                and not self._thread_terminate
                and remaining > 0):

            sleep(min(remaining, 1))
            remaining = target_time - time_func()
    
    def _create_socket_connection(self):
        # addr = (self._host, self._port)
        # source = (self._bind_address, 0)
        # return socket.create_connection(addr, source_address=source, timeout=self._keepalive)

        err = None
        responses = None
        try:
            responses = socket.getaddrinfo(self._host, self._port, 0, socket.SOCK_STREAM)
        except OSError as err:
            self._easy_log(MQTT_LOG_DEBUG, "_create_socket_connection err: %s", err)
        if not responses:
            raise socket.error("getaddrinfo returns an empty list")
        if not isinstance(responses, list):
            raise socket.error("getaddrinfo did not return a list")
        
        for res in responses:
            self._easy_log(MQTT_LOG_DEBUG, ">> Connecting {}".format(res))
            if len(res) >= 5:
                af, socktype, proto, canonname, sa = res
                sock = None
                try:
                    sock = socket.socket(af, socktype, proto)
                    if self._keepalive:
                        sock.settimeout(self._keepalive)
                    if self._bind_address:
                        sock.bind(self._bind_address)

                    sock.connect(sa)
                    # Break explicitly a reference cycle
                    err = None
                    return sock

                except socket.error as _:
                    err = _
                    if sock is not None:
                        sock.close()

        if err is not None:
            try:
                raise err
            finally:
                # Break explicitly a reference cycle
                err = None

    # def create_connection(self, address, timeout=-1, source_address=None):
    #     """Connect to *address* and return the socket object.

    #     Convenience function.  Connect to *address* (a 2-tuple ``(host,
    #     port)``) and return the socket object.  Passing the optional
    #     *timeout* parameter will set the timeout on the socket instance
    #     before attempting to connect.  If no *timeout* is supplied, the
    #     global default timeout setting returned by :func:`getdefaulttimeout`
    #     is used.  If *source_address* is set it must be a tuple of (host, port)
    #     for the socket to bind as a source address before making the connection.
    #     A host of '' or port 0 tells the OS to use the default.
    #     """

    #     host, port = address
    #     err = None
    #     responses = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    #     print(responses)
    #     for res in responses:
    #         print(">> Connecting {}".format(res))
    #         if len(res) >= 5:
    #             af, socktype, proto, canonname, sa = res
    #             sock = None
    #             try:
    #                 sock = socket.socket(af, socktype, proto)
    #                 if timeout is not -1:
    #                     sock.settimeout(timeout)
    #                 if source_address:
    #                     sock.bind(source_address)
    #                 sock.connect(sa)
    #                 # Break explicitly a reference cycle
    #                 err = None
    #                 return sock

    #             except socket.error as _:
    #                 err = _
    #                 if sock is not None:
    #                     sock.close()

    #     if err is not None:
    #         try:
    #             raise err
    #         finally:
    #             # Break explicitly a reference cycle
    #             err = None
    #     else:
    #         raise socket.error("getaddrinfo returns an empty list")
