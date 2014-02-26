import pyuv
import uuid
import random

class FanoutClient(object):
    def __init__(self, address, loop=None):
        self.address = address
        self.host, self.port = address

        self.ibuf = []

        self.channel_handlers = {}

        if not loop:
            loop = pyuv.Loop.default_loop()

        self._conn = pyuv.TCP(loop)

    def connect(self):
        self._conn.connect(self.address, self._on_connected)

    def disconnect(self):
        self._conn.close(self._on_close)

    def on_connected(self):
        pass

    def on_close(self):
        pass

    def _on_connected(self, handle, error):
        self.on_connected()
        self._conn.start_read(self._on_read)

    def _on_read(self, handle, data, error):
        if not data:
            self.on_close()
            return

        parts = data.split('\n')

        while parts:
            if len(parts) > 1:
                self.ibuf.append(parts.pop(0))
                self.handle_incoming("".join(self.ibuf))
                self.ibuf = []
            elif len(parts) == 1:
                if not parts[0]:
                    break
                else:
                    self.ibuf.append(parts.pop(0))

    def _on_close(self, handle):
        self.on_close()

    def handle_incoming(self, data):
        channel, msg = data.split("!")

        if channel in self.channel_handlers.keys():
            self.channel_handlers[channel](msg)
        # else:
            # print 'no handler for <%s>' % channel

    def _send_command(self, cmd, callback=None):
        # print "sending cmd <%s>" % cmd
        def after_write(handle, error):
            if callback:
                callback()

        self._conn.write("%s\n" % cmd, after_write)

    def subscribe(self, channel, handler=None, **kwargs):
        if handler:
            self.channel_handlers[channel] = handler

        self._send_command("subscribe %s" % channel, **kwargs)

    def unsubscribe(self, channel, **kwargs):
        if channel in self.channel_handlers:
            del self.channel_handlers[channel]

        self._send_command("unsubscribe %s" % channel, **kwargs)

    def announce(self, channel, msg, **kwargs):
        self._send_command("announce %s %s" % (channel, msg), **kwargs)
