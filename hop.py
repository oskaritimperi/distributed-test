from app.fanoutclient import FanoutClient
from app.terminatorscanner import TerminatorScannerBasic
import pyuv
import random

class Hopper(FanoutClient):
    def __init__(self, address, port):
        super(Hopper, self).__init__(address)
        self.server_listen_port = port

    def on_connected(self):
        self.subscribe('join')
        self.subscribe('leave')
        self.announce('join', 'hopper|%s' % self.server_listen_port)

    def disconnect(self):
        def after_announce():
            super(Hopper, self).disconnect()
        self.announce("leave", 'hopper', callback=after_announce)

loop = pyuv.Loop.default_loop()
ihandler = TerminatorScannerBasic('\n')

clients = []
ports = []

def on_connection(server, error):
    client = pyuv.TCP(server.loop)
    server.accept(client)
    clients.append(client)
    client.start_read(on_read)

def on_read(client, data, error):
    if data is None:
        client.close()
        clients.remove(client)
        return

    ihandler.handle_read(data)

    while ihandler.incoming:
        msg = ihandler.incoming.pop(0)
        cmd, data = msg.split(' ', 1)
        if cmd == 'hello':
            _, port = data.split()
            ports.append(port)
            print 'hello from %s' % port

port = random.randint(10000, 50000)
server = pyuv.TCP(loop)
server.bind(('127.0.0.1', port))
server.listen(on_connection)

def on_timer(timer):
    timer.stop()
    port = random.choice(ports)

    print 'sending hop command to port %s' % port

    def on_connected(handle, error):
        handle.write('hop hop\n', on_written)

    def on_written(handle, error):
        handle.close()
        fanout_client.disconnect()
        server.close()

    handle = pyuv.TCP(timer.loop)
    handle.connect(('127.0.0.1', int(port)), on_connected)

timer = pyuv.Timer(loop)
timer.start(on_timer, 2, 0)

fanout_client = Hopper(('127.0.0.1', 9898), port)
fanout_client.connect()

loop.run()
