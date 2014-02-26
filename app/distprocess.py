import random
import signal
import uuid

import pyuv

from fanoutclient import FanoutClient
from terminatorscanner import TerminatorScannerBasic


class PeerFinder(FanoutClient):
    def __init__(self, process, address, loop=None):
        super(PeerFinder, self).__init__(address, loop)
        self.process = process

    def join_handler(self, msg):
        peer_id, port = msg.split('|')

        if peer_id != self.process.client_id:
            print "got peer at port %s" % port
            self.process.peers.append((peer_id, port))
            self.process.send_hello(peer_id, port)

    def leave_handler(self, msg):
        print 'peer %s has left' % msg
        self.process.peers = [p for p in self.process.peers if p[0] != msg]

    def control_handler(self, msg):
        if msg == 'quit':
            self.process.disconnect()


    def on_connected(self):
        print "peerfinder connected"
        self.subscribe("join", self.join_handler)
        self.subscribe("leave", self.leave_handler)
        self.subscribe("control", self.control_handler)
        self.announce("join", "%s|%s" % (self.process.client_id, self.process.port))

    def disconnect(self):
        def after_announce():
            print 'LEFT'
            super(PeerFinder, self).disconnect()
        print 'LEAVING'
        self.announce("leave", self.process.client_id, callback=after_announce)
        # super(PeerFinder, self).disconnect()


class DistProcess(object):
    def __init__(self, loop=None):
        self.peers = []

        self.epoch = 0

        self.freq = -1
        self.new_freq = None
        self.freq_epoch = 0

        self.client_id = str(uuid.uuid4())

        if not loop:
            loop = pyuv.Loop.default_loop()

        self.loop = loop

        self.signal_watchers = set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            handle = pyuv.Signal(loop)
            handle.start(self._signal_cb, sig)
            self.signal_watchers.add(handle)

        self.peerfinder = PeerFinder(self, ('127.0.0.1', 9898), loop)

        self.port = random.randint(10000, 50000)

        self.clients = []

        self.incoming = TerminatorScannerBasic('\n')
        self.cmd_handlers = {}
        self.cmd_handlers['hello'] = self.handle_hello
        self.cmd_handlers['update'] = self.handle_update
        self.cmd_handlers['elect_me'] = self.handle_elect_me
        self.cmd_handlers['you_have_my_vote'] = self.handle_you_have_my_vote
        self.cmd_handlers['hop'] = self.handle_hop

        self.votes = []
        self.voting = False
        self.vote_timer = pyuv.Timer(self.loop)

    def start(self):
        self.server = pyuv.TCP(self.loop)
        self.server.bind(('127.0.0.1', self.port))
        self.server.listen(self._on_connection)

        self.update_timer = pyuv.Timer(self.loop)
        self.update_timer.start(self._on_timer, random.random() * 5 + 5,
            random.random() * 5 + 5)

        print "listening on port %s" % self.port

        self.peerfinder.connect()

    def _on_connection(self, server, error):
        # print 'client connected'
        client = pyuv.TCP(server.loop)
        server.accept(client)
        self.clients.append(client)
        client.start_read(self._on_read)

    def _on_read(self, client, data, error):
        if data is None:
            client.close()
            self.clients.remove(client)
            return

        self.incoming.handle_read(data)

        while self.incoming.incoming:
            msg = self.incoming.incoming.pop(0)
            self._handle_msg(msg)

    def _handle_msg(self, msg):
        # print 'received data <%s>' % msg

        cmd, data = msg.split(' ', 1)

        if cmd in self.cmd_handlers:
            handler = self.cmd_handlers[cmd]
            handler(data)

    def handle_hello(self, data):
        peer, port = data.split()
        print 'got hello from %s' % peer
        self.peers.append((peer, int(port)))

    def handle_update(self, data):
        print 'got update %s' % data

        epoch, freq, freq_epoch = [int(x) for x in data.split(':')]

        if freq_epoch > self.freq_epoch:
            print 'freq updated to %s (freq_epoch %s)' % (freq, freq_epoch)
            self.freq_epoch = freq_epoch
            self.freq = freq

        if epoch > self.epoch:
            print 'updating epoch to %s' % epoch
            self.epoch = epoch

    def handle_elect_me(self, data):
        peer, epoch = data.split()

        print '%s wants to be elected (epoch %s)' % (peer, epoch)

        epoch = int(epoch)

        if epoch > self.epoch:
            self.epoch = epoch

        if epoch == self.epoch:
            self.send_vote(peer)
        else:
            pass

    def handle_you_have_my_vote(self, data):
        peer, epoch = data.split()

        print '%s votes for me (epoch %s)' % (peer, epoch)

        epoch = int(epoch)

        if epoch == self.epoch:
            self.votes.append(peer)

    def handle_hop(self, data):
        self.change_freq()

    def disconnect(self):
        self.peerfinder.disconnect()
        [c.close() for c in self.clients]
        [h.close() for h in self.signal_watchers]
        self.update_timer.stop()
        self.vote_timer.stop()
        self.server.close()

    def _signal_cb(self, handle, signum):
        self.disconnect()

    def _on_timer(self, timer):
        # print '_on_timer'

        if (self.freq < 0) and (self.epoch == 0):
            self.change_freq()
            return

        if self.peers:
            self.send_update()

    def change_freq(self):
        print 'trying to change freq'

        self.epoch += 1
        self.new_freq = random.randint(0, 1000)

        self.send_elect()

        def on_timer(timer):
            print 'voting time is up'

            timer.stop()

            vote_count = len(self.votes)
            peer_count = len(self.peers)
            peer_count_m = peer_count

            if peer_count % 2 != 0:
                peer_count_m = peer_count_m + 1

            if ((peer_count > 1) and (vote_count > peer_count/2)) or ((peer_count == 1) and (vote_count == 1)):
                print "i'm elected"

                self.freq_epoch = self.epoch
                self.freq = self.new_freq

                self.send_update()

            self.votes = []

            self.voting = False

        self.vote_timer.start(on_timer, 5, 0)

    def send_update(self):
        if self.voting:
            return

        peer, port = random.choice(self.peers)

        print 'sending update to port %s' % port

        def on_connected(handle, error):
            handle.write('update %s:%s:%s\n' % (self.epoch, self.freq, self.freq_epoch),
                after_write)

        def after_write(handle, error):
            handle.close()

        handle = pyuv.TCP(self.server.loop)
        handle.connect(('127.0.0.1', int(port)), on_connected)

    def send_elect(self):
        print 'sending elects'

        self.voting = True

        def on_connected(handle, error):
            handle.write('elect_me %s %s\n' % (self.client_id, self.epoch), after_write)

        def after_write(handle, error):
            handle.close()

        for peer, port in self.peers:
            handle = pyuv.TCP(self.server.loop)
            handle.connect(('127.0.0.1', int(port)), on_connected)

    def send_vote(self, peer_id):
        peer = [p for p in self.peers if p[0] == peer_id]

        if peer:
            _, port = peer[0]

            def on_connected(handle, error):
                handle.write('you_have_my_vote %s %s\n' % (self.client_id, self.epoch), after_write)

            def after_write(handle, error):
                handle.close()

            handle = pyuv.TCP(self.server.loop)
            handle.connect(('127.0.0.1', int(port)), on_connected)

    def send_hello(self, peer, port):
        print 'saying hello to port %s' % port

        def on_connected(handle, error):
            handle.write('hello %s %s\n' % (self.client_id, self.port),
                on_written)

        def on_written(handle, error):
            handle.close()

        handle = pyuv.TCP(self.server.loop)
        handle.connect(('127.0.0.1', int(port)), on_connected)
