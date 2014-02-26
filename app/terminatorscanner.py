class TerminatorScanner(object):
    def __init__(self, terminator=None):
        self.in_buffer = ''
        self.terminator = terminator

    def handle_read(self, data):
        self.in_buffer = self.in_buffer + data

        terminator_len = len(self.terminator)

        while True:
            index = self.in_buffer.find(self.terminator)

            if index < 0:
                break

            incoming = self.in_buffer[:index]
            self.in_buffer = self.in_buffer[index+terminator_len:]

            self.handle_incoming(incoming)

    def handle_incoming(self, data):
        raise NotImplementedError('must be implemented in subclass')


class TerminatorScannerBasic(TerminatorScanner):
    def __init__(self, terminator=None):
        super(TerminatorScannerBasic, self).__init__(terminator)
        self.incoming = []

    def handle_incoming(self, data):
        self.incoming.append(data)
