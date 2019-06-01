import json

from gevent import sleep

from logger import log


class Event(object):
    def __init__(self, event, data, channel='general'):
        self.event = event
        self.data = data
        self.channel = channel


class EventSubscriber(object):
    def __init__(self, channels=['general']):
        self.channels = channels
        self.events = []
        self.stream = None

    def receive(self, event):
        self.events.append(event)

    def subscribe(self, stream):
        self.stream = stream
        stream.subscribe(self)

    def unsubscribe(self):
        self.stream.unsubscribe(self)

    def read(self):
        try:
            log('Subscriber \'%s\' listening on channels: %s' % (self, ', '.join(self.channels)))
            while True:
                sleep(0.1)
                if len(self.events) > 0:
                    new_events = self.events
                    self.events = []
                    for event in new_events:
                        log('Subscriber \'%s\': read event \'%s\'' % (self, event.event))
                        yield self.serialize(event)
        except GeneratorExit:
            self.unsubscribe()
            log('Subscriber \'%s\' disconnected' % self)

    def serialize(self, event):
        return event


class JavascriptEventSourceSubscriber(EventSubscriber):
    def serialize(self, event):
        # FIXME: Doesn't seem to be working
        # return '\n'.join([ ': '.join([ 'event', event.event ]), ': '.join([ 'data', json.dumps(event.data) ]), '', '' ])
        return '\n'.join([': '.join(['data', json.dumps(event.__dict__)]), '', ''])


class EventStream(object):
    def __init__(self):
        self.subscribers = []

    def subscribe(self, subscriber):
        if subscriber not in self.subscribers:
            self.subscribers.append(subscriber)

    def unsubscribe(self, subscriber):
        self.subscribers = filter(lambda s: s != subscriber, self.subscribers)

    def subscriber(self, subtype=JavascriptEventSourceSubscriber, channels=['general']):
        subscriber = subtype(channels)
        subscriber.subscribe(self)
        return subscriber.read()

    def publish(self, event):
        for subscriber in self.subscribers:
            if event.channel in subscriber.channels:
                subscriber.receive(event)
