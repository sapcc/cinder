"""Empirical test: stop_consuming during drain (the 'deregister').

Verifies with oslo.messaging 17.1.0 + eventlet + kombu memory transport:
  1. In-flight RPC handler completes after deregister (not killed).
  2. Outbound RPC call from the in-flight handler still works after deregister.
  3. New casts sent after deregister are NOT consumed by the draining server
     (they stay queued), and ARE consumed by a second server (the "new pod")
     on the same topic.
"""
import sys
import time

import eventlet
eventlet.monkey_patch()

import faulthandler
faulthandler.dump_traceback_later(25, exit=True)

from oslo_config import cfg
import oslo_messaging as messaging

TRANSPORT_URL = 'kombu+memory://'
TOPIC = 'test_topic'
AUX_TOPIC = 'aux_topic'

CONF = cfg.CONF
messaging.set_transport_defaults(control_exchange='cinder')

results = {}


class AuxEndpoint(object):
    target = messaging.Target(topic=AUX_TOPIC, exchange='cinder',
                              version='1.0')

    def ping(self, ctx, tag):
        return 'pong-%s' % tag


class DrainingEndpoint(object):
    target = messaging.Target(version='1.0')

    def __init__(self):
        self.received = []
        self.events = {}

    def do_work(self, ctx, delay=1.0, call_aux=False):
        """Long-running op (in-flight handler)."""
        self.received.append(('do_work', time.time()))
        eventlet.sleep(delay)
        outbound = None
        if call_aux:
            # Make an outbound RPC call while the drain (deregister) is active
            transport = messaging.get_rpc_transport(CONF, TRANSPORT_URL)
            client = messaging.RPCClient(transport, AuxEndpoint.target)
            outbound = client.call(ctx, 'ping', tag='after-deregister')
            client = None
        self.received.append(('do_work_done', time.time(), outbound))
        return 'work-done'

    def quick(self, ctx, tag=None):
        self.received.append(('quick', tag, time.time()))
        return 'quick-ok'


def main():
    transport = messaging.get_rpc_transport(CONF, TRANSPORT_URL)

    # Server A: the "draining pod"
    ep_a = DrainingEndpoint()
    target_a = messaging.Target(topic=TOPIC, server='A', exchange='cinder')
    server_a = messaging.get_rpc_server(transport, target_a, [ep_a],
                                        executor='eventlet')

    # Aux server C: target for outbound calls from in-flight op
    ep_c = AuxEndpoint()
    server_c = messaging.get_rpc_server(
        transport, messaging.Target(topic=AUX_TOPIC, server='C',
                                    exchange='cinder'),
        [ep_c], executor='eventlet')

    server_a.start()
    server_c.start()

    # Probe the listener structure to find the connection
    listener = server_a.listener
    print('listener type:', type(listener).__name__)
    ps = getattr(listener, '_poll_style_listener', None)
    print('poll_style_listener:', type(ps).__name__ if ps else None)
    if ps is not None:
        print('RpcAMQPListener attrs:', [a for a in dir(ps)
                                         if not a.startswith('__')])
    conn = None
    if ps is not None:
        for attr in ('conn', 'connection', '_connection'):
            if hasattr(ps, attr):
                conn = getattr(ps, attr)
                print('found connection via', attr, '->', type(conn).__name__)
                break
    if conn is not None:
        print('connection has stop_consuming:',
              hasattr(conn, 'stop_consuming'))

    client = messaging.RPCClient(
        transport,
        messaging.Target(topic=TOPIC, exchange='cinder', version='1.0'))

    # 1) Send a long-running cast to A (single consumer -> A gets it)
    client.cast({}, 'do_work', delay=2.0, call_aux=True)
    eventlet.sleep(0.5)  # let the handler start
    in_flight = len(ep_a.received)
    print('STEP1 in-flight op started on A:', in_flight == 1)

    # 2) Deregister A (stop consuming) mid-op.
    #    In 17.1.0, MessageHandlingServer.stop() == listener.stop(): stops
    #    consumption, joins the listen thread, does NOT touch the work
    #    executor, so in-flight handlers keep running. This is what
    #    cinder's rpcserver.stop() calls.
    server_a.stop()
    print('STEP2 deregistered A (server_a.stop())')

    # 3) Send a new cast AFTER deregister -> must NOT be consumed by A
    client.cast({}, 'quick', tag='post-deregister')
    eventlet.sleep(0.7)
    quick_after = [r for r in ep_a.received if r[0] == 'quick']
    print('STEP3 A consumed post-deregister cast (want False):',
          len(quick_after) > 0)

    # 4) Start server B ("new pod") on same topic, send another cast
    ep_b = DrainingEndpoint()
    server_b = messaging.get_rpc_server(
        transport, messaging.Target(topic=TOPIC, server='B', exchange='cinder'),
        [ep_b], executor='eventlet')
    server_b.start()
    client.cast({}, 'quick', tag='to-new-pod')
    eventlet.sleep(0.7)
    print('STEP4 B consumed cast after A deregistered:',
          len(ep_b.received) > 0)

    # 5) Check A's in-flight op completed AND outbound call worked
    eventlet.sleep(2.0)  # let do_work (2s) finish
    done = [r for r in ep_a.received if r[0] == 'do_work_done']
    print('STEP5 A in-flight op completed after deregister:',
          len(done) == 1)
    if done:
        outbound = done[0][2]
        print('STEP5 A outbound RPC reply after deregister:', outbound)

    # 6) cinder's _drain_pool path: read server._work_executor._pool AFTER
    #    server.stop() and waitall() on it — must still exist and return.
    wex = getattr(server_a, '_work_executor', None)
    print('STEP6 work_executor still present after stop():',
          wex is not None)
    if wex is not None:
        pool = getattr(wex, '_pool', None)
        print('STEP6 pool still present after stop():', pool is not None)
        if pool is not None:
            pool.waitall()
            print('STEP6 pool.waitall() after stop() returned (no hang)')

    # summary
    ok = (in_flight == 1
          and len(quick_after) == 0
          and len(ep_b.received) > 0
          and len(done) == 1
          and done and done[0][2] == 'pong-after-deregister'
          and wex is not None)
    print('\nRESULT:', 'PASS' if ok else 'FAIL')
    print('A received:', [r[0] for r in ep_a.received])
    print('B received:', [r[0] for r in ep_b.received])

    # cleanup
    for s in (server_b, server_a, server_c):
        try:
            s.stop(); s.wait()
        except Exception as e:
            print('cleanup error:', type(e).__name__, str(e)[:100])


if __name__ == '__main__':
    sys.exit(main())
