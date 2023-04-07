Extensions
==========

Transports
----------

TCP
~~~

.. automodule:: rsocket.transports.tcp
    :members:

Websocket
~~~~~~~~~

aiohttp
+++++++

.. automodule:: rsocket.transports.aiohttp_websocket
    :members:

quart
+++++

.. automodule:: rsocket.transports.quart_websocket
    :members:

quic
~~~~

.. automodule:: rsocket.transports.aioquic_transport
    :members:

http3
~~~~~

.. automodule:: rsocket.transports.http3_transport
    :members:

Routing
-------

RequestRouter
~~~~~~~~~~~~~

.. automodule:: rsocket.routing.request_router
    :members:

RoutingRequestHandler
~~~~~~~~~~~~~~~~~~~~~

.. automodule:: rsocket.routing.routing_request_handler
    :members:


Load Balancer
-------------

.. automodule:: rsocket.load_balancer.load_balancer_rsocket
    :members:

Strategies
~~~~~~~~~~

.. automodule:: rsocket.load_balancer.round_robin
    :members:
    :inherited-members:

.. automodule:: rsocket.load_balancer.random_client
    :members:
    :inherited-members:
