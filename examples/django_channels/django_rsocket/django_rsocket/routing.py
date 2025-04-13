from django.urls import path
from .consumers import RSocketConsumer

websocket_urlpatterns = [
    path('rsocket', RSocketConsumer.as_asgi()),
]