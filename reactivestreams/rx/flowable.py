from rxbp.flowables.anonymousflowablebase import AnonymousFlowableBase
from rxbp.init.initflowable import init_flowable
from rxbp.init.initsubscription import init_subscription
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class DefaultObservable(Observable):
    def __init__(self, scheduler: Subscriber = None):
        self.scheduler = scheduler

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer

        def action(_, __):
            observer.on_error(RuntimeError("Not implemented"))

        return self.scheduler.subscribe_scheduler.schedule(action)


def default_flowable(subscriber: Subscriber = None):
    def func(default: Subscriber) -> Subscription:
        _subscriber = subscriber or default

        return init_subscription(
            observable=DefaultObservable(_subscriber)
        )

    return init_flowable(AnonymousFlowableBase(func))
