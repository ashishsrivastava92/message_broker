import threading
import time

from consumer import ISubscriber, SleepingSubscriber
from model.topic_model import Topic
from model.topic_consumer_model import TopicSubscriber
from broker.topic_service import TopicService


class BrokerService:
    """Messaging broker service implementation"""

    def __init__(self) -> None:
        # stores all topic handlers
        self.topic_handlers = {}
        self.threads = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # join all threads
        # for t in self.threads:
        #     t.join()
        # # shutdown threadpool executor running per handler
        # for t_h in self.topic_handlers.keys():
        #     self.topic_handlers[t_h].shutdown()
        return

    def create_topic(self, name: str) -> Topic:
        """
        Create a new topic and add it to handler.
        """
        _topic = Topic(name)
        self.topic_handlers[name] = TopicService(_topic)
        return _topic

    def subscribe(self, sub_name: ISubscriber, topic: Topic) -> None:
        """
        Subscribe to a topic.
        """
        topic.add_subscriber(TopicSubscriber(sub_name))

    def publish(self, topic: Topic, msg: str) -> None:
        """
        Publish message to a topic"""
        topic.add_message(msg)
        # spawn a new thread to notify handler about the new message.
        t = threading.Thread(target=self.topic_handlers[topic.name].publish)
        t.start()

    # def reset_offset(self, topic: Topic, subscriber: ISubscriber, offset: int) -> bool:
    #     for t_sub in topic.subscribers:
    #         if t_sub.subscriber == subscriber:
    #             t_sub.offset = offset
    #             t = threading.Thread(
    #                 target=self.topic_handlers[
    #                     topic.name].start_subscriber_worker, args=(t_sub,))
    #             t.start()
    #             return True
    #     return False


if __name__ == "__main__":

    # with BrokerService() as ms:
    ms = BrokerService()
    subscriber = SleepingSubscriber('sub1', 0.1)
    subscriber2 = SleepingSubscriber('sub2', 0.1)
    subscriber3 = SleepingSubscriber('sub3', 0.1)
    subscriber4 = SleepingSubscriber('sub4', 0.1)
    subscriber5 = SleepingSubscriber('sub5', 0.1)
    subscriber6 = SleepingSubscriber('sub6', 0.1)
    subscriber7 = SleepingSubscriber('sub7', 0.1)
    topic = ms.create_topic('product')
    ms.subscribe(subscriber, topic)
    ms.subscribe(subscriber2, topic)
    ms.subscribe(subscriber3, topic)
    ms.subscribe(subscriber4, topic)
    ms.subscribe(subscriber5, topic)
    ms.subscribe(subscriber6, topic)
    ms.subscribe(subscriber7, topic)
    i = 0
    while i < 5:
        ms.publish(topic, 'Car')
        ms.publish(topic, 'Truck')
        ms.publish(topic, 'Bus')
        ms.publish(topic, 'Cycle')
        ms.publish(topic, 'Tri-Cycle')
        ms.publish(topic, 'Van')
        time.sleep(3)
        print('-----------', i)
        i += 1
    time.sleep(5)
        # ms.reset_offset(topic, subscriber, 5)
