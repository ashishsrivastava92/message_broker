import threading
from model.message_model import Message
from model.topic_consumer_model import TopicSubscriber


class Topic:
    """Topic to store messages in order of their publishing time"""

    def __init__(self, name: str) -> None:
        self.name = name
        self.messages = []
        self.subscribers = []
        self.lock = threading.Lock()

    def add_message(self, message: str) -> None:
        """Add message to the topic"""
        # Acquire lock before updating the message broker.
        with self.lock:
            self.messages.append(Message(message))

    def add_subscriber(self, subscriber: TopicSubscriber) -> None:
        self.subscribers.append(subscriber)
