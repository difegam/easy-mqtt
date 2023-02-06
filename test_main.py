import unittest

from mqtt_client import MqttClient


class TestMqttClient(unittest.TestCase):

    def setUp(self):
        self.mqtt_client = MqttClient("test-client", "test.mosquitto.org", 1883)
        self.mqtt_client.connect()

    def test_publish(self):
        self.mqtt_client.publish("test/topic", "test message")
        self.assertTrue(True)

    def test_subscribe(self):

        def callback(topic, message):
            self.assertEqual(message.topic, "test/topic")
            self.assertEqual(message.payload.decode("utf-8"), "test message")

        self.mqtt_client.subscribe("test/topic", callback)
        self.mqtt_client.publish("test/topic", "test message")
        self.mqtt_client.start()

    def tearDown(self):
        self.mqtt_client.stop()


if __name__ == "__main__":
    unittest.main()
