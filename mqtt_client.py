import logging
from typing import Callable, List, Tuple, Union

import paho.mqtt.client as mqtt
from paho.mqtt.client import connack_string as ack
from paho.mqtt.client import error_string


class MqttClient:

    def __init__(self,
                 client_id: str,
                 broker_url: str,
                 port: int,
                 username: Union[str, None] = None,
                 password: Union[str, None] = None):
        """
        Initialize MQTT client instance with client_id, broker_url, and port.

        :param client_id: Unique identifier for this client.
        :param broker_url: URL or IP address of the MQTT broker.
        :param port: Port number for the MQTT broker.
        """

        self.client_id = client_id
        self.broker_url = broker_url
        self.port = port

        self.client = mqtt.Client(client_id, clean_session=True)
        if username and password:
            self.client.username_pw_set(username=username, password=password)

        self.client.on_message = self.on_message
        self.client.on_connect = self.on_connect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_disconnect = self.on_disconnect

    def on_subscribe(self, client, userdata, mid, granted_qos):
        mqtt_client_id = client._client_id.decode('ascii')
        logging.info(f"MQTT Client {mqtt_client_id} subscribed with mid {str(mid)} received.")

    def connect(self):
        """
        Connect to the MQTT broker specified by the broker_url and port.
        """
        try:
            self.client.connect(self.broker_url, self.port)
        except Exception as e:
            logging.error(f"Error connecting to broker: {str(e)}")

    def publish(self, topic: str, payload: str):
        """
        Publish a message to the specified topic.

        :param topic: Topic to publish the message to.
        :param payload: Message payload to be published.
        """
        try:
            self.client.publish(topic, payload)
        except Exception as e:
            logging.error(f"Error publishing message: {type(e).__name__} - {str(e)}")

    def subscribe(self, topic: str | List[Tuple[str, int]], callback: Callable[[str, dict], None]):
        """
        Subscribe to a topic and set a callback function to be called when a message is received.

        :param topic: Topic to subscribe to.
        :param callback: Callback function to be called when a message is received.
        """
        try:
            self.client.subscribe(topic)
            self.callback = callback
        except Exception as e:
            logging.error(f"Error subscribing to topic: {str(e)}")

    def on_message(self, client, userdata, message):
        """
        Callback function to be called when a message is received.
        """
        try:
            self.callback(message.topic, message.payload.decode("utf-8"))
        except Exception as e:
            client_id = client._client_id.decode('ascii')
            logging.error(f"Callback error: {client_id} - {type(e).__name__}, message: {str(e)}")

    def on_connect(self, client, userdata, flags, rc):
        """
        Callback function to be called when a connection is established.
        """
        client_id = client._client_id.decode('ascii')
        if rc == 0:
            logging.info(f"Connected to broker. Client ID: {client_id} - Connection returned result: {ack(rc)}")
        else:
            logging.error(f"Connection failed. Result code: {str(e)}", rc)

    def on_disconnect(self, client, userdata, rc):
        """
        Callback function to be called when the client disconnects.
        """
        if rc != 0:
            logging.warning(f"Unexpected disconnection. Result code: {str(rc)} - {error_string(rc)}")
        else:
            logging.warning(f"Disconnected from broker. Result code: {str(rc)}")

    def start(self):
        """
        Start the MQTT client loop in the background.
        """
        try:
            self.client.loop_start()
        except Exception as e:
            logging.error(f"Error starting MQTT client loop: {str(e)}")

    def stop(self):
        """
        Stop the MQTT client loop.
        """
        try:
            self.client.loop_stop()
        except Exception as e:
            logging.error(f"Error stopping MQTT client loop: {str(e)}")

    def disconnect(self):
        """
        Disconnect from the MQTT broker.
        """
        try:
            self.client.disconnect()
        except Exception as e:
            logging.error(f"Error disconnecting from broker: {str(e)}")
