from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import uuid
import time
import threading
import queue


class Client:
    """An SDK (client) for the Synopses Data Engine.
    Args:
      brokers (str) : The URLs on which the Kafka Brokers containing topics used by an SDE job.
                      For more than one brokers split them by commas like <address_1>:<port_1>, <address_2>:<port_2>

      request_topic (str) :  The name of the request topic from which the SDE fetches requests. Defaults to 'request'.
      data_topic (str) : The name of the data topic from which the SDE ingests Datapoints. Defaults to 'data'.
      estimation_topic (str) :  The name of the estimation topic in which the SDE produces estimations upon requests. Defaults to 'estimation'.
      logging_topic (str) : The name of the logging topic in which the SDE produces answers or messages to requests. Defaults to 'logging'.

      message_queue_size (str) : Optionally set the size of the queue in which the Client maintains answers in case they were
                                 not fetched in time. Defaults to 20.

      response_timeout (str) :  Optionally the maximum timeout upon which the Client awaits for a response after a request has been set.
                                Defaults to 10s.
      parallelism (str) : The degree of parallelism under which the SDE runs. Defaults to 2.
    """

    def __init__(
        self,
        brokers: str,
        request_topic: str = "request",
        data_topic: str = "data",
        estimation_topic: str = "estimation",
        logging_topic: str = "logging",
        message_queue_size: int = 20,
        response_timeout: int = 10,
        parallelism: int = 2,
    ):

        # Initiliaze the variables of the Client propagated later to various sub-components
        self._brokers = brokers
        self._request_topic = request_topic
        self._data_topic = data_topic
        self._estimation_topic = estimation_topic
        self._logging_topic = logging_topic
        self._message_queue_size = message_queue_size
        self._response_timeout = response_timeout
        self._parallelism = parallelism

        # Create a Kafka producer.
        self._producer = self._create_producer()

        # Create a Kafka consumer subscribed to the topics where outputs from the SDE are produced.
        self._consumer = self._create_consumer(
            [self._estimation_topic, self._logging_topic]
        )

        # A dict mapping correlation ids to per-request queues.
        self._pending_requests = {}

        # A general response queue for messages not matching a pending request.
        self._response_queue = queue.Queue(maxsize=self._message_queue_size)

        # Event to signal the consumer thread to stop.
        self._stop_event = threading.Event()

        # Start the background consumer thread.
        self._consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._consumer_thread.start()

    def _create_producer(self) -> KafkaProducer:
        """Creates and returns a KafkaProducer with JSON serialization."""
        producer = KafkaProducer(
            bootstrap_servers=self._brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        return producer

    def _create_consumer(self, topics) -> KafkaConsumer:
        """Creates and returns a KafkaConsumer subscribed to the given topics with JSON deserialization."""
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self._brokers,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="sde_client_group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        return consumer

    def _consume_loop(self):
        """Background thread loop: polls Kafka and dispatches incoming messages."""
        while not self._stop_event.is_set():
            try:
                msg_pack = self._consumer.poll(timeout_ms=500)
            except KafkaError as e:
                print(f"Error polling Kafka: {e}")
                continue

            for tp, messages in msg_pack.items():
                for message in messages:
                    # Each message value should be a dict formed by the Message class of SDE (JSON-deserialized)
                    msg_value = message.value
                    external_uid = msg_value.get("relatedRequestIdentifier")

                    if external_uid and external_uid in self._pending_requests:
                        try:
                            # Dispatch to the waiting request's queue.
                            self._pending_requests[external_uid].put(
                                msg_value, timeout=1
                            )
                        except queue.Full:
                            print(
                                f"Pending queue for relatedRequestIdentifier {external_uid} is full. Dropping message."
                            )
                    else:
                        try:
                            # Put into the general response queue.
                            self._response_queue.put(msg_value, timeout=1)
                        except queue.Full:
                            print("General response queue is full. Dropping message.")

    def send_request(self, request_data: dict, key: str) -> dict:
        """
        Sends a JSON request to the request topic and awaits a corresponding response.

        If no 'external_uid' is provided in request_data, one is added automatically.
        The method waits up to self._response_timeout seconds for a response with the same correlation id.
        Raises:
            TimeoutError: if no response is received in time.
        Returns:
            The response message (as a dict) matching the request.
        """
        # Ensure the request has a unique correlation id.
        external_uid = request_data.get("externalUID", uuid.uuid4().hex)
        request_data["externalUID"] = external_uid
        request_data["key"] = key

        # Create a dedicated queue for this pending request.
        pending_queue = queue.Queue(maxsize=1)
        self._pending_requests[external_uid] = pending_queue

        # Send the request to the designated request topic.
        try:
            self._producer.send(self._request_topic, request_data, key.encode("utf-8"))
            self._producer.flush()
        except KafkaError as e:
            del self._pending_requests[external_uid]
            raise RuntimeError(f"Failed to send request: {e}")

        # Wait for the response.
        try:
            response = pending_queue.get(timeout=self._response_timeout)
            return response
        except queue.Empty:
            print(
                f"No response received for external_uid {external_uid} within {self._response_timeout} seconds"
            )

        finally:
            # Clean up the pending request regardless of outcome.
            if external_uid in self._pending_requests:
                del self._pending_requests[external_uid]

    def send_datapoint(self, datapoint_data: dict):
        """
        Sends a datapoint to the data topic.

        Args:
            datapoint_data: A dictionary representing the datapoint to be ingested.
        """
        try:
            self._producer.send(self._data_topic, datapoint_data)
            self._producer.flush()
        except KafkaError as e:
            raise RuntimeError(f"Failed to send datapoint: {e}")

    def send_storage_auth_request(
        self, access_key: str, secret_key: str, session_token: str, endpoint: str
    ):
        """Sends a request to SDE to alter the StorageManager credentials
        used to access the data layer.
        Args:
            access_key: The MinIO compatible access key.
            secret_key: The MinIO compatible secret key.
            session_token: The MinIO compatbile session token.
            endpoint: The endpoint in which the MinIO API listens to.
        """
        payload = {
            "requestID": 101,
            "param": ["sts", access_key, secret_key, session_token, endpoint],
            "noOfP": self._parallelism,
        }
        self.send_request(payload, key="__CLIENT__")

    def get_synopses(self, key: str = "__any__"):
        """Sends a request to SDE to request the maintained synopses.
        Args:
           key: Optionally provide the datasetkey for which the synopses are requested.
        """
        payload = {
            "key": key ,
            "dataSetkey": key,
            "requestID": 1000,
            "param": [],
            "noOfP": self._parallelism,
        }
        return self.send_request(payload, key=key)

    def close(self):
        """
        Gracefully shuts down the client:
        - Stops the consumer thread.
        - Closes the Kafka consumer and producer.
        """
        self._stop_event.set()
        self._consumer_thread.join(timeout=5)
        try:
            self._consumer.close()
        except Exception as e:
            print(f"Error closing Kafka consumer: {e}")
        try:
            self._producer.close()
        except Exception as e:
            print(f"Error closing Kafka producer: {e}")
        print("Client shutdown completed.")
