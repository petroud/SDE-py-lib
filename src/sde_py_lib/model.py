from enum import Enum
from .client import Client  # Assumes you have a Kafka-based Client implementation
import random


def validate_properties(method):
    """
    Decorator that checks if the required attributes have been set on the instance.
    Raises a ValueError if any of the required attributes are missing.
    """

    def wrapper(self, *args, **kwargs):
        required_attributes = ["_streamID", "_key", "_uid", "_parallelism"]
        if not all(hasattr(self, attr) for attr in required_attributes):
            raise ValueError(
                "Missing required attributes. Ensure object is in sync with SDE"
            )
        return method(self, *args, **kwargs)

    return wrapper


class SynopsisSpec(Enum):
    CountMin = {
        "id": 1,
        "name": "CountMin",
        "parameters": [
            "KeyField",
            "ValueField",
            "epsilon",
            "confidence",
            "seed",
        ],
    }
    BloomFilter = {
        "id": 2,
        "name": "BloomFilter",
        "parameters": [
            "KeyField",
            "ValueField",
            "numberOfElements",
            "FalsePositive",
        ],
    }
    AMSSynopsis = {
        "id": 3,
        "name": "AMS",
        "parameters": ["KeyField", "ValueField", "Depth", "Buckets"],
    }


class OperationMode(Enum):
    QUERYABLE = "Queryable"
    CONTINUOUS = "Continuous"


class PartitioningMode(Enum):
    KEYED = 1
    PARTITIONING = 4


class Synopsis:
    def __init__(self, spec: SynopsisSpec, client: Client):
        """
        Initialize the Synopsis instance with a specific specification and Kafka client.

        Args:
            spec (SynopsisSpec): The specification for the synopsis (e.g., CountMin, BloomFilter, AMS).
            client (Client): An instance of the client.
        """
        self._spec = spec
        self._client = client

    def _validate_parameters(self, param: dict):
        """
        Validate that the provided parameters exactly match the expected keys from the spec.

        Args:
            param (dict): The parameters dictionary to validate.

        Raises:
            ValueError: If there are missing or unexpected parameter keys.
        """
        expected_keys = set(self._spec.value["parameters"])
        provided_keys = set(param.keys())
        if expected_keys != provided_keys:
            missing = expected_keys - provided_keys
            extra = provided_keys - expected_keys
            message = "Invalid parameters provided."
            if missing:
                message += f" Missing keys: {missing}."
            if extra:
                message += f" Unexpected keys: {extra}."
            raise ValueError(message)

    def new(
        self,
        streamID: str,
        key: str,
        parallelism: int,
        uid: int,
    ) -> dict:
        """
        Create a new synopsis instance.

        Args:
            streamID (str): The stream identifier used to identify which tuple reach which Synopsis
            key (str): The key identifier in case of batch processing (datasetKey)
            parallelism (int): Degree of parallelism.
            uid (int): The UID of the Synopsis in the SDE.

        Returns:
            Initialized synopsis object
        """
        self._parallelism = parallelism
        self._streamID = streamID
        self._key = key
        self._uid = uid
        return self

    def add(
        self,
        streamID: str,
        key: str,
        param: dict,
        parallelism: int,
        uid: int,
        partition_mode: PartitioningMode = PartitioningMode.KEYED,
        operation_mode: OperationMode = OperationMode.QUERYABLE,
    ) -> dict:
        """
        Add (instantiate) a new synopsis instance.

        Args:
            streamID (str): The stream identifier used to identify which tuple reach which Synopsis
            key (str): The key identifier in case of batch processing (datasetKey)
            param (dict): Instantiation parameters as list dict with keys (e.g., {"KeyField": "StockID", "ValueField": "price", ...}).
            parallelism (int): Degree of parallelism.
            uid (int): The UID of the Synopsis in the SDE.
            partition_mode: The mode upon which the parallelization scheme is enforced (Random Partitioning VS Keyed Partitioning).
            operation_mode: The mode upon which estimations are received (Queryable for on-demand, Continuous for emition upon data arrival).
        Returns:
            dict: The response returned by the client.
        """
        self._validate_parameters(param)
        params = list(param.values())
        self._parallelism = parallelism
        self._streamID = streamID
        self._key = key
        self._uid = uid
        # Decide the operation mode of the Synopsis, Queryable or Continuous
        # Decide also the partitioning mode (Keyed or Random). Continous mode overrides Random partitioning.
        req_id = partition_mode.value
        req_id = 5 if operation_mode == OperationMode.CONTINUOUS else req_id
        params.insert(2, operation_mode.value)
        request_payload = {
            "key": key,
            "streamID": streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 1,  # ADD operati
            "dataSetkey": key,
            "param": params,
            "noOfP": parallelism,
            "uid": uid,
        }
        return self._client.send_request(request_payload, key)

    @validate_properties
    def delete(self) -> dict:
        """
        Delete an existing synopsis instance.

        Returns:
            dict: The response from the deletion request.
        """
        request_payload = {
            "streamID": self._streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 2,
            "dataSetkey": self._key,
            "uid": self._uid,
            "noOfP": self._parallelism,
        }
        return self._client.send_request(request_payload, self._key)

    @validate_properties
    def estimate(self, params: list) -> dict:
        """
        Estimate an existing synopsis.

        Returns:
            dict: The response from the estimation request.
        """
        request_payload = {
            "streamID": self._streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 3,
            "dataSetkey": self._key,
            "uid": self._uid,
            "noOfP": self._parallelism,
            "param": params if params else [],
        }
        return self._client.send_request(request_payload, self._key)

    @validate_properties
    def snapshot(self) -> dict:
        """
        Create a snapshot of the current synopsis state.
        Synopsis should have been added to the SDE first by calling add()
        or fetched through it.

        Returns:
            dict: The response from the snapshot request.
        """
        request_payload = {
            "streamID": self._streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 100,
            "dataSetkey": self._key,
            "uid": self._uid,
            "noOfP": self._parallelism,
            "param": [],
        }
        return self._client.send_request(request_payload, self._key)

    @validate_properties
    def list_snapshots(self) -> dict:
        """
        Lists the snapshots of a Synopsis that have been captured in the past as files from MinIO.

        Returns:
            dict: The response from the list snapshot request.
        """
        request_payload = {
            "streamID": self._streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 301,
            "dataSetkey": self._key,
            "uid": self._uid,
            "noOfP": self._parallelism,
            "param": [],
        }
        return self._client.send_request(request_payload, self._key)

    @validate_properties
    def load_latest_snapshot(self) -> dict:
        """
        Load the latest snapshot for this synopsis.

        Returns:
            dict: The response containing the SDE's response
        """
        request_payload = {
            "streamID": self._streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 200,
            "dataSetkey": self._key,
            "uid": self._uid,
            "noOfP": self._parallelism,
            "param": [],
        }
        return self._client.send_request(request_payload, self._key)

    @validate_properties
    def load_custom_snapshot(self, version_number: int = 0) -> dict:
        """
        Load a specific snapshot for this synopsis.
        Args:
            version_number: The version number of the snapshot
                            to load from the data layer.
        Returns:
            dict: The response containing the SDE's response
        """
        params = [version_number]
        request_payload = {
            "streamID": self._streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 201,
            "dataSetkey": self._key,
            "uid": self._uid,
            "noOfP": self._parallelism,
            "param": params if params else [0],
        }
        return self._client.send_request(request_payload, self._key)

    @validate_properties
    def new_synopsis_from_snapshot(
        self, version_number: int = 0, new_uid: int = random.randint(90000, 100000)
    ) -> dict:
        """
        Instantiate a new synopsis using a stored snapshot.

        Uses a custom requestID (e.g., 11) for instantiation from snapshot.

        Args:
            version_number: The version_number of the snapshot
                            the new synopsis will be generated upon.
            new_uid: The UID of the new Synopsis.
        Returns:
            dict: The response from the instantiation request.
        """
        params = [version_number, new_uid]
        request_payload = {
            "streamID": self._streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 202,
            "dataSetkey": self._key,
            "param": params if params else [],
            "noOfP": self._parallelism,
            "uid": self._uid,
        }
        return self._client.send_request(request_payload, self._key)
