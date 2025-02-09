from enum import Enum
from .client import Client  # Assumes you have a Kafka-based Client implementation


class SynopsisSpec(Enum):
    CountMin = {
        "id": 1,
        "name": "CountMin",
        "parameters": ["KeyField", "ValueField", "OperationMode", "epsilon", "confidence", "seed"]
    }
    BloomFilter = {
        "id": 2,
        "name": "BloomFilter",
        "parameters": ["KeyField", "ValueField", "OperationMode", "numberOfElements", "FalsePositive"]
    }
    AMSSynopsis = {
        "id": 3,
        "name": "AMS",
        "parameters": ["KeyField", "ValueField", "OperationMode", "Depth", "Buckets"]
    }


class OperationMode(Enum):
    QUERYABLE = "queryable"
    CONTINUOUS = "continuous"


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

    def add(self, streamID: str, key: str, param: dict, parallelism: int, uid: int) -> dict:
        """
        Add (instantiate) a new synopsis instance.

        Uses requestID 1 for "ADD" (with keyed partitioning as per your table).

        Args:
            streamID (str): The stream identifier used to identify which tuple reach which Synopsis
            key (str): The key identifier in case of batch processing (datasetKey)
            param (dict): Instantiation parameters as list dict with keys (e.g., {"KeyField": "StockID", "ValueField": "price", ...}).
            parallelism (int): Degree of parallelism.
            uid (int): The UID of the Synopsis in the SDE

        Returns:
            dict: The response returned by the client.
        """
        self._validate_parameters(param)

        self._parallelism = parallelism
        self._streamID = streamID
        self._key = key

        request_payload = {
            "key": key,
            "streamID": streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 1,  # ADD operation
            "dataSetkey": key,
            "param": param,
            "noOfP": parallelism,
            "uid": uid
        }
        return self._client.send_request(request_payload)

    def delete(self, streamID: str, datasetKey: str, uid: int) -> dict:
        """
        Delete an existing synopsis instance.

        Uses requestID 2 for DELETE.

        Args:
            streamID (str): The stream identifier.
            datasetKey (str): The key used for grouping.
            uid (int): User or job identifier.
        
        Returns:
            dict: The response from the deletion request.
        """
        request_payload = {
            "streamID": streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 2,  # DELETE operation
            "dataSetkey": datasetKey,
            "uid": uid
        }
        return self._client.send_request(request_payload)

    def snapshot(self, streamID: str, datasetKey: str, uid: int) -> dict:
        """
        Create a snapshot of the current synopsis state.

        Uses a custom requestID (e.g., 8) for snapshot operation.

        Args:
            streamID (str): The stream identifier.
            datasetKey (str): The key used for grouping.
            uid (int): User or job identifier.
        
        Returns:
            dict: The response from the snapshot request.
        """
        request_payload = {
            "streamID": streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 100,
            "dataSetkey": datasetKey,
            "uid": uid,
            "noOfP": 1,
            "param": [],
        }
        return self._client.send_request(request_payload, datasetKey)

    def loadLatestSnapshot(self, streamID: str, datasetKey: str, uid: int) -> dict:
        """
        Load the latest snapshot for this synopsis.

        Uses a custom requestID (e.g., 9) for loading the latest snapshot.

        Args:
            streamID (str): The stream identifier.
            datasetKey (str): The key used for grouping.
            uid (int): User or job identifier.
        
        Returns:
            dict: The response containing the latest snapshot.
        """
        request_payload = {
            "streamID": streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 200,  # Load latest snapshot operation
            "dataSetkey": datasetKey,
            "uid": uid
        }
        return self._client.send_request(request_payload)

    def loadCustomSnapshot(self, streamID: str, datasetKey: str, snapshotID: int, uid: int) -> dict:
        """
        Load a specific (custom) snapshot for this synopsis.

        Uses a custom requestID (e.g., 10) for loading a custom snapshot.

        Args:
            streamID (str): The stream identifier.
            datasetKey (str): The key used for grouping.
            snapshotID (int): The identifier of the desired snapshot.
            uid (int): User or job identifier.
        
        Returns:
            dict: The response containing the custom snapshot.
        """
        request_payload = {
            "streamID": streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 10,  # Load custom snapshot operation
            "dataSetkey": datasetKey,
            "snapshotID": snapshotID,
            "uid": uid
        }
        return self._client.send_request(request_payload)

    def instatiateNewSynopsisFromSnapshot(
        self, streamID: str, datasetKey: str, snapshotID: int, param: dict, noOfP: int, uid: int
    ) -> dict:
        """
        Instantiate a new synopsis using a stored snapshot.

        Uses a custom requestID (e.g., 11) for instantiation from snapshot.

        Args:
            streamID (str): The stream identifier.
            datasetKey (str): The key used for grouping.
            snapshotID (int): The snapshot identifier from which to instantiate.
            param (dict): Instantiation parameters (should match the spec).
            noOfP (int): Number of partitions (or parallelism).
            uid (int): User or job identifier.
        
        Returns:
            dict: The response from the instantiation request.
        """
        self._validate_parameters(param)
        request_payload = {
            "streamID": streamID,
            "synopsisID": self._spec.value["id"],
            "requestID": 11,  # Instantiate from snapshot operation
            "dataSetkey": datasetKey,
            "snapshotID": snapshotID,
            "param": param,
            "noOfP": noOfP,
            "uid": uid
        }
        return self._client.send_request(request_payload)
