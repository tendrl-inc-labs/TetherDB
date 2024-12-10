from logging import Logger

import boto3
from etcd3gw.client import Etcd3Client


class BackendInitializer:
    """
    Handles initialization for backends: local (dbm), DynamoDB, and etcd.
    """

    __slots__ = ["local_db_file", "dynamodb_table", "etcd", "logger"]

    def __init__(self, config: dict, logger: Logger):
        self.logger = logger
        self.local_db_file = None
        self.dynamodb_table = None
        self.etcd = None

        self._init_local(config)
        self._init_dynamodb(config)
        self._init_etcd(config)

    def _init_local(self, config: dict):
        if "local" in config:
            self.local_db_file = config["local"]["filepath"]
            self.logger.debug(f"Local backend initialized at {self.local_db_file}")

    def _init_dynamodb(self, config: dict):
        try:
            dynamo_cfg = config.get("dynamodb", {})
            if "table_name" in dynamo_cfg:
                self.dynamodb_table = boto3.resource("dynamodb").Table(
                    dynamo_cfg["table_name"]
                )
                self.logger.debug("DynamoDB backend initialized.")
        except Exception as e:
            self.logger.error(f"Error initializing DynamoDB backend: {e}")

    def _init_etcd(self, config: dict):
        try:
            etcd_cfg = config.get("etcd", {})
            host = etcd_cfg.get("host", "localhost")
            port = etcd_cfg.get("port", 2379)
            timeout = etcd_cfg.get("timeout", 5)

            # SSL parameters
            use_ssl = etcd_cfg.get("use_ssl", False)
            ca_cert = etcd_cfg.get("ca_cert_file")
            cert_file = etcd_cfg.get("cert_file")
            key_file = etcd_cfg.get("key_file")

            # Initialize etcd client
            if use_ssl:
                self.etcd = Etcd3Client(
                    host=host,
                    port=port,
                    protocol="https",
                    ca_cert=ca_cert,
                    cert_cert=cert_file,
                    cert_key=key_file,
                    timeout=timeout,
                )
            else:
                self.etcd = Etcd3Client(
                    host=host,
                    port=port,
                    timeout=timeout,
                )

            self.logger.debug("etcd backend initialized.")
        except Exception as e:
            self.logger.error(f"Error initializing etcd backend: {e}")
