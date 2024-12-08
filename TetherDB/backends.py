from logging import Logger
import ssl

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
            # TODO Fix this https://docs.openstack.org/etcd3gw/latest/_modules/etcd3gw/client.html#Etcd3Client
            if etcd_cfg.get("use_ssl"):
                ssl_context = ssl.create_default_context(
                    cafile=etcd_cfg.get("ca_cert_file")
                )
                ssl_context.load_cert_chain(
                    certfile=etcd_cfg.get("cert_file"), keyfile=etcd_cfg.get("key_file")
                )
            self.etcd = Etcd3Client(
                host=etcd_cfg["host"],
                port=etcd_cfg["port"],
                timeout=etcd_cfg.get("timeout", 5)
            )
            self.logger.debug("etcd backend initialized.")
        except Exception as e:
            self.logger.error(f"Error initializing etcd backend: {e}")
