# pylint: skip-file
# """ pytest fixtures that can be resued across tests
# """
import sys
sys.path.insert(0, "/home/glue_user/workspace/jupyter_workspace/glue/")
import json
import os
import pytest
import boto3
import botocore
import json

from typing import Callable, Any
from unittest.mock import MagicMock

import aiobotocore.awsrequest
import aiobotocore.endpoint
import aiohttp
import aiohttp.client_reqrep
import aiohttp.typedefs
import botocore.awsrequest
import botocore.model
import pytest

import moto
from moto import mock_aws

from scripts.utils.file_loader import FileLoader
from definitions import ROOT_DIR
from moto.server import ThreadedMotoServer
import pg8000 as pg
import sqlparse

from scripts.utils.log_support import set_logger

log = set_logger()



class MockAWSResponse(aiobotocore.awsrequest.AioAWSResponse):
    """
    Mocked AWS Response.

    https://github.com/aio-libs/aiobotocore/issues/755
    https://gist.github.com/giles-betteromics/12e68b88e261402fbe31c2e918ea4168
    """

    def __init__(self, response: botocore.awsrequest.AWSResponse):
        self._moto_response = response
        self.status_code = response.status_code
        self.raw = MockHttpClientResponse(response)

    # adapt async methods to use moto's response
    async def _content_prop(self) -> bytes:
        return self._moto_response.content

    async def _text_prop(self) -> str:
        return self._moto_response.text


class MockHttpClientResponse(aiohttp.client_reqrep.ClientResponse):
    """
    Mocked HTP Response.

    See <MockAWSResponse> Notes
    """

    def __init__(self, response: botocore.awsrequest.AWSResponse):
        """
        Mocked Response Init.
        """

        async def read(self: MockHttpClientResponse, n: int = -1) -> bytes:
            return response.content

        self.content = MagicMock(aiohttp.StreamReader)
        self._loop = None
        self.content.read = read
        self.response = response

    @property
    def raw_headers(self) -> Any:
        """
        Return the headers encoded the way that aiobotocore expects them.
        """
        return {k.encode("utf-8"): str(v).encode("utf-8") for k, v in self.response.headers.items()}.items()


@pytest.fixture(scope="session", autouse=True)
def patch_aiobotocore() -> None:
    """
    Pytest Fixture Supporting S3FS Mocks.

    See <MockAWSResponse> Notes
    """

    def factory(original: Callable[[Any, Any], Any]) -> Callable[[Any, Any], Any]:
        """
        Response Conversion Factory.
        """

        def patched_convert_to_response_dict(
            http_response: botocore.awsrequest.AWSResponse,
            operation_model: botocore.model.OperationModel,
        ) -> Any:
            return original(MockAWSResponse(http_response), operation_model)

        return patched_convert_to_response_dict

    aiobotocore.endpoint.convert_to_response_dict = factory(aiobotocore.endpoint.convert_to_response_dict)


@pytest.fixture(scope="session", autouse=True)
def mock_AWSResponse() -> None:
    class MockedAWSResponse(botocore.awsrequest.AWSResponse):
        raw_headers = {}  # type: ignore

        async def read(self):  # type: ignore
            return self.text

    botocore.awsrequest.AWSResponse = MockedAWSResponse
    moto.core.models.AWSResponse = MockedAWSResponse


@pytest.fixture(scope="session")
def setup_database():
    try:
        con = None
        con = pg.connect(
            database="dwh",
            user="postgres",
            password="pwd123",
            host="se_postgres",
            port=5432,
            # ssl=True,
        )
        cursor = con.cursor()
        log.info("Connection successful")

        postgres_ddl_path = ROOT_DIR + "/tests/sql_files/" + "postgres_ddl.sql"
        statements = []
        with open(postgres_ddl_path, "r") as file:
            statements = sqlparse.split(file.read())

        for statement in statements:
            # log.info(statement)
            cursor.execute(statement)
            # log.info("Execution successful")

    except Exception as exc:
        log.info(f"Exception: {exc} in setup_database()")
        raise exc
    finally:
        if con:
            con.commit()
            cursor.close()


@pytest.fixture(scope="session")
def moto_server():
    server = ThreadedMotoServer()
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope="session")
def s3_client(moto_server):
    """
    Function to create mock aws infrastructure
    """
    with mock_aws():
        os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
        bucket_name = "dev-s3-bucket"
        s3_client = boto3.client("s3", endpoint_url="http://localhost:5000")
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        yield s3_client


@pytest.fixture(scope="session")
def sts_client(moto_server):
    """
    Function to create mock aws infrastructure
    """
    with mock_aws():
        sts_client = boto3.client(
            "sts",
            endpoint_url="http://localhost:5000",
            region_name="us-east-2",
        )
        identity = sts_client.get_caller_identity()

        identity["Arn"] = "arn:aws:sts::123456789012:user/moto"
        identity["UserId"] = "AKIAIOSFODNN7EXAMPLE"
        identity["Account"] = "123456789012"
        yield sts_client


@pytest.fixture(scope="session")
def sns_client(moto_server):
    """
    Function to create mock aws infrastructure
    """
    with mock_aws():
        sns_client = boto3.client(
            "sns",
            region_name="us-east-2",
            endpoint_url="http://localhost:5000",
        )
        # Create topic
        response = sns_client.create_topic(Name="sns_for_glue")
        topic_arn = response["TopicArn"]
        yield topic_arn
