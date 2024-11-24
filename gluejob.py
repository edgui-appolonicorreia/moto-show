from moto import mock_aws
#from dotenv import load_dotenv
from icecream import ic
import pytest
import boto3
import unittest
from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StructType, StructField, StringType


@mock_aws
class TestGlue(unittest.TestCase):

    @pytest.fixture(scope="session")
    def glueContext():
        """
        Function to setup test environment for PySpark and Glue
        """
        ic()
        spark_context = SparkContext()
        glueContext = GlueContext(spark_context)
        yield glueContext
        spark_context.stop()

    def test_glue(self):
        ic()
        # Mocking the AWS services
        s3 = boto3.resource("s3")
        bucket = s3.Bucket("XXXXXXXXXXX")
        bucket.create()

        # Running the Glue job
        func_to_test("test-bucket", "/path/to/obj", b"abc")

        # Checking the file was uploaded as expected
        object = s3.Object("test-bucket", "/path/to/obj")
        actual = object.get()["Body"].read()
        self.assertEqual(actual, b"abc")