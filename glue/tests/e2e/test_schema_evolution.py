import pytest
from typing import Any, Dict
import pg8000 as pg
from botocore.exceptions import ClientError
from scripts.utils.log_support import set_logger
from scripts.schema_evolution import SchemaEvolution
from scripts.utils.file_loader import FileLoader

log = set_logger()

@pytest.mark.usefixtures("setup_database", "s3_client")
class TestSchemaEvolution:
    """
    Class to test schema evolution process.
    """

    def create_conn(self) -> Any:
        """
        Creates a connection to the PostgreSQL database.

        Returns:
            pg8000.dbapi.Connection: Connection object.
        """
        con = None
        con = pg.connect(
            database="dwh",
            user="postgres",
            password="pwd123",
            host="se_postgres",
            port=5432,
        )
        cursor = con.cursor()
        log.info("Connection successful in E2E tests")
        return con


    def setup_mocked_infra(self, s3_client: Any) -> None:
        """
        Sets up mocked infrastructure by uploading raw files to S3.

        Args:
            s3_client (Any): S3 client object.
        """
        bucket_config = "dev-s3-bucket"
        log.info("Uploading raw files in E2E tests")

        try:
            file_path_list_input = ["tests", "raw", "employee.csv"]
            file_path_input = FileLoader.load_data(file_path_list_input)
            s3_client.upload_file(
                file_path_input,
                bucket_config,
                "raw/employee/employee.csv",
            )
            file_path_list_input = ["tests", "raw", "employee_v2.csv"]
            file_path_input = FileLoader.load_data(file_path_list_input)
            s3_client.upload_file(
                file_path_input,
                bucket_config,
                "raw/employee/employee_v2.csv",
            )
            file_path_list_input = ["tests", "raw", "employee_v3.csv"]
            file_path_input = FileLoader.load_data(file_path_list_input)
            s3_client.upload_file(
                file_path_input,
                bucket_config,
                "raw/employee/employee_v3.csv",
            )
            file_path_list_input = ["tests", "raw", "employee_v4.csv"]
            file_path_input = FileLoader.load_data(file_path_list_input)
            s3_client.upload_file(
                file_path_input,
                bucket_config,
                "raw/employee/employee_v4.csv",
            )
            file_path_list_input = ["tests", "raw", "employee_v5.csv"]
            file_path_input = FileLoader.load_data(file_path_list_input)
            s3_client.upload_file(
                file_path_input,
                bucket_config,
                "raw/employee/employee_v5.csv",
            )
            file_path_list_input = ["tests", "raw", "employee_v6.csv"]
            file_path_input = FileLoader.load_data(file_path_list_input)
            s3_client.upload_file(
                file_path_input,
                bucket_config,
                "raw/employee/employee_v6.csv",
            )
            log.info("Uploaded employee raw files in E2E tests")
        except ClientError as client_error:
            if client_error.response["Error"]["Code"] == "ResourceExistsException":
                log.error("config_path already exists")
            log.error(client_error)

    def test_initial_load(self, s3_client: Any) -> None:
        """
        Test initial data load.

        Args:
            s3_client (Any): S3 client object.
        """
        self.setup_mocked_infra(s3_client)
        log.info("Starting test execution")
        schema_evol_obj = SchemaEvolution()
        schema_evol_obj.process()
        log.info("Test execution completed")

        conn = self.create_conn()
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM stg.employee;"
        row_count = cursor.execute(query).fetchall()[0][0]
        assert row_count == 3
    
    def test_column_addition(self, s3_client: Any) -> None:
        """
        Test addition of columns.

        Args:
            s3_client (Any): S3 client object.
        """
        self.setup_mocked_infra(s3_client)
        log.info("Starting test execution")
        schema_evol_obj = SchemaEvolution()
        schema_evol_obj.process("employee_v2.csv")
        log.info("Test execution completed")

        conn = self.create_conn()
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM stg.employee;"
        row_count = cursor.execute(query).fetchall()[0][0]

        schema_dict = schema_evol_obj.get_table_schema("employee")
        assert "experience" in schema_dict.keys()
        assert row_count == 7
    
    def test_column_removal(self, s3_client: Any) -> None:
        """
        Test removal of columns.

        Args:
            s3_client (Any): S3 client object.
        """
        self.setup_mocked_infra(s3_client)
        log.info("Starting test execution")
        schema_evol_obj = SchemaEvolution()
        schema_evol_obj.process("employee_v3.csv")
        log.info("Test execution completed")

        conn = self.create_conn()
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM stg.employee;"
        row_count = cursor.execute(query).fetchall()[0][0]
        schema_dict = schema_evol_obj.get_table_schema("employee")
        assert "experience" in schema_dict.keys()
        assert "last_name" in schema_dict.keys() # column won't be dropped from table
        assert row_count == 11
    
    def test_incompatible_change(self, s3_client: Any) -> None:
        """
        Test incompatible schema changes.

        Args:
            s3_client (Any): S3 client object.
        """
        self.setup_mocked_infra(s3_client)
        log.info("Starting test execution")
        schema_evol_obj = SchemaEvolution()
        with pytest.raises(Exception) as exc:
            schema_evol_obj.process("employee_v4.csv")
        assert exc is not None
        log.info("Test execution completed")

        conn = self.create_conn()
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM stg.employee;"
        row_count = cursor.execute(query).fetchall()[0][0]
        assert row_count == 11
    
    def test_datatype_narrowing(self, s3_client: Any) -> None:
        """
        Test narrowing of data types.

        Args:
            s3_client (Any): S3 client object.
        """
        self.setup_mocked_infra(s3_client)
        log.info("Starting test execution")
        schema_evol_obj = SchemaEvolution()
        schema_evol_obj.process("employee_v5.csv")
        log.info("Test execution completed")

        conn = self.create_conn()
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM stg.employee;"
        row_count = cursor.execute(query).fetchall()[0][0]
        assert row_count == 14
    
    def test_datatype_widening(self, s3_client: Any) -> None:
        """
        Test widening of data types.

        Args:
            s3_client (Any): S3 client object.
        """
        self.setup_mocked_infra(s3_client)
        log.info("Starting test execution")
        schema_evol_obj = SchemaEvolution()
        with pytest.raises(Exception) as exc:
            schema_evol_obj.process("employee_v6.csv")
        assert exc is not None
        log.info("Test execution completed")

        conn = self.create_conn()
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM stg.employee;"
        row_count = cursor.execute(query).fetchall()[0][0]
        assert row_count == 14
