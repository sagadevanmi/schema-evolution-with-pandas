import pandas as pd
import pg8000 as pg
from sqlalchemy import create_engine, Engine, inspect, exc as sqla_exc, text
from scripts.utils.mappings import (
    pandas_to_postgres_datatypes,
    pandas_compatible_datatypes,
    postgres_to_pandas_datatypes,
)
from scripts.utils.log_support import set_logger

logger = set_logger()


class SchemaEvolution:
    def __init__(self) -> None:
        """Constructor"""
        self.database = "dwh"
        self.username = "postgres"
        self.password = "pwd123"
        self.hostname = "se_postgres"
        self.port = 5432
        # Create pg8000 connection
        self.connection = pg.connect(
            database=self.database,
            user=self.username,
            password=self.password,
            host=self.hostname,
            port=5432,
        )

    def get_table_schema(self, table_name: str, postgres_schema: str = "stg") -> dict:
        """Get the schema of a table in Postgres.

        Args:
            table_name (str): The name of the table.
            postgres_schema (str, optional): The schema of the table. Defaults to "stg".

        Returns:
            dict: A dictionary of the table schema.
        """
        schema_exc = None
        schema_dict = {}
        logger.info(f"PG schema: {postgres_schema} in get_table_schema")

        logger.info(f"Trying to get table schema for {table_name}")
        try:
            engine = self.create_sqlalchemy_engine()
            insp = inspect(engine)
            if not insp.has_table(table_name, schema=postgres_schema):
                raise Exception(f"Table {table_name} not present in Postgres")

            connection = engine.connect()
            # Retrieve the current table schema from the database
            query = f"""SELECT column_name, data_type, is_nullable, character_maximum_length FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND table_schema = '{postgres_schema}';"""
            # logger.info(f"{query}")
            table_schema_df = pd.read_sql_query(query, connection)
            table_schema_df["character_maximum_length"] = (
                table_schema_df["character_maximum_length"].astype("float").fillna(-1).astype("int")
            )
            logger.info(f"Pandas query to get schema was successful")
            # connection.close()

            if table_schema_df.empty:
                raise Exception(f"Table {table_name} not present in Postgres")

            schema_dict = table_schema_df.set_index("column_name").agg(lambda x: x.tolist(), axis=1).to_dict()
            schema_dict.pop("auto_id", None)
            logger.info(f"{schema_dict}")

        except Exception as exc:
            logger.error(f"Exc: {exc} while trying to get table schema")
            schema_exc = str(exc)[:150] + " in get_table_schema()"
            raise Exception(schema_exc)

        return schema_dict

    def abbreviate_column_name(self, column_name: str) -> str:
        """Abbreviate a column name to 62 characters.

        Args:
            column_name (str): The column name to abbreviate.

        Returns:
            str: The abbreviated column name.
        """
        if len(column_name) > 62:
            words = column_name.split("_")  # Assuming column names use underscores as separators
            if len(words) < 10:
                abbreviated_words = [word[:6] for word in words]
            elif len(words) < 12:
                abbreviated_words = [word[:5] for word in words]
            elif len(words) < 15:
                abbreviated_words = [word[:4] for word in words]
            elif len(words) < 20:
                abbreviated_words = [word[:3] for word in words]
            abbreviated_name = "_".join(abbreviated_words)
        return abbreviated_name

    def rename_new_columns(self, column_list: list) -> list:
        """Rename new columns to 62 characters.

        Args:
            column_list (list): The list of columns to rename.

        Returns:
            list: The list of renamed columns.
        """
        new_columns_mapping = {}
        for column in column_list:
            if len(column) > 62:
                abb_column = self.abbreviate_column_name(column)
                new_columns_mapping[f"{column}"] = abb_column
            else:
                new_columns_mapping[f"{column}"] = column

        return new_columns_mapping

    def update_table_schema(
        self,
        table_name: str,
        dataframe: pd.DataFrame,
        postgres_schema: str = "stg",
    ) -> pd.DataFrame:
        """Update the schema of a table in Postgres.

        Args:
            table_name (str): The name of the table.
            dataframe (pd.DataFrame): The dataframe with the new schema.
            postgres_schema (str, optional): The schema of the table. Defaults to "stg".

        Returns:
            pd.DataFrame: The dataframe with the updated schema.
        """
        alter_exc = None
        logger.info(f"PG schema in update_schema: {postgres_schema}")
        # Get the existing table schema
        try:
            table_schema = self.get_table_schema(table_name, postgres_schema)
        except Exception as exc:
            logger.error(f"Exc: {exc} while trying to alter table")
            alter_exc = str(exc)[:150] + "in update_table_schema()"
            return dataframe, alter_exc

        file_schema = dataframe.dtypes.to_dict()
        connection = self.create_sqlalchemy_engine().connect()

        # Compare file schema with the existing table schema
        new_columns = set(file_schema.keys()) - set(table_schema.keys())
        removed_columns = set(table_schema.keys()) - set(file_schema.keys())

        logger.info("New Columns")
        logger.info(f"{new_columns}")
        logger.info("Removed Columns")
        logger.info(f"{removed_columns}")

        if len(new_columns) == 0:
            logger.info(f"No changes to be made to the Postgres table")
        else:
            new_columns_mapping = self.rename_new_columns(new_columns)
            new_columns = new_columns_mapping.values()

            try:
                # Alter the table to add new columns
                for column in new_columns:
                    data_type = pandas_to_postgres_datatypes[f"{file_schema[column]}"]
                    query = f"ALTER TABLE {postgres_schema}.{table_name} ADD COLUMN {column} {data_type};"
                    connection.execute(text(query))
                connection.execute(text("COMMIT;"))
                connection.close()
            except Exception as exc:
                logger.error(f"Exc: {exc} while trying to alter table")
                alter_exc = str(exc)[:150] + " in update_table_schema()"

        # Insert null values to dataframe for removed columns
        for column in removed_columns:
            dataframe[column] = None

        return dataframe, alter_exc

    def create_sqlalchemy_engine(self) -> Engine:
        """Create SQLAlchemy Engine

        Returns:
            sqlalchemy.Engine: SQLAlchemy Engine
        """
        try:
            engine = create_engine(
                f"postgresql://{self.username}:{self.password}@{self.hostname}:{self.port}/{self.database}",
                pool_pre_ping=True,
            )
            logger.info("Sqlalchemy engine created")
            return engine
        except Exception as exc:
            logger.error(f"Exc: {exc} while trying to create sqlalchemy engine")
            raise exc

    def load_to_postgres(self, table_name: str, dataframe: pd.DataFrame) -> (int, str):
        """Load a dataframe to a Postgres table.

        Args:
            table_name (str): The name of the table.
            dataframe (pd.DataFrame): The dataframe to load.

        Returns:
            (int, str): The number of rows loaded and any errors.
        """
        stg_exc = None
        count = 0
        try:
            engine = self.create_sqlalchemy_engine()
            insp = inspect(engine)
            if not insp.has_table(f"{table_name}", schema="stg"):
                raise Exception(f"Table {table_name} not present in Postgres")
            logger.info(f"Insertion into stg layer for {table_name} started")
            count = dataframe.to_sql(
                table_name,
                engine,
                schema="stg",
                if_exists="append",
                index=False,
            )
            if not count:
                count = 0
            logger.info(f"{count} rows inserted into stg layer for {table_name}")
        except sqla_exc.DBAPIError as excep:
            logger.error(f"DBInterface error while trying to insert into stg layer")
            logger.error(excep)
            stg_exc = str(excep)[:150] + " in load_to_stg()"
        except Exception as exc:
            logger.error(f"Exc: {exc} while trying to insert into stg layer")
            stg_exc = str(exc)[:150] + " in load_to_stg()"

        return count, stg_exc

    def check_float_columns(self, dataframe: pd.DataFrame, incompatible_dict: dict) -> dict:
        """Check if float columns are actually integers.

        Args:
            dataframe (pd.DataFrame): The dataframe to check.
            incompatible_dict (dict): The dictionary of incompatible columns.

        Returns:
            dict: The dictionary of incompatible columns.
        """
        temp_dict = {}
        for col, col_details in incompatible_dict.items():
            # if inferred schema is float64, but actual column may be integer
            if col_details.get("file_dtype") == "float64":
                if (dataframe[f"{col}"].fillna(-99999) % 1 == 0).all():
                    continue
            # if inferred schema is int64, but actual column may be int32
            if col_details.get("file_dtype") == "int64":
                if (dataframe[f"{col}"].apply(lambda x: x >= -2147483648 and x <= 2147483647)).all():
                    continue
            if col_details.get("file_dtype") == "object":
                # if inferred schema is varchar, but actual column may be integer
                if dataframe[f"{col}"].str.isdigit().all():
                    dataframe[f"{col}"] = dataframe[f"{col}"].astype("float").fillna(-1).astype("int")
                    continue
                # if inferred schema is varchar, but actual column may be float
                elif dataframe[col].str.replace(".", "", 1).str.isdecimal().all():
                    logger.info("All are decimal point numbers")
                    dataframe[f"{col}"] = dataframe[f"{col}"].astype("float").fillna(-1)
                    continue
            temp_dict[f"{col}"] = col_details

        return temp_dict

    def check_schema_compatability(
        self,
        dataframe: pd.DataFrame,
        table_name: str,
        postgres_schema: str = "stg",
    ) -> bool:
        """Check if the schema of a dataframe is compatible with a Postgres table.

        Args:
            dataframe (pd.DataFrame): The dataframe to check.
            table_name (str): The name of the table.
            postgres_schema (str, optional): The schema of the table. Defaults to "stg".

        Returns:
            bool: True if the schema is compatible, False otherwise.
        """
        logger.info(f"Schema compatability check for evolution started")
        table_schema = self.get_table_schema(table_name, postgres_schema)
        file_schema = dataframe.dtypes.astype(str).to_dict()
        logger.info(str(file_schema))
        incompatible_dict = {}
        compatablity_status = True

        for table_col, table_details in table_schema.items():
            table_col_dtype, table_col_nullability, table_col_length = (
                table_details[0],
                table_details[1],
                table_details[2],
            )
            if not file_schema.get(f"{table_col}", None):
                # column removal
                # check nullabality
                compatablity_status = False
                if table_col_dtype == "character varying" and table_col_nullability == "NO":
                    logger.info(f"{table_col} is_nullable: {table_col_nullability}")
                    incompatible_dict[f"{table_col}"] = {
                        "table_col_dtype": table_col_dtype,
                        "file_dtype": file_schema.get(f"{table_col}"),
                        "file_length": None,
                        "nullable": table_col_nullability,
                    }
                continue

            corresponding_dtype = postgres_to_pandas_datatypes[table_col_dtype]
            if file_schema.get(f"{table_col}") in pandas_compatible_datatypes[f"{corresponding_dtype}"]:
                if "char" not in table_col_dtype:
                    continue
                incoming_length = dataframe[f"{table_col}"].str.len().max()
                if table_col_length >= incoming_length:
                    continue
                else:
                    compatablity_status = False
                    incompatible_dict[f"{table_col}"] = {
                        "table_col_dtype": table_col_dtype,
                        "file_dtype": file_schema.get(f"{table_col}"),
                        "file_length": incoming_length,
                    }
            else:
                compatablity_status = False
                incompatible_dict[f"{table_col}"] = {
                    "table_col_dtype": table_col_dtype,
                    "file_dtype": file_schema.get(f"{table_col}"),
                    "file_length": None,
                }
        logger.info(f"Schema compatability check completed")

        if len(incompatible_dict) != 0:
            incompatible_dict = self.check_float_columns(dataframe, incompatible_dict)
        if len(incompatible_dict) == 0:
            return True, {}

        return compatablity_status, incompatible_dict

    def perform_schema_evolution(self, transformed_df: pd.DataFrame, table_name: str, postgres_schema: str = "stg"):
        """Perform schema evolution on a dataframe.

        Args:
            transformed_df (pd.DataFrame): The dataframe to evolve.
            table_name (str): The name of the table.
            postgres_schema (str, optional): The schema of the table. Defaults to "stg".

        Returns:
            pd.DataFrame: The evolved dataframe.
        """
        # schema evolution
        (
            compatability_status,
            incompatability_dict,
        ) = self.check_schema_compatability(transformed_df, table_name, postgres_schema)
        if not compatability_status:
            # TODO send notification and make sense of incompatability_dict
            logger.info(f"The file schema is not compatible with the existing Postgres table schema")
            logger.info(f"{incompatability_dict}")
            return pd.DataFrame(), incompatability_dict

        logger.info(f"The file schema is compatible with the Postgres table schema, hence proceeding forward")
        evolved_df, evol_exc = self.update_table_schema(table_name, transformed_df, postgres_schema)
        return evolved_df, evol_exc

    def process(self, filename="employee.csv"):
        """Process a file and perform schema evolution.

        Args:
            filename (str, optional): The name of the file. Defaults to "employee.csv".
        """
        bucket_name = "dev-s3-bucket"
        table_name = "employee"
        dataframe = pd.read_csv(f"s3://{bucket_name}/raw/employee/{filename}")

        evolved_df, evol_exc = self.perform_schema_evolution(dataframe, table_name)
        if not evol_exc:
            stg_rows_count, stg_exc = self.load_to_postgres(table_name, evolved_df)
        else:
            raise Exception(evol_exc)


if __name__ == "__main__":
    schema_evol_obj = SchemaEvolution()
    schema_evol_obj.process()
