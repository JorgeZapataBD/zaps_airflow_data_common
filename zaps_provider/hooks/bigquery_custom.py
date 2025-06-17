
import io
from typing import IO, Any, Dict, Iterable, Union

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.cloud.bigquery import (Client, LoadJobConfig, SourceFormat,
                                   TimePartitioning, WriteDisposition)
from google.cloud.bigquery.dataset import DatasetReference
from google.cloud.bigquery.job import LoadJob
from google.cloud.bigquery.table import Table, TableReference
from google.cloud.exceptions import NotFound
from pandas import DataFrame


class BigQueryCustomHook(GoogleBaseHook):
    """
    Interact with BigQuery.

    This hook uses the Google Cloud connection.

    :param gcp_conn_id: The Airflow connection used for GCP credentials.
    :param location: The location of the BigQuery resource.
    """

    def __init__(
        self,
        gcp_conn_id: str,
        location: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(gcp_conn_id=gcp_conn_id, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.location = self._get_field('location', location)

        # Check project id in connection
        if not self.project_id:
            raise ValueError(
                f'A valid project_id could not be found in the GCP connection "{self.gcp_conn_id}"')

    def get_client(self, location: str | None = None) -> Client:
        """
        Get an authenticated BigQuery Client.

        :param project_id: Project ID for the project which the client acts on behalf of.
        :param location: Default location for jobs / datasets / tables.
        """
        return Client(
            project=self.project_id,
            location=location or self.location,
            credentials=self.get_credentials(),
        )

    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """
        Check if a table exists in Google BigQuery.

        :param bq_dataset_id: The name of the dataset in which to look for the
            table.
        :param bq_table_id: The name of the table to check the existence of.
        """
        table_reference = TableReference(
            DatasetReference(self.project_id, dataset_id), table_id)
        try:
            self.get_client().get_table(table_reference)
            self.log.info(f'Table {dataset_id}.{table_id} exist')
            return True
        except NotFound:
            self.log.warning(f'Table {dataset_id}.{table_id} NOT exist')
            return False

    def table_partition_exists(
        self, bq_dataset_id: str, bq_table_id: str, bq_partition_id: str
    ) -> bool:
        """
        Check if a partition exists in Google BigQuery.

        :param bq_dataset_id: The name of the dataset in which to look for the
            table.
        :param bq_table_id: The name of the table to check the existence of.
        :param bq_partition_id: The name of the partition to check the existence of.
        """
        table_reference = TableReference(DatasetReference(
            self.project_id, bq_dataset_id), bq_table_id)
        try:
            return bq_partition_id in self.get_client().list_partitions(table_reference)
        except NotFound:
            return False

    def create_empty_table(
        self,
        dataset_id: str,
        table_id: str,
        schema_fields: list | None = None,
        time_partitioning: TimePartitioning | None = None,
        clustering_fields: list[str] | None = None
    ) -> Table:
        """
        Creates a new table in the dataset.

        :param dataset_id: Target dataset in BigQuery.
        :param table_id: Target table in BigQuery.
        :param description: Description of the table in BigQuery.
        :param schema_fields: List of schema fields.
            https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema
        :param time_partitioning: Configures optional time partitioning fields, i.e.,
            partition by field, type, and expiration according to API specifications.
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#timePartitioning
        :param clustering_fields: [Optional] Fields used for clustering.
            BigQuery supports clustering for both partitioned and non-partitioned tables.
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#clustering.fields

        :return: BigQuery Table Object
        """
        # Create table object
        table_reference = TableReference(
            DatasetReference(self.project_id, dataset_id), table_id)
        table = Table(
            table_reference,
            schema=schema_fields
        )
        # Add table properties if defined
        if time_partitioning:
            table.time_partitioning = time_partitioning
        if clustering_fields:
            table.clustering_fields = clustering_fields
        try:
            self.get_client().create_table(table, exists_ok=True)
            self.log.info(
                f'BigQuery table {table_id} successfully created or already exists')
            return table
        except Exception as e:
            self.log.error(f'Uncontrolled Error: {e}')
            raise

    def load_batch_job(
            self,
            dataset_id: str,
            table_id: str,
            data: Union[Iterable[Dict[str, Any]], IO[bytes], DataFrame],
            schema_fields: list | None = None,
            source_format: SourceFormat = SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition: WriteDisposition = WriteDisposition.WRITE_APPEND,
            **kwargs
    ) -> LoadJob:
        """
        This function executes a batch job against Google BigQuery. Depending on the input parameters,
        it loads data from different sources, such as a file, a DataFrame, or a JSON object.
        The function is designed to handle the various data formats dynamically,
        ensuring that the correct loading mechanism is applied based on the type of input provided.

        :param dataset_id: Destination BigQuery Dataset.
        :param table_id: Destination BigQuery Table.
        :param data: Data to be ingested into the target table
        :param schema_fields: If set, the schema field list as defined here:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema
        :param source_format: Optional. The format of the data files.
            For CSV files, specify "CSV".
            For newline-delimited JSON, specify "NEWLINE_DELIMITED_JSON".
        :param write_disposition: Optional. Specifies the action that occurs if the destination table already exists. The following values are supported:
            WRITE_TRUNCATE: If the table already exists, BigQuery overwrites the data, removes the constraints, and uses the schema from the query result.
            WRITE_APPEND: If the table already exists, BigQuery appends the data to the table.
            WRITE_EMPTY: If the table already exists and contains data, a 'duplicate' error is returned in the job result.
            The default value is WRITE_EMPTY. Each action is atomic and only occurs if BigQuery is able to complete the job successfully.
        :return: BigQuery LoadJob Object
        """
        try:
            # Define Job Configuration and Destination BigQuery Table
            job_config = LoadJobConfig(
                schema=schema_fields,
                source_format=source_format,
                write_disposition=write_disposition,
                **kwargs
            )
            table_reference = TableReference(DatasetReference(
                self.project_id, dataset_id), table_id)
        except Exception as e:
            raise e
        print(type(data))
        try:
            if isinstance(data, DataFrame):
                load_job = self.get_client().load_table_from_dataframe(
                    data,
                    table_reference,
                    job_config=job_config
                )
            elif isinstance(data, io.BytesIO):
                load_job = self.get_client().load_table_from_file(
                    data,
                    table_reference,
                    job_config=job_config
                )
            else:
                load_job = self.get_client().load_table_from_json(
                    data,
                    table_reference,
                    job_config=job_config
                )
            # Waiting for Job Result
            load_job.result()
            self.log.info(
                f'Load Batch Job Executed Correctly: {load_job.output_rows} Rows')
            return load_job
        except Exception as e:
            raise e
