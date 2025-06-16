from typing import Union

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.api_core.retry import Retry
from google.cloud.firestore import Client


class FirestoreCustomHook(GoogleBaseHook):
    """
    Interact with Google Firestore.

    This hook uses the Google Cloud connection.

    :param gcp_conn_id: The Airflow connection used for GCP credentials.
    :param retry_policy: Retry policy to connect to firestore
    """

    def __init__(
        self,
        gcp_conn_id: str,
        retry_policy: Retry | None = None,
        database: str = '(default)',
        **kwargs,
    ) -> None:
        super().__init__(gcp_conn_id=gcp_conn_id, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.retry_policy = retry_policy
        self.database = database

        # Check project id in connection
        if not self.project_id:
            raise ValueError(
                f'A valid project_id could not be found in the GCP connection "{gcp_conn_id}"')

    def get_client(self) -> Client:
        """
        Get an authenticated Firestore Client.

        :param project_id: Project ID for the project which the client acts on behalf of.
        :param location: Default location for database.
        """
        return Client(
            project=self.project_id,
            credentials=self.get_credentials(),
            database=self.database
        )

    def upsert_document(self, collection_name: str, doc_id: str, data: dict) -> None:
        """
        Adds or updates a document in Firestore.

        :param collection_name: Collection name.
        :param doc_id: Document ID.
        :param data: Data to be stored in the document.
        """
        try:
            doc_ref = self.get_client().collection(collection_name).document(doc_id)
            doc_ref.set(data, retry=self.retry_policy)
            self.log.info(
                f'Document "{doc_id}" added/updated in collection "{collection_name}"')
        except Exception as e:
            self.log.error(
                f'Error adding/updating document ({doc_id}) in collection ({collection_name}): {e}')
            raise

    def get_document(self, collection_name: str, doc_id: str) -> Union[dict, None]:
        """
        Retrieves a document from Firestore.

        :param collection_name: Collection name.
        :param doc_id: Document ID.
        :return: The document data as a dictionary, or None if the document does not exist.
        """
        try:
            doc_ref = self.get_client().collection(collection_name).document(doc_id)
            doc = doc_ref.get(retry=self.retry_policy)
            if doc.exists:
                return doc.to_dict()
            self.log.info(
                f'Document "{doc_id}" does not exist in collection "{collection_name}"')
            return None
        except Exception as e:
            self.log.error(
                f'Error retrieving document ({doc_id}) in collection ({collection_name}): {e}')
            raise

    def delete_document(self, collection_name: str, doc_id: str) -> None:
        """
        Deletes a document from Firestore.

        :param collection_name: Collection name.
        :param doc_id: Document ID.
        """
        try:
            doc_ref = self.get_client().collection(collection_name).document(doc_id)
            doc_ref.delete()
            self.log.info(
                f'Document "{doc_id}" deleted from collection "{collection_name}"')
        except Exception as e:
            self.log.error(
                f'Error deleting document ({doc_id}) in collection ({collection_name}): {e}')
            raise
