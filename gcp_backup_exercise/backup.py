"""Given a `table_to_backup` from sub msg, backup table to Cloud Storage."""
from datetime import datetime, timedelta

from google.cloud import pubsub_v1
from google.cloud import bigquery


def run_backup_callback(message):
    """Given a `table_to_backup` from sub msg, run backup process.

    Backup process would be a BigQuery extact_table() to Cloud Storage.

    This could be optimized based on the size of the dataset:
        For example, maybe it could run faster to partition and back up
        only based on the delta changes in the last 24 hours.

    Args:
        message: Pub/Sub payload
    """
    project = message.data.table_to_backup.project
    dataset_id = message.data.table_to_backup.dataset_id
    table_id = message.data.table_to_backup.table_id

    client = bigquery.Client(project=project)
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    backup_job = client.extract_table(
        table_ref,
        f"gs://my-organization-bucket-name/{project}/backup/{dataset_id}/{table_id}/*.csv"
    )

    backup_job.result()

    message.ack()


def process_backup_sub_msg():
    """Process pub/sub message to perform table backup to Cloud Storage."""
    with pubsub_v1.SubscriberClient() as subscriber:
        topic_path = subscriber.topic_path(
            "gcp-platform-team-project",
            "run-bq-backup-to-cs"
        )

        subscription_path = subscriber.subscription_path(
            "gcp-platform-team-project",
            "run-bq-backup-to-cs-sub"
        )

        subscriber.create_subscription(subscription_path, topic=topic_path)
        future = subscriber.subscribe(subscription_path, run_backup_callback)


if __name__ == "__main__":
    process_backup_sub_msg()
