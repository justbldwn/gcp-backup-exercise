"""Given a `project_to_check` from sub msg, see if any tables need to be backed up."""
from datetime import datetime, timedelta

from google.cloud import pubsub_v1
from google.cloud import bigquery


def check_backup_callback(message):
    """Given a `project_to_check` from sub msg, see if any tables need to be backed up.

    If so, send a message to the `run-bq-backup-to-cs` topic.

    Args:
        message (_type_): _description_
    """
    last_24h_datetime = datetime.now() - timedelta(days=1)
    project_to_check = message.data.project_to_check

    client = bigquery.Client(project=project_to_check)
    dataset_list = list(client.list_datasets())

    for dataset in dataset_list:
        tables = client.list_tables(dataset.dataset_id)

        for table_obj in tables:
            table = client.get_table(table_obj.reference)

            # if the table's last modified time is within last 24 hours,
            # send a request to the backup pub-sub topic to run backup script
            if table.modified <= last_24h_datetime:
                publisher = pubsub_v1.PublisherClient()
                topic_path = publisher.topic_path(
                    "gcp-platform-team-project",
                    "run-bq-backup-to-cs"
                )

                publisher.create_topic(name=topic_path)
                publisher.publish(
                    topic_path,
                    b"Table to backup found.",
                    table_to_backup=table
                )

    message.ack()


def process_sub_msg():
    """Process pub/sub message to see if a table needs to be backed up."""
    with pubsub_v1.SubscriberClient() as subscriber:
        topic_path = subscriber.topic_path(
            "gcp-platform-team-project",
            "check-project-tables"
        )

        subscription_path = subscriber.subscription_path(
            "gcp-platform-team-project",
            "check-project_tables-sub"
        )

        subscriber.create_subscription(subscription_path, topic=topic_path)
        subscriber.subscribe(subscription_path, check_backup_callback)


if __name__ == "__main__":
    process_sub_msg()
