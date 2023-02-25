"""
The purpose of this script is to scan as GCP organization to
find all projects within the orgnization.

From there, send a message to a topic ("check-project-tables") in a general
project ("gcp-platform-team-project") to run a script to check what
tables within the project passed to the topic are eligible to be backed up.
"""
from google.cloud import resourcemanager_v3
from google.cloud import pubsub_v1


def main():
    list_of_projects = get_project_list()
    publish_project(list_of_projects)


def get_project_list():
    """Get list of projects across GCP organization.

    Returns:
        ListProjectsPager: A page of the response received from the ListProjects method.
    """
    rm_client = resourcemanager_v3.ProjectsClient()

    request = resourcemanager_v3.ListProjectsRequest(
        parent="my_organization_name",
    )

    list_of_projects = rm_client.list_projects(request=request)

    return list_of_projects


def publish_project(list_of_projects):
    """Publish event to check-project-tables topic.

    `check-project-tables` topic is responsible for checking what
    tables in a given project were last modified within 24h.

    That topic will take in a metadata field `project_to_check` which
    is a project_id that will scan through the tables to check their
    last modified time.

    Args:
        list_of_projects (ListProjectsPager): A page of the response received from the ListProjects method.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        "gcp-platform-team-project",
        "check-project-tables"
    )

    for project in list_of_projects:
        publisher.create_topic(name=topic_path)

        future = publisher.publish(
            topic_path,
            b"Project found, check tables if they should be backed up.",
            project_to_check=project.project_id
        )

        future.result()


if __name__ == "__main__":
    main()
