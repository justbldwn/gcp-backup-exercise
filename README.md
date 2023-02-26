# gcp-backup-exercise

## Summary
A workflow to process data backups on Google Cloud Platform (GCP). This includes the architecture and pseudo-code required to orchestrate, execute, and log the process.

## Workflow Diagram
![diagram](docs/gcp-backup-diagram.png)

## GCP Components Used

### Google Cloud Scheduler
Used to manage the automated scheduling of the backup workflow.

### Google Pub/Sub
Used as an event messaging queue to help kick-off various Google Cloud Functions scripts throughout the workflow.

### Google Cloud Functions
Used to execute Python scripts at various points throughout the workflow. Pseudocode of the example Python scripts are included in the `gcp_backup_exercise` directory.

### Google Cloud Logging
Used to centralize successful and failed logs for each step of the workflow.
