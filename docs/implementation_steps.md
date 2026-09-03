# Implementation steps
1. Clone the repository and run `bash scripts/run_tests.sh`.
2. Set `AQ_BUCKET`, `AQ_STREAM`, and `AWS_REGION`; run `scripts/bootstrap_resources.sh`.
3. Create IAM runtime roles using least privilege based on `iam/policies/aq_data_engineer_policy.json`.
4. Upload job scripts with `scripts/upload_jobs.sh`.
5. Deploy `poll_air_quality` Lambda and schedule it every 10-15 minutes.
6. Create a Kinesis event-source mapping for `kinesis_to_s3` Lambda.
7. Create the Glue database and Glue job pointing to `glue/silver_etl.py`.
8. Create an EMR application/job role and point to `emr/gold_aggregates.py`.
9. Run `redshift/sql/01_ddl.sql`, then configure the Redshift loader Lambda.
10. Replace Step Functions `${...}` placeholders and create the state machine.
11. Run the state machine manually until Bronze -> Silver -> Gold -> Redshift succeeds.
12. Upload the MWAA DAG and set `AQ_STATE_MACHINE_ARN`.
13. Add CloudWatch alarms and verify one controlled failure/retry.
14. Configure Glue Catalog/Athena and Lake Formation grants.
15. Benchmark a full scan vs incremental partitioned run and add only measured metrics to your resume.
