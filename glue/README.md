# Schema-Evolution-with_Pandas
# Handling Schema Evolution while loading files from S3 to Postgres using Pandas

# To run:
```
cd glue/tests/

docker-compose up -d
```
# Attach VS Code to the running container glue_pytest
# Run E2E test case
`pytest -o log_cli=TRUE --log-cli-level=INFO tests/e2e/test_schema_evolution.py`
