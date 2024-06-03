import typing as t
from enum import Enum
from pathlib import Path

import click
from sqlglot import Dialect


class ProjectTemplate(Enum):
    AIRFLOW = "airflow"
    DBT = "dbt"
    DEFAULT = "default"
    EMPTY = "empty"


class DialectConnectionTemplate(Enum):
    BIGQUERY_CHOOSE = "bigquery"
    BIGQUERY_OAUTH = "bigquery_oauth"
    BIGQUERY_OAUTH_SECRETS = "bigquery_oauth_secrets"
    BIGQUERY_SERVICE_ACCOUNT_KEYFILE = "bigquery_service_account_keyfile"
    BIGQUERY_SERVICE_ACCOUNT_JSON = "bigquery_service_account_json"
    DATABRICKS = "databricks"
    DUCKDB = "duckdb"
    MOTHERDUCK = "motherduck"
    MYSQL = "mysql"
    MSSQL = "mssql"
    POSTGRES = "postgres"
    REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"
    SPARK = "spark"
    TRINO = "trino"


def _gen_config(dialect: t.Optional[str], template: ProjectTemplate) -> str:
    if dialect == "bigquery":
        # TODO: consider prompting user (in a wizard-style) for which auth method to use
        #       "The bigquery dialect supports multiple connection methods, please choose one of the following: "
        #
        #dialect_template = DialectConnectionTemplate.BIGQUERY_OAUTH_SECRETS
        #dialect_template = DialectConnectionTemplate.BIGQUERY_SERVICE_ACCOUNT_KEYFILE
        #dialect_template = DialectConnectionTemplate.BIGQUERY_SERVICE_ACCOUNT_JSON
        #dialect_template = DialectConnectionTemplate.BIGQUERY_OAUTH
        #
        # NOTE: for now, simplifing to just put all options in the config and let the user choose by uncommenting what they want
        dialect_template = DialectConnectionTemplate.BIGQUERY_CHOOSE
    else:
        dialect_template = DialectConnectionTemplate(dialect)

    dialect_connection_templates = {
      DialectConnectionTemplate.BIGQUERY_CHOOSE: """
      # Please uncomment and configure one of the following four methods
      #
      # Method 1
      #
      #type: bigquery
      #method: oauth
      ##scopes: <The scopes used to obtain authorization, specified as a list>
      #
      # Method 2
      #
      #type: bigquery
      #method: oauth-secrets
      ##token: <Can be None if refresh information is provided.>
      ##refresh_token: <If specified, credentials can be refreshed.>
      ##client_id: <Must be specified for refresh, can be left as None if the token can not be refreshed.>
      ##client_secret: <Must be specified for refresh, can be left as None if the token can not be refreshed.>
      ##token_uri: <Must be specified for refresh, can be left as None if the token can not be refreshed.>
      ##scopes: <OAuth 2.0 credentials can not request additional scopes after authorization. The scopes must be derivable from the refresh token if refresh information is provided (e.g. The refresh token scopes are a superset of this or contain a wild card scope like 'https://www.googleapis.com/auth/any-api')>
      #
      # Method 3
      #
      #type: bigquery
      #method: service-account
      #keyfile: <Path to the keyfile>
      ##scopes: <The scopes used to obtain authorization, specified as a list>
      #
      # Method 4
      #
      #type: bigquery
      #method: service-account-json
      #keyfile_json: <Keyfile information provided inline (not recommended)>
      ##scopes: <The scopes used to obtain authorization, specified as a list>
      """,
      DialectConnectionTemplate.BIGQUERY_OAUTH: """
      type: bigquery
      method: oauth
      #scopes: <The scopes used to obtain authorization, specified as a list>
      """,
      DialectConnectionTemplate.BIGQUERY_OAUTH_SECRETS: """
      type: bigquery
      method: oauth-secrets
      #token: <Can be None if refresh information is provided.>
      #refresh_token: <If specified, credentials can be refreshed.>
      #client_id: <Must be specified for refresh, can be left as None if the token can not be refreshed.>
      #client_secret: <Must be specified for refresh, can be left as None if the token can not be refreshed.>
      #token_uri: <Must be specified for refresh, can be left as None if the token can not be refreshed.>
      #scopes: <OAuth 2.0 credentials can not request additional scopes after authorization. The scopes must be derivable from the refresh token if refresh information is provided (e.g. The refresh token scopes are a superset of this or contain a wild card scope like 'https://www.googleapis.com/auth/any-api')>
      """,
      DialectConnectionTemplate.BIGQUERY_SERVICE_ACCOUNT_KEYFILE: """
      type: bigquery
      method: service-account
      keyfile: <Path to the keyfile>
      #scopes: <The scopes used to obtain authorization, specified as a list>
      """,
      DialectConnectionTemplate.BIGQUERY_SERVICE_ACCOUNT_JSON: """
      type: bigquery
      method: service-account-json
      keyfile_json: <Keyfile information provided inline (not recommended)>
      #scopes: <The scopes used to obtain authorization, specified as a list>
      """,
      DialectConnectionTemplate.DATABRICKS: """
      type: databricks
      #server_hostname: <Databricks instance host name>
      #http_path: <HTTP path, either to a DBSQL endpoint (such as /sql/1.0/endpoints/1234567890abcdef) or to an All-Purpose cluster (such as /sql/protocolv1/o/1234567890123456/1234-123456-slid123)>
      #access_token: <HTTP Bearer access token, such as Databricks Personal Access Token>
      #catalog: <Spark 3.4+ Only if not using SQL Connector. The name of the catalog to use for the connection. Defaults to use Databricks cluster default. (string)>
      #http_headers: <SQL Connector Only: An optional dictionary of HTTP headers that will be set on every request>
      #session_configuration: <SQL Connector Only: An optional dictionary of Spark session parameters. Execute the SQL command SET -v to get a full list of available commands.>
      #databricks_connect_server_hostname: <Databricks Connect Only: Databricks Connect server hostname. Uses server_hostname if not set.>
      #databricks_connect_access_token: <Databricks Connect Only: Databricks Connect access token. Uses access_token if not set.>
      #databricks_connect_cluster_id: <Databricks Connect Only: Databricks Connect cluster ID. Uses http_path if not set. Cannot be a Databricks SQL Warehouse.>
      #force_databricks_connect: <When running locally, a bool to force the use of Databricks Connect for all model operations (so don't use SQL Connector for SQL models)>
      #disable_databricks_connect: <When running locally, a bool to disable the use of Databricks Connect for all model operations (so use SQL Connector for all models) (bool)>
      #disable_spark_session: <When running in a notebook, a bool to specify not to use SparkSession if it is available>
      """,
      DialectConnectionTemplate.DUCKDB: """
      type: duckdb
      #database: <The optional database name. If not specified, the in-memory database is used. Cannot be defined if using catalogs.>
      #catalogs: <Mapping to define multiple catalogs. Can attach DuckDB catalogs or catalogs for other connections. First entry is the default catalog. Cannot be defined if using database.>
      #extensions: <Extension to load into duckdb. Only autoloadable extensions are supported. Note: specified as a list>
      #connector_config: <Configuration to pass into the duckdb connector, specified as a dict>
      """,
      DialectConnectionTemplate.MOTHERDUCK: """
      type: motherduck
      database: <The database name.>
      #token: <The optional MotherDuck token. If not specified, the user will be prompted to login with their web browser.>
      #extensions: <Extension to load into duckdb. Only autoloadable extensions are supported. Note: specified as a list>
      #connector_config: <Configuration to pass into the duckdb connector, specified as a dict>
      """,
      DialectConnectionTemplate.MYSQL: """
      type: mysql
      host: <The hostname of the MysQL server>
      user: <The username to use for authentication with the MySQL server>
      password: <The password to use for authentication with the MySQL server>
      #port: <The port number of the MySQL server>
      #charset: <The character set used for the connection>
      #ssl_disabled: <Is SSL disabled>
      """,
      DialectConnectionTemplate.MSSQL: """
      type: mssql
      host: <The hostname of the MSSQL server>
      #user: <The username to use for authentication with the MSSQL server>
      #password: <The password to use for authentication with the MSSQL server>
      #port: <The port number of the MSSQL server>
      #database: <The target database>
      #charset: <The character set used for the connection>
      #timeout: <The query timeout in seconds. Default: no timeout>
      #login_timeout: <The timeout for connection and login in seconds. Default: 60>
      #appname: <The application name to use for the connection>
      #conn_properties: <The list of connection properties>
      #autocommit: <Is autocommit mode enabled. Default: false>
      """,
      DialectConnectionTemplate.POSTGRES: """
      # Please uncomment and configure one of the following, depending on whether using GCP 
      #
      # Postgres standard config
      #
      #type: postgres
      #host: <The hostname of the Postgres server>
      #user: <The username to use for authentication with the Postgres server>
      #password: <The password to use for authentication with the Postgres server>
      #port: <The port number of the Postgres server>
      #database: <The name of the database to connect to>
      ##keepalives_idle: <The number of seconds between each keepalive packet sent to the server.>
      ##connect_timeout: <The number of seconds to wait for the connection to the server. (Default: 10)>
      ##role: <The role to use for authentication with the Postgres server>
      ##sslmode: <The security of the connection to the Postgres server>
      #
      # GCP Postgres config
      #
      #type: postgres
      #instance_connection_str: <Connection name for the postgres instance>
      #user: <The username (posgres or IAM) to use for authentication>
      ##password: <The password to use for authentication. Required when connecting as a Postgres user>
      ##enable_iam_auth: <Enables IAM authentication. Required when connecting as an IAM user>
      #db: <The name of the database instance to connect to>
      """,
      DialectConnectionTemplate.REDSHIFT: """
      type: redshift
      #user: <The username to use for authentication with the Amazon Redshift cluster>
      #password: <The password to use for authentication with the Amazon Redshift cluster>
      #database: <The name of the database instance to connect to>
      #host: <The hostname of the Amazon Redshift cluster>
      #port: 5439
      #ssl: <Is SSL enabled. SSL must be enabled when authenticating using IAM (Default: True)>
      #sslmode: <The security of the connection to the Amazon Redshift cluster. verify-ca and verify-full are supported.>
      #timeout: <The number of seconds before the connection to the server will timeout.>
      #tcp_keepalive: <Is TCP keepalive used. (Default: True)>
      #application_name: <The name of the application>
      #preferred_role: <The IAM role preferred for the current connection>
      #principal_arn: <The ARN of the IAM entity (user or role) for which you are generating a policy>
      #credentials_provider: <The class name of the IdP that will be used for authenticating with the Amazon Redshift cluster>
      #region: <The AWS region of the Amazon Redshift cluster>
      #cluster_identifier: <The cluster identifier of the Amazon Redshift cluster>
      #iam: <If IAM authentication is enabled. IAM must be True when authenticating using an IdP. Note: specified as a dict>
      #is_serverless: <If the Amazon Redshift cluster is serverless (Default: False)>
      #serverless_acct_id: <The account ID of the serverless cluster>
      #serverless_work_group: <The name of work group for serverless end point>
      """,
      DialectConnectionTemplate.SNOWFLAKE: """
      type: snowflake
      #user: <The Snowflake username>
      #password: <The Snowflake password>
      #authenticator: <The Snowflake authenticator method>
      account: <The Snowflake account name>
      #warehouse: <The Snowflake warehouse name>
      #database: <The Snowflake database name>
      #role: <The Snowflake role name>
      #token: <The Snowflake OAuth 2.0 access token>
      #private_key: <The optional private key to use for authentication. Key can be Base64-encoded DER format (representing the key bytes), a plain-text PEM format, or bytes (Python config only).>
      #private_key_path: <The optional path to the private key to use for authentication. This would be used instead of private_key.>
      #private_key_passphrase: <The optional passphrase to use to decrypt private_key (if in PEM format) or private_key_path. Keys can be created without encryption so only provide this if needed.>
      """,
      DialectConnectionTemplate.SPARK: """
      type: spark
      #config_dir: <Value to set for SPARK_CONFIG_DIR>
      #catalog: <The catalog to use when issuing commands. See Catalog Support for details>
      #config: <Key/value pairs to set for the Spark Configuration, specified as a dict.>
      """,
      DialectConnectionTemplate.TRINO: """
      type: trino
      user: <The username (of the account) to log in to your cluster. When connecting to Starburst Galaxy clusters, you must include the role of the user as a suffix to the username.>
      host: <The hostname of your cluster. Don't include the http:// or https:// prefix.>
      catalog: <The name of a catalog in your cluster.>
      http_scheme: <The HTTP scheme to use when connecting to your cluster. By default, it's https and can only be http for no-auth or basic auth.>
      port: <The port to connect to your cluster. By default, it's 443 for https scheme and 80 for http>
      roles: <Mapping of catalog name to a role, specified as a dict>
      http_headers: <Additional HTTP headers to send with each request, specified as a dict.>
      session_properties: <Trino session properties. Run SHOW SESSION to see all options. Note: specified as a dict>
      retries: <Number of retries to attempt when a request fails. Default: 3>
      timezone: <Timezone to use for the connection. Default: client-side local timezone>
      """,
    }

    default_configs = {
        ProjectTemplate.DEFAULT: f"""gateways:
  prod:
    connection:
      {dialect_connection_templates[dialect_template].strip()}
  local:
    connection:
      type: duckdb
      database: db.db

default_gateway: prod

model_defaults:
  dialect: {dialect}
""",
        ProjectTemplate.AIRFLOW: f"""gateways:
  prod:
    connection:
      {dialect_connection_templates[dialect_template].strip()}
  local:
    connection:
      type: duckdb
      database: db.db

default_gateway: local

default_scheduler:
  type: airflow
  airflow_url: http://localhost:8080/
  username: airflow
  password: airflow

model_defaults:
  dialect: {dialect}
""",
        ProjectTemplate.DBT: """from pathlib import Path

from sqlmesh.dbt.loader import sqlmesh_config

config = sqlmesh_config(Path(__file__).parent)
""",
    }

    default_configs[ProjectTemplate.EMPTY] = default_configs[ProjectTemplate.DEFAULT]
    return default_configs[template]


EXAMPLE_SCHEMA_NAME = "sqlmesh_example"
EXAMPLE_FULL_MODEL_NAME = f"{EXAMPLE_SCHEMA_NAME}.full_model"
EXAMPLE_INCREMENTAL_MODEL_NAME = f"{EXAMPLE_SCHEMA_NAME}.incremental_model"
EXAMPLE_SEED_MODEL_NAME = f"{EXAMPLE_SCHEMA_NAME}.seed_model"

EXAMPLE_FULL_MODEL_DEF = f"""MODEL (
  name {EXAMPLE_FULL_MODEL_NAME},
  kind FULL,
  cron '@daily',
  grain item_id,
  audits (assert_positive_order_ids),
);

SELECT
  item_id,
  COUNT(DISTINCT id) AS num_orders,
FROM
  {EXAMPLE_INCREMENTAL_MODEL_NAME}
GROUP BY item_id
"""

EXAMPLE_INCREMENTAL_MODEL_DEF = f"""MODEL (
  name {EXAMPLE_INCREMENTAL_MODEL_NAME},
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date
  ),
  start '2020-01-01',
  cron '@daily',
  grain (id, event_date)
);

SELECT
  id,
  item_id,
  event_date,
FROM
  {EXAMPLE_SEED_MODEL_NAME}
WHERE
  event_date BETWEEN @start_date AND @end_date
"""

EXAMPLE_SEED_MODEL_DEF = f"""MODEL (
  name {EXAMPLE_SEED_MODEL_NAME},
  kind SEED (
    path '../seeds/seed_data.csv'
  ),
  columns (
    id INTEGER,
    item_id INTEGER,
    event_date DATE
  ),
  grain (id, event_date)
);
"""

EXAMPLE_AUDIT = """AUDIT (
  name assert_positive_order_ids,
);

SELECT *
FROM @this_model
WHERE
  item_id < 0
"""

EXAMPLE_SEED_DATA = """id,item_id,event_date
1,2,2020-01-01
2,1,2020-01-01
3,3,2020-01-03
4,1,2020-01-04
5,1,2020-01-05
6,1,2020-01-06
7,1,2020-01-07
"""

EXAMPLE_TEST = f"""test_example_full_model:
  model: {EXAMPLE_FULL_MODEL_NAME}
  inputs:
    {EXAMPLE_INCREMENTAL_MODEL_NAME}:
      rows:
      - id: 1
        item_id: 1
      - id: 2
        item_id: 1
      - id: 3
        item_id: 2
  outputs:
    query:
      rows:
      - item_id: 1
        num_orders: 2
      - item_id: 2
        num_orders: 1
"""


def init_example_project(
    path: t.Union[str, Path],
    dialect: t.Optional[str],
    template: ProjectTemplate = ProjectTemplate.DEFAULT,
) -> None:
    root_path = Path(path)
    config_extension = "py" if template == ProjectTemplate.DBT else "yaml"
    config_path = root_path / f"config.{config_extension}"
    audits_path = root_path / "audits"
    macros_path = root_path / "macros"
    models_path = root_path / "models"
    seeds_path = root_path / "seeds"
    tests_path = root_path / "tests"

    if config_path.exists():
        raise click.ClickException(f"Found an existing config in '{config_path}'")

    if not dialect and template != ProjectTemplate.DBT:
        raise click.ClickException(
            "Default SQL dialect is a required argument for SQLMesh projects"
        )

    _create_config(config_path, dialect, template)
    if template == ProjectTemplate.DBT:
        return

    _create_folders([audits_path, macros_path, models_path, seeds_path, tests_path])

    if template != ProjectTemplate.EMPTY:
        _create_macros(macros_path)
        _create_audits(audits_path)
        _create_models(models_path)
        _create_seeds(seeds_path)
        _create_tests(tests_path)


def _create_folders(target_folders: t.Sequence[Path]) -> None:
    for folder_path in target_folders:
        folder_path.mkdir(exist_ok=True)
        (folder_path / ".gitkeep").touch()


def _create_config(config_path: Path, dialect: t.Optional[str], template: ProjectTemplate) -> None:
    if dialect:
        Dialect.get_or_raise(dialect)

    project_config = _gen_config(dialect, template)

    _write_file(
        config_path,
        project_config,
    )


def _create_macros(macros_path: Path) -> None:
    (macros_path / "__init__.py").touch()


def _create_audits(audits_path: Path) -> None:
    _write_file(audits_path / "assert_positive_order_ids.sql", EXAMPLE_AUDIT)


def _create_models(models_path: Path) -> None:
    for model_name, model_def in [
        (EXAMPLE_FULL_MODEL_NAME, EXAMPLE_FULL_MODEL_DEF),
        (EXAMPLE_INCREMENTAL_MODEL_NAME, EXAMPLE_INCREMENTAL_MODEL_DEF),
        (EXAMPLE_SEED_MODEL_NAME, EXAMPLE_SEED_MODEL_DEF),
    ]:
        _write_file(models_path / f"{model_name.split('.')[-1]}.sql", model_def)


def _create_seeds(seeds_path: Path) -> None:
    _write_file(seeds_path / "seed_data.csv", EXAMPLE_SEED_DATA)


def _create_tests(tests_path: Path) -> None:
    _write_file(tests_path / "test_full_model.yaml", EXAMPLE_TEST)


def _write_file(path: Path, payload: str) -> None:
    with open(path, "w", encoding="utf-8") as fd:
        fd.write(payload)
