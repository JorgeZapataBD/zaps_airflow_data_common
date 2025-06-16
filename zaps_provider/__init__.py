__version__ = "1.0.0"

# This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-zapsthne i have to",
        "name": "Zaps",
        "description": "Custom Airflow provider with Zaps-specific integrations",
        "versions": [__version__],
        "hooks": [
            {
                "integration-name": "api_custom",
                "python-modules": ["airflow_provider_zaps.hooks.api_hook"]
            },
            {
                "integration-name": "bigquery_custom",
                "python-modules": ["airflow_provider_zaps.hooks.bigquery_hook"]
            },
            {
                "integration-name": "firestore_custom",
                "python-modules": ["airflow_provider_zaps.hooks.firestore_hook"]
            }
        ],
        "operators": [
            {
                "integration-name": "dbt_custom",
                "python-modules": ["airflow_provider_zaps.operators.dbt_custom"]
            }
        ],
        "sensors": [],
        "executors": [],
        "macros": [],
    }
