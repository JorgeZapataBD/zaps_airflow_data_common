import json

from airflow.providers.standard.operators.bash import BashOperator


class DbtBashOperator(BashOperator):
    def __init__(
        self,
        base_command: str,
        dbt_args: dict = None,
        *args,
        **kwargs
    ) -> None:
        self.base_command = base_command
        self.dbt_args = dbt_args or {}
        bash_command = self._build_dbt_command()
        super().__init__(bash_command=bash_command, *args, **kwargs)

    def _build_dbt_command(self):
        command = f'dbt {self.base_command}'
        for key, value in self.dbt_args.items():
            if isinstance(value, dict):
                value = json.dumps(value).replace('"', "'")
                print(value)
            if isinstance(value, list):
                value = " ".join(value)
            command += f' --{key}' if not value else f' --{key} {value}'

        return command
