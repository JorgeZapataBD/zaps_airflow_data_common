import pytest

from zaps_provider.operators.dbt_custom import DbtBashOperator


@pytest.mark.parametrize(
    "base_command, dbt_args, expected_command",
    [
        ("run", None, "dbt run"),
        ("test", {"select": "model_a"}, "dbt test --select model_a"),
        ("compile", {"models": ["model1", "model2"]},
         "dbt compile --models model1 model2"),
        ("seed", {"full_refresh": None}, "dbt seed --full_refresh"),
    ]
)
def test_build_dbt_command(base_command, dbt_args, expected_command):
    op = DbtBashOperator(
        task_id='test',
        base_command=base_command,
        dbt_args=dbt_args)
    # Access the private method (or public attribute) that builds the command
    command = op._build_dbt_command()
    assert command == expected_command
