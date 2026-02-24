"""
Converts YAML quality expectations into DLT Expectations dicts.

DLT uses these decorators:
  @dlt.expect_all({"name": "constraint", ...})           -> warn (log but keep)
  @dlt.expect_all_or_drop({"name": "constraint", ...})   -> drop bad rows
  @dlt.expect_all_or_fail({"name": "constraint", ...})   -> fail pipeline
"""
from .config_reader import TableConfig


def apply_expectations(table_config: TableConfig) -> dict:
    """
    Group expectations by action type for use with DLT decorators.

    Returns:
        {
            "warn": {"expectation_name": "SQL constraint", ...},
            "drop": {"expectation_name": "SQL constraint", ...},
            "fail": {"expectation_name": "SQL constraint", ...}
        }
    """
    grouped = {"warn": {}, "drop": {}, "fail": {}}
    for exp in table_config.expectations:
        grouped[exp.action][exp.name] = exp.constraint
    return grouped


def apply_expectations_from_raw(expectations_list: list) -> dict:
    """
    Same as apply_expectations but works with raw dicts (for Gold models).

    Args:
        expectations_list: List of dicts with name, constraint, action keys
    """
    grouped = {"warn": {}, "drop": {}, "fail": {}}
    for exp in expectations_list:
        grouped[exp["action"]][exp["name"]] = exp["constraint"]
    return grouped
