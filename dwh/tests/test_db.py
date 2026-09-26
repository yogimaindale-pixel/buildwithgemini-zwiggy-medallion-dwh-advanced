from zwiggy_dwh.config import DbTarget
from zwiggy_dwh.db import check_connection, DatabaseConnectionError


def test_dbtarget_string_formatting():
    target = DbTarget("localhost", 5432, "zwiggy_db", "admin", "secret_pass")
    assert "secret_pass" not in str(target)
    assert "***" in str(target)


def test_check_connection_invalid_host():
    invalid_target = DbTarget("invalid_host_xyz", 5432, "invalid_db", "user", "pass")
    assert check_connection(invalid_target) is False
