import unittest
import os
from treeno.builder.convert import query_from_sql

RESOURCES_DIR = os.path.join(os.path.dirname(__file__), "resources")
RESOURCES_PATH = os.path.join(RESOURCES_DIR, "statements.sql")
GENERATED_RESOURCES_PATH = os.path.join(
    RESOURCES_DIR, "generated_statements.sql"
)

with open(RESOURCES_PATH) as f:
    # TODO: We assume there's no ; character used elsewhere in the SQL statement
    SQL_STATEMENTS = f.read().split(";\n")


class IntegrationSQL(unittest.TestCase):
    def test_integration(self):
        with open(GENERATED_RESOURCES_PATH) as f:
            generated = f.read().split(";\n")

        for sql_statement, generated_sql_statement in zip(
            SQL_STATEMENTS, generated
        ):
            print(sql_statement)
            print(generated_sql_statement)
            if not sql_statement:
                continue
            result_sql = query_from_sql(sql_statement).sql()
            assert result_sql == generated_sql_statement
            # Make sure the result sql can be correctly parsed and transformed as well
            assert query_from_sql(result_sql).sql() == generated_sql_statement


def update_generated_resources():
    generated = ""
    for sql_statement in SQL_STATEMENTS:
        if not sql_statement:
            continue
        print(generated)
        generated += query_from_sql(sql_statement).sql() + ";\n"
    with open(GENERATED_RESOURCES_PATH, "w") as f:
        f.write(generated)
        f.flush()


if __name__ == "__main__":
    if os.environ["UPDATE"]:
        update_generated_resources()
    else:
        unittest.main()
