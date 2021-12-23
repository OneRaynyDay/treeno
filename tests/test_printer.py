import unittest
from treeno.printer import StatementPrinter
from treeno.base import PrintOptions, PrintMode


class TestPrinter(unittest.TestCase):
    def test_statement_printer(self):
        printer = StatementPrinter()
        printer.add_entry("SELECT", "1,2,3")
        printer.add_entry("FROM", "t")
        printer.update({"WHERE": "x>y", "ORDER": "BY x", "LIMIT": "5"})
        assert printer.to_string(PrintOptions(mode=PrintMode.PRETTY)) == (
            "SELECT 1,2,3\n"
            "  FROM t\n"
            " WHERE x>y\n"
            " ORDER BY x\n"
            " LIMIT 5"
        )
        assert (
            printer.to_string(PrintOptions(mode=PrintMode.DEFAULT))
            == "SELECT 1,2,3 FROM t WHERE x>y ORDER BY x LIMIT 5"
        )


if __name__ == "__main__":
    unittest.main()
