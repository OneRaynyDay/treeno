import unittest
from treeno.printer import StatementPrinter, JoinPrinter
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

    def test_join_printer(self):
        printer = JoinPrinter(delimiter=",", max_length=10)
        printer.add_entry("123")
        printer.add_entry("456")
        printer.add_entry("789")
        assert (
            printer.to_string(PrintOptions(mode=PrintMode.PRETTY))
            == "123,456,\n789"
        )
        assert (
            printer.to_string(PrintOptions(mode=PrintMode.DEFAULT))
            == "123,456,789"
        )

        printer = JoinPrinter(delimiter=",", max_length=10)
        printer.add_entry("1234567890")
        assert (
            printer.to_string(PrintOptions(mode=PrintMode.PRETTY))
            == printer.to_string(PrintOptions(mode=PrintMode.DEFAULT))
            == "1234567890"
        )

        printer = JoinPrinter(delimiter=",", max_length=10)
        printer.add_entry("12")
        printer.add_entry("34")
        printer.add_entry("56")
        printer.add_entry("67")
        assert (
            printer.to_string(PrintOptions(mode=PrintMode.PRETTY, spaces=4))
            == "12,34,\n56,67"
        )
        assert (
            printer.to_string(PrintOptions(mode=PrintMode.PRETTY, spaces=5))
            == "12,\n34,\n56,67"
        )

        printer = JoinPrinter(delimiter=",", max_length=10)
        printer.add_entry("12345678901")
        assert (
            printer.to_string(PrintOptions(mode=PrintMode.PRETTY))
            == "12345678901"
        )


if __name__ == "__main__":
    unittest.main()
