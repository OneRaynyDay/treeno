from tests.convert.helpers import VisitorTest, get_parser
from treeno.expression import Field
from treeno.functions.string import (
    Chr,
    CodePoint,
    HammingDistance,
    Length,
    LevenshteinDistance,
    Lower,
    LPad,
    LTrim,
    NormalForm,
    Normalize,
    Replace,
    Reverse,
    RPad,
    RTrim,
    Soundex,
)
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser


class TestFunction(VisitorTest):
    def test_normalize(self):
        ast = get_parser("NORMALIZE(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.NormalizeContext)
        self.visitor.visit(ast).assert_equals(Normalize(string=Field("x")))

        ast = get_parser("NORMALIZE(x, NFD)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.NormalizeContext)
        self.visitor.visit(ast).assert_equals(
            Normalize(string=Field("x"), normal_form=NormalForm.NFD)
        )

        ast = get_parser("NORMALIZE(x, NFKC)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.NormalizeContext)
        self.visitor.visit(ast).assert_equals(
            Normalize(string=Field("x"), normal_form=NormalForm.NFKC)
        )

    def test_replace(self):
        ast = get_parser("REPLACE(x, y)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            Replace(string=Field("x"), search=Field("y"))
        )

        ast = get_parser("REPLACE(x, y, z)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            Replace(string=Field("x"), search=Field("y"), replace=Field("z"))
        )

    def test_functions(self):
        ast = get_parser("CHR(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Chr(number=Field("x")))

        ast = get_parser("CODEPOINT(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(CodePoint(string=Field("x")))

        ast = get_parser("HAMMING_DISTANCE(x, y)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            HammingDistance(string1=Field("x"), string2=Field("y"))
        )

        ast = get_parser("LENGTH(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Length(string=Field("x")))

        ast = get_parser("LEVENSHTEIN_DISTANCE(x, y)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            LevenshteinDistance(string1=Field("x"), string2=Field("y"))
        )

        ast = get_parser("LOWER(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Lower(string=Field("x")))

        ast = get_parser("LPAD(x, y, z)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            LPad(string=Field("x"), size=Field("y"), pad_string=Field("z"))
        )

        ast = get_parser("LTRIM(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(LTrim(string=Field("x")))

        ast = get_parser("REVERSE(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Reverse(string=Field("x")))

        ast = get_parser("RPAD(x, y, z)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            RPad(string=Field("x"), size=Field("y"), pad_string=Field("z"))
        )

        ast = get_parser("RTRIM(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(RTrim(string=Field("x")))

        ast = get_parser("SOUNDEX(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Soundex(string=Field("x")))
