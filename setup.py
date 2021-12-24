import os
import subprocess
from setuptools import setup
from setuptools.command.develop import develop


class AntlrDevelopCommand(develop):
    def run(self):
        output_dir = compile_grammar()
        print(f"Compiled ANTLRv4 grammar in {output_dir}")
        develop.run(self)


def compile_grammar():
    parser_dir = os.path.join(os.path.dirname(__file__), "treeno/grammar")
    subprocess.check_output(
        ["antlr", "SqlBase.g4", "-Dlanguage=Python3", "-visitor", "-o", "gen"],
        cwd=parser_dir,
    )
    # The result is created in the subfolder `gen`
    return os.path.join(parser_dir, "gen")


setup(cmdclass={"develop": AntlrDevelopCommand})
