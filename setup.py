import os
import subprocess
from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install import install


class AntlrDevelopCommand(develop):
    def run(self):
        output_dir = compile_grammar()
        print(f"Compiled ANTLRv4 grammar in {output_dir}")
        develop.run(self)


class AntlrInstallCommand(install):
    def run(self):
        output_dir = compile_grammar()
        print(f"Compiled ANTLRv4 grammar in {output_dir}")
        install.run(self)


def compile_grammar():
    parser_dir = os.path.join(os.path.dirname(__file__), "treeno/grammar")
    subprocess.check_output(
        ["antlr", "SqlBase.g4", "-Dlanguage=Python3", "-visitor", "-o", "gen"],
        cwd=parser_dir,
    )
    # The result is created in the subfolder `gen`
    return os.path.join(parser_dir, "gen")


setup(
    name="treeno",
    description="A trino SQL parsing library",
    version="0.0.1",
    author="Ray Zhang",
    author_email="peifeng2005@gmail.com",
    packages=["treeno"],
    install_requires=[
        "setuptools",
        "antlr4-python3-runtime==4.9.2",
        "nltk==3.6.5",
        "attrs==21.2.0",
    ],
    cmdclass={"install": AntlrInstallCommand, "develop": AntlrDevelopCommand},
    license="MIT",
    python_requires=">=3.9",
)
