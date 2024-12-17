"""
Install script.
We are not using poetry or pantsbuild for to keep the simplicity of this exercice
and focus on the targeted questions.
"""
__author__ = "Corsi Christophe"

from setuptools import setup, find_packages


setup(
    name="axpo",
    version="1.0",
    packages=find_packages(),
)
