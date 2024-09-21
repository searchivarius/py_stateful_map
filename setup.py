from os import path
from setuptools import setup, find_packages

from version import __version__

print('Building version:', __version__)

curr_dir = path.abspath(path.dirname(__file__))
with open(path.join(curr_dir, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='py_stateful_map',
    version=__version__,
    description='A an extension of the multiproc/thread map that supports stateful workers & size-aware iterators.',
    author="Leonid Boytsov",
    author_email="leo@boytsov.info",
    long_description=long_description,
    long_description_content_type='text/markdown',
    py_modules=['py_stateful_map'],
    install_requires=[],
    license='Apache 2.0',
    python_requires='>=3.6',
)
