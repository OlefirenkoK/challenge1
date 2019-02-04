import os
from setuptools import setup, find_packages


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


setup(
    name='challenge',
    version='0.1',
    author='Olefirenko K',
    description='',
    install_requires=read("requirements.txt"),
    packages=find_packages(),
    test_suite='tests',
    entry_points={
        "console_scripts": [
            "run_challenge = challenge.run:run",
            "init_challenge_db = challenge.run:init_db"
        ]
    }
)
