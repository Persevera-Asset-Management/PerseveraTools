from setuptools import setup, find_packages

setup(
    name="persevera_tools",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "numpy",
        "psycopg2-binary",
        "sqlalchemy",
        "python-dotenv",
    ],
)