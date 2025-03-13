from setuptools import setup, find_packages

setup(
    name="persevera_tools",
    version="0.2.11",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "numpy",
        "psycopg2-binary",
        "sqlalchemy",
        "python-dotenv",
        "xbbg",
        "sidrapy",
        "fredapi",
        "python-bcb"
    ],
)