from setuptools import setup, find_packages

setup(
    name="tetherdb",
    version="0.1.0",
    author="Hunter McGuire",
    author_email="hunter@tendrl.com",
    description="A hybrid key-value store supporting Local, DynamoDB, and etcd backends.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/tendrl-inc-labs/TetherDB",
    packages=find_packages(),
    install_requires=[
        "boto3>=1.26.0",
        "etcd3gw>=1.0.2"
    ],
    extras_require={
        "dev": ["pytest", "flake8"],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.13",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.13",
    include_package_data=True,
    package_data={"": ["assets/TDB_logo.png"]}
)