from setuptools import setup, find_packages

setup(
    name="synthetic_builder_py",
    version="0.1.0",
    author="Priyam",
    description="Synthetic builder",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/priyam-gsc/synthetic_builder_py",
    packages=find_packages(),
    install_requires=[
        "numpy",
        "pandas",
        "requests",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    package_data={
        "synthetic_builder_py": ["config/*.json"]
    },
    include_package_data=True,
)
