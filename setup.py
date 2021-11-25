from glob import glob

from os.path import basename
from os.path import splitext

from setuptools import find_packages, setup


setup(
    name="purviewatlaspoc",
    version="0.0.1",
    description="purview atlas poc",
    python_requires=">=3.8",
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    include_package_data=True,
    author="jvanbuel",
    zip_safe=False,
    keywords="purview, atlas, data pipelines, data engineering",
    extras_require={},
)