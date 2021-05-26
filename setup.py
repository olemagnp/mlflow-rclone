from pydoc import describe
from importlib_metadata import entry_points
from setuptools import setup, find_packages


setup(
    name="mlflow-rclone-plugin",
    version="0.0.1",
    description="Plugin that allows the use of RClone as an artifact store.",
    packages=find_packages(),
    install_requires=["mlflow", "python-rclone"],
    entry_points={
        "mlflow.artifact_repository": "rclone=mlflow_rclone_plugin.rclone_repository:RCloneArtifactRepository"
    },
)
