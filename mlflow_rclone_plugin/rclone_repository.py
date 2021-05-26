import json
import os

import urllib.parse
import logging

import rclone

from mlflow.entities.file_info import FileInfo
from mlflow.store.artifact.artifact_repo import ArtifactRepository
from mlflow.exceptions import MlflowException

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)


class RCloneArtifactRepository(ArtifactRepository):
    """Stores artifacts using a generic RClone-call."""

    is_plugin = True

    def __init__(self, artifact_uri):
        self.uri = artifact_uri
        parsed = urllib.parse.urlparse(artifact_uri)
        self.config = {
            "remote": parsed.netloc,
        }
        self.path = parsed.path

        _logger.info(self.config)

        rclone_conf = os.environ.get(
            "MLFLOW_RCLONE_CONF_FILE",
            os.path.join(os.path.expanduser("~"), ".config", "rclone", "rclone.conf"),
        )

        with open(rclone_conf, "r") as f:
            cfg = f.read()
        self.rclone = rclone.RClone(cfg)

        self.rclone.log.setLevel(logging.ERROR)
        remotes = self.rclone.listremotes()
        if remotes["code"] != 0:
            raise remotes["error"]

        if self.config["remote"] is None:
            raise MlflowException("No remote found in uri")

        remotes = remotes["out"].decode("utf-8").strip().split("\n")
        if f"{self.config['remote']}:" not in remotes:
            raise MlflowException(f"{self.config['remote']} not found in remotes")

        super().__init__(artifact_uri)

    def _is_dir(self, path):
        res = self.rclone.run_cmd(
            "rmdir", ["--dry-run", f"{self.config['remote']}:{path}"]
        )
        return res["code"] == 0

    def _mkdir(self, artifact_dir):
        self.rclone.run_cmd("mkdir", [f"{self.config['remote']}:{artifact_dir}"])

    def _size(self, full_file_path):
        res = self.rclone.run_cmd(
            "size", ["--json", f"{self.config['remote']}:{full_file_path}"]
        )
        res = res["out"].decode("utf-8")
        res = json.loads(res)

        if res["count"] != 1:
            raise MlflowException(f"Illegal number of files: {res['count']}")

        return res["bytes"]

    def log_artifact(self, local_file, artifact_path=None):
        artifact_dir = (
            os.path.join(self.path, artifact_path) if artifact_path else self.path
        )
        self._mkdir(artifact_dir)
        self.rclone.copy(
            local_file,
            f"{self.config['remote']}:{os.path.join(artifact_dir)}",
        )

    def log_artifacts(self, local_dir, artifact_path=None):
        dest_path = (
            os.path.join(self.path, artifact_path) if artifact_path else self.path
        )
        local_dir = os.path.abspath(local_dir)

        self.rclone.copy(local_dir, f"{self.config['remote']}:{dest_path}")

    def list_artifacts(self, path=None):
        artifact_dir = self.path
        list_dir = os.path.join(artifact_dir, path) if path else artifact_dir
        res = self.rclone.run_cmd(
            "lsf", ["--recursive", f"{self.config['remote']}:{list_dir}"]
        )

        if res["code"] != 0:
            return []

        files = res["out"].decode("utf-8").strip().split()

        infos = []

        for f in files:
            path = os.path.join(list_dir, f)
            if self._is_dir(path):
                infos.append(FileInfo(path, True, None))
            else:
                size = self._size(path)
                infos.append(FileInfo(path, False, size))

        return infos

    def _download_file(self, remote_file_path, local_path):
        _logger.error(remote_file_path + ", " + local_path)
        remote_full_path = (
            os.path.join(self.path, remote_file_path) if remote_file_path else self.path
        )
        self.rclone.copy(f"{self.config['remote']}:{remote_full_path}", local_path)

    def delete_artifacts(self, artifact_path=None):
        raise MlflowException("Not implemented yet")
