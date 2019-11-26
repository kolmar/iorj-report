import pathlib


def ensure_directory(path):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
