import json
import os
import sys
from typing import Any, TypeVar

T = TypeVar('T')


class JSONConfig:

    def __init__(self, file_path: str | None = None) -> None:

        if file_path is None:
            script_root = os.path.abspath(os.path.dirname(sys.argv[0]))
            file_path = os.path.join(script_root, "config.json")

        self.file_path = file_path

        if os.path.exists(file_path):
            with open(file_path) as f:
                self.config = json.load(f)
        else:
            self.config: dict[Any, Any] = {}

    def __sync(self) -> None:

        with open(self.file_path, "w+") as f:
            f.write(json.dumps(self.config, indent=4))

    def __get_node(self, path: str) -> Any | None:

        cur_node = self.config

        for k in path.split("/")[1:]:

            if k in cur_node:
                cur_node = cur_node[k]
            else:
                return None

        return cur_node

    def __str__(self) -> str:
        return self.file_path

    ############################################################################
    # PUBLIC
    ############################################################################
    ##########################################
    # GET
    ##########################################

    def get(self, path: str, default: Any | None = None) -> Any:

        v = self.__get_node(path)

        if v is not None:
            return v

        if default is not None:
            return default

        raise KeyError(f"{path} was not found")

    def get_bool(self, path: str, default: bool | None = None) -> bool:

        v = self.get(path, default)

        if isinstance(v, bool):
            return v

        raise ValueError(f"{v} is not a int")

    def get_int(self, path: str, default: int | None = None) -> int:

        v = self.get(path, default)

        if isinstance(v, int):
            return v

        raise ValueError(f"{v} is not a int")

    def get_str(self, path: str, default: str | None = None) -> str:

        v = self.get(path, default)

        if isinstance(v, str):
            return v

        raise ValueError(f"{v} is not a str")

    def get_float(self, path: str, default: float | None = None) -> float:

        v = self.get(path, default)

        if isinstance(v, float):
            return v

        raise ValueError(f"{v} is not a float")

    def get_list(self, path: str, default: list[T] | None) -> list[T]:

        v = self.get(path, default)

        if isinstance(v, list):
            return v  # type: ignore

        raise ValueError(f"{v} is not a list")

    ##########################################
    # SET
    ##########################################

    def set(self, path: str, value: Any | None) -> None:

        cur_node = self.config

        path_list = path.split("/")[1:]

        for k in path_list[:-1]:

            if k not in cur_node:
                cur_node[k] = {}
            cur_node = cur_node[k]

        cur_node[path_list[-1]] = value
        self.__sync()
