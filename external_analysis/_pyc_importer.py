from __future__ import annotations

import importlib.abc
import importlib.util
import marshal
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Optional


@dataclass(frozen=True)
class _BytecodeModuleLoader(importlib.abc.Loader):
    pyc_path: Path

    def create_module(self, spec):  # type: ignore[override]
        return None

    def exec_module(self, module: ModuleType) -> None:  # type: ignore[override]
        data = self.pyc_path.read_bytes()
        if len(data) < 16:
            raise ImportError(f"invalid pyc payload: {self.pyc_path}")
        code = marshal.loads(data[16:])
        module.__dict__.setdefault("__file__", str(self.pyc_path))
        module.__dict__.setdefault("__cached__", str(self.pyc_path))
        exec(code, module.__dict__)


class _BytecodeModuleFinder(importlib.abc.MetaPathFinder):
    def __init__(self, package_prefix: str, package_dir: Path) -> None:
        self._package_prefix = package_prefix
        self._package_dir = package_dir

    def find_spec(self, fullname: str, path, target=None):  # type: ignore[override]
        del path, target
        if not fullname.startswith(f"{self._package_prefix}."):
            return None
        relative = fullname[len(self._package_prefix) + 1 :].split(".")
        module_path = self._package_dir.joinpath(*relative)
        source_path = module_path.with_suffix(".py")
        if source_path.exists():
            return None
        pyc_path = module_path.parent / "__pycache__" / f"{module_path.name}.cpython-{sys.version_info.major}{sys.version_info.minor}.pyc"
        if not pyc_path.exists():
            return None
        loader = _BytecodeModuleLoader(pyc_path)
        return importlib.util.spec_from_loader(fullname, loader, origin=str(pyc_path))


def install_bytecode_finder(package_prefix: str, package_dir: Path) -> None:
    finder = _BytecodeModuleFinder(package_prefix=package_prefix, package_dir=package_dir)
    for existing in sys.meta_path:
        if isinstance(existing, _BytecodeModuleFinder) and existing._package_prefix == package_prefix:
            return
    sys.meta_path.insert(0, finder)

