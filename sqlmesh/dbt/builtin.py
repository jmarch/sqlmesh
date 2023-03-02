from __future__ import annotations

import os
import typing as t

from sqlmesh.utils.errors import ConfigError


class ExceptionsJinja:
    """Implements the dbt "exceptions" jinja namespace."""

    def raise_compiler_error(self, msg: str) -> None:
        from dbt.exceptions import CompilationError

        raise CompilationError(msg)

    def warn(self, msg: str) -> str:
        print(msg)
        return ""


class Api:
    def __init__(self) -> None:
        from dbt.adapters.base.column import Column
        from dbt.adapters.base.relation import BaseRelation

        self.Relation = BaseRelation
        self.Column = Column


def env_var(name: str, default: t.Optional[str] = None) -> t.Optional[str]:
    if name not in os.environ and default is None:
        raise ConfigError(f"Missing environment variable '{name}'")
    return os.environ.get(name, default)


def is_incremental() -> bool:
    return False


def log(msg: str, info: bool = False) -> str:
    print(msg)
    return ""


def no_log(msg: str, info: bool = False) -> str:
    return ""


def config(*args: t.Any, **kwargs: t.Any) -> str:
    return ""


def generate_var(variables: t.Dict[str, t.Any]) -> t.Callable:
    DBT_VAR_MAPPING = variables.copy()

    def var(name: str, default: t.Optional[str] = None) -> str:
        return DBT_VAR_MAPPING.get(name, default)

    return var


def generate_ref(refs: t.Dict[str, str]) -> t.Callable:
    DBT_REF_MAPPING = refs.copy()

    # TODO suport package name
    def ref(package: str, name: t.Optional[str] = None) -> t.Optional[str]:
        name = name or package
        return DBT_REF_MAPPING.get(name)

    return ref


def generate_source(sources: t.Dict[str, str]) -> t.Callable:
    DBT_SOURCE_MAPPING = sources.copy()

    def source(package: str, name: str) -> t.Optional[str]:
        return DBT_SOURCE_MAPPING.get(f"{package}.{name}")

    return source


BUILTIN_JINJA = {
    "env_var": env_var,
    "is_incremental": is_incremental,
    "log": no_log,
    "config": config,
    "sqlmesh": True,
    "exceptions": ExceptionsJinja(),
    "api": Api(),
}
