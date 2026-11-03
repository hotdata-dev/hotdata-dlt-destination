"""Run dbt packages against the instant database a dlt pipeline loads.

dlt's stock ``dlt.dbt.package()`` can't drive this destination: it requires a
``DestinationClientDwhConfiguration`` (ours is not one — the destination
addresses an instant database, not a dataset) and dlt's bundled profiles.yml
has no ``hotdata`` entry. This module wires dlt's dbt runner up directly:
it ships its own profile (``dbt_profiles/profiles.yml``, adapter type
``hotdata`` from the ``dbt-hotdata`` package) and hands the pipeline's
resolved destination configuration to dbt through the ``DLT__*`` environment
variables the runner already exports before every dbt command.

The dbt package builds in the SAME instant database the pipeline loaded, in
the same schema by default (``destination_dataset_name`` var overrides). The
``dbt-hotdata`` adapter must be importable where dbt runs: install this
package's ``dbt`` extra to run in the current environment, or pass the
isolated venv from :func:`get_venv` (which installs dbt-core + dbt-hotdata).

    pipeline = dlt.pipeline(destination=hotdata(database_id="..."), ...)
    pipeline.run(source)

    from hotdata_dlt_destination import dbt as hotdata_dbt
    runner = hotdata_dbt.package(pipeline, "./transform", venv=hotdata_dbt.get_venv(pipeline))
    runner.run_all()
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import TYPE_CHECKING

from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.schema import Schema
from dlt.common.schema.utils import normalize_schema_name
from dlt.helpers.dbt.runner import DBTPackageRunner, create_runner

# Stock get_venv works unchanged for this destination: it derives the adapter
# package from the destination name ("hotdata" -> "dbt-hotdata" on PyPI) and
# runs dbt in a subprocess, so the venv's deps never touch the pipeline's.
from dlt.pipeline.dbt import get_venv

from hotdata_dlt_destination.configuration import (
    HotdataClientConfiguration,
    validate_credentials,
)

if TYPE_CHECKING:
    from dlt.common.runners import Venv
    from dlt.pipeline.pipeline import Pipeline

__all__ = ["get_venv", "package"]

PROFILES_DIR = str(Path(__file__).parent / "dbt_profiles")


def _resolve_database_id(config: HotdataClientConfiguration) -> str | None:
    """The id of the instant database dbt must build in, or None.

    A pinned ``database_id`` wins. Failing that, a database resolved or
    created earlier in this process left its record cached on the shared
    config (``_hotdata_db``, see ``HotdataClient.bind_run_cache``).
    """
    if config.database_id:
        return config.database_id
    cached = getattr(config, "_hotdata_db", None)
    return cached.id if cached is not None else None


def package(
    pipeline: Pipeline,
    package_location: str,
    package_repository_branch: str | None = None,
    package_repository_ssh_key: str = "",
    auto_full_refresh_when_out_of_sync: bool = True,
    venv: Venv | None = None,
) -> DBTPackageRunner:
    """Create a dbt runner for `package_location`, wired to `pipeline`'s database.

    Mirrors ``dlt.dbt.package()``: `package_location` is a local path or a git
    URL, `venv=None` runs dbt in the current environment (needs the ``dbt``
    extra), and the returned runner's ``run_all()``/``test()`` execute against
    the pipeline's instant database. Raises ``ConfigurationValueError`` when no
    ``database_id`` can be determined — dbt building in a freshly created
    database instead of the one dlt loaded is exactly the mistake id-first
    addressing exists to prevent, so there is no create-if-missing fallback.
    """
    schema = (
        pipeline.default_schema
        if pipeline.default_schema_name
        else Schema(normalize_schema_name(pipeline.dataset_name or pipeline.pipeline_name))
    )
    job_client = pipeline._get_destination_clients(schema)[0]
    config = job_client.config
    if not isinstance(config, HotdataClientConfiguration):
        raise ConfigurationValueError(
            f"pipeline '{pipeline.pipeline_name}' does not target the hotdata destination"
            f" (got {type(config).__name__})"
        )
    validate_credentials(config)
    database_id = _resolve_database_id(config)
    if not database_id:
        raise ConfigurationValueError(
            "cannot determine which instant database dbt should build in: pin database_id"
            " (hotdata(database_id=...), HOTDATA_DATABASE_ID, or [destination.hotdata]"
            " database_id) — the id is logged when a first run creates the database"
        )

    # The runner exports this config as DLT__* env vars before each dbt
    # command; dbt_profiles/profiles.yml reads them back. The copy pins the
    # resolved id, forbids creates on the dbt side, and carries the schema as
    # `dataset_name` — the attribute create_runner reads for dbt's
    # source_dataset_name var. Injected on the copy only: it is not a declared
    # field, so it never reaches the environment.
    run_config = copy.copy(config)
    run_config.database_id = database_id
    run_config.create_database_if_missing = False
    run_config.dataset_name = config.schema  # type: ignore[attr-defined]
    return create_runner(
        venv,  # type: ignore[arg-type]  # None means current environment, handled inside
        run_config,  # type: ignore[arg-type]
        pipeline.working_dir,
        package_location,
        package_repository_branch=package_repository_branch,
        package_repository_ssh_key=package_repository_ssh_key,
        package_profiles_dir=PROFILES_DIR,
        package_profile_name="hotdata",
        auto_full_refresh_when_out_of_sync=auto_full_refresh_when_out_of_sync,
    )
