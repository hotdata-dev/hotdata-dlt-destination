"""Live ibis backend bridge for the Hotdata dlt destination.

Routes ``pipeline.dataset().ibis()`` to a live ``ibis.hotdata`` connection by
wrapping ``dlt.helpers.ibis.create_ibis_backend``: Hotdata clients get the
out-of-tree ``hotdata-ibis`` backend, every other destination is delegated to
dlt's original dispatch.

The wrapper is installed by :func:`install_ibis_backend` on package import. ibis
is imported lazily, so a base install without the ``[ibis]`` extra is unaffected.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dlt.common.destination import TDestinationReferenceArg
    from dlt.common.destination.client import JobClientBase

# dlt's original create_ibis_backend, captured by install_ibis_backend().
_original_create_ibis_backend: Any = None


def ibis_connect(client: JobClientBase) -> Any:
    """Open a live ``ibis.hotdata`` connection for a Hotdata destination client.

    Maps the destination config onto the backend's ``connect`` parameters and
    binds the managed database by id (managed databases are addressed by id only).
    """
    import ibis

    from hotdata_dlt_destination.job_client import HotdataJobClient, _hotdata_api

    assert isinstance(client, HotdataJobClient)
    config = client.config

    with _hotdata_api(config) as api:
        database_id = api.resolved_database_id()

    return ibis.hotdata.connect(
        api_url=config.api_base_url,
        token=config.credentials.api_key,
        workspace_id=config.workspace_id,
        default_schema=config.schema,
        database_id=database_id,
    )


def _create_ibis_backend(
    destination: TDestinationReferenceArg,
    client: JobClientBase,
    read_only: bool = False,
    schemas: Any = (),
) -> Any:
    """Drop-in wrapper for ``dlt.helpers.ibis.create_ibis_backend``.

    Hotdata clients get a live ``ibis.hotdata`` connection; anything else is
    delegated to dlt's original dispatch.
    """
    from hotdata_dlt_destination.job_client import HotdataJobClient

    if isinstance(client, HotdataJobClient):
        return ibis_connect(client)
    return _original_create_ibis_backend(
        destination, client, read_only=read_only, schemas=schemas
    )


def install_ibis_backend() -> None:
    """Route ``dataset().ibis()`` through :func:`_create_ibis_backend`.

    Idempotent, and a no-op when ibis is not installed.
    """
    global _original_create_ibis_backend

    from dlt.common.exceptions import MissingDependencyException

    try:
        import dlt.helpers.ibis as dlt_ibis
    except MissingDependencyException:
        return

    if dlt_ibis.create_ibis_backend is _create_ibis_backend:
        return

    _original_create_ibis_backend = dlt_ibis.create_ibis_backend
    dlt_ibis.create_ibis_backend = _create_ibis_backend


__all__ = ["ibis_connect", "install_ibis_backend"]
