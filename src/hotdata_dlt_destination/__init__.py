from hotdata_dlt_destination.factory import hotdata
from hotdata_dlt_destination.ibis_backend import install_ibis_backend

# Route dataset().ibis() through our wrapper so a Hotdata dataset gets a live
# ibis.hotdata backend. No-op when ibis is not installed.
install_ibis_backend()

__all__ = ["hotdata"]
