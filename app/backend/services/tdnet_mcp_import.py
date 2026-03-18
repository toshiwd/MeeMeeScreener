import importlib

_impl = importlib.import_module("app.backend.services.data.tdnet_mcp_import")

globals().update(_impl.__dict__)
