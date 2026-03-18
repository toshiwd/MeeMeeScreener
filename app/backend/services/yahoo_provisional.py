import importlib

_impl = importlib.import_module("app.backend.services.data.yahoo_provisional")

globals().update(_impl.__dict__)
