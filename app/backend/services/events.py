import importlib

_impl = importlib.import_module("app.backend.services.data.events")

globals().update(_impl.__dict__)
