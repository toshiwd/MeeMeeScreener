import importlib

_impl = importlib.import_module("app.backend.services.data.txt_update")

globals().update(_impl.__dict__)
