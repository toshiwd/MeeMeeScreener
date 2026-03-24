# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_submodules
from PyInstaller.utils.hooks import collect_all

datas = [('C:\\work\\meemee-screener\\app\\main.py', 'app'), ('C:\\work\\meemee-screener\\app\\__init__.py', 'app'), ('C:\\work\\meemee-screener\\app\\core\\__init__.py', 'app/core'), ('C:\\work\\meemee-screener\\app\\core\\*.py', 'app/core'), ('C:\\work\\meemee-screener\\app\\backend\\__init__.py', 'app/backend'), ('C:\\work\\meemee-screener\\app\\backend\\*.py', 'app/backend'), ('C:\\work\\meemee-screener\\app\\backend\\api', 'app/backend/api'), ('C:\\work\\meemee-screener\\app\\backend\\core\\__init__.py', 'app/backend/core'), ('C:\\work\\meemee-screener\\app\\backend\\core\\*.py', 'app/backend/core'), ('C:\\work\\meemee-screener\\app\\desktop\\*.py', 'app/desktop'), ('C:\\work\\meemee-screener\\app\\backend\\static', 'app/backend/static'), ('C:\\work\\meemee-screener\\resources\\icons\\app_icon.ico', 'resources/icons'), ('C:\\work\\meemee-screener\\tools\\export_pan.vbs', 'tools'), ('C:\\work\\meemee-screener\\tools\\code.txt', 'tools'), ('C:\\work\\meemee-screener\\app\\backend\\rank_config.json', 'app/backend'), ('C:\\work\\meemee-screener\\app\\backend\\update_state.json', 'app/backend'), ('C:\\work\\meemee-screener\\app\\backend\\favorites.sqlite', 'app/backend'), ('C:\\work\\meemee-screener\\app\\backend\\practice.sqlite', 'app/backend'), ('C:\\work\\meemee-screener\\app\\backend\\stocks.duckdb', 'app/backend'), ('C:\\work\\meemee-screener\\fixtures', 'fixtures')]
binaries = []
hiddenimports = ['uvicorn', 'uvicorn.lifespan.on', 'uvicorn.protocols.http.h11_impl', 'uvicorn.protocols.websockets.websockets_impl', 'pythonnet', 'clr', 'clr_loader', 'System', 'win32timezone', 'webview.platforms.winforms', 'app.backend', 'app.backend.main', 'app.core', 'app.core.config']
hiddenimports += collect_submodules('multipart')
hiddenimports += collect_submodules('app.backend')
hiddenimports += collect_submodules('app')
tmp_ret = collect_all('uvicorn')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]


a = Analysis(
    ['..\\..\\app\\desktop\\launcher.py'],
    pathex=['C:\\work\\meemee-screener'],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='MeeMeeScreener',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['C:\\work\\meemee-screener\\resources\\icons\\app_icon.ico'],
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='MeeMeeScreener',
)
