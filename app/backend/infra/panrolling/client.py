import subprocess
import os
import logging

logger = logging.getLogger(__name__)


def _hidden_process_kwargs() -> dict[str, object]:
    kwargs: dict[str, object] = {
        "creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0),
    }
    startupinfo_factory = getattr(subprocess, "STARTUPINFO", None)
    if startupinfo_factory is not None:
        startupinfo = startupinfo_factory()
        startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
        startupinfo.wShowWindow = getattr(subprocess, "SW_HIDE", 0)
        kwargs["startupinfo"] = startupinfo
    return kwargs

class PanRollingClient:
    def __init__(self, vbs_path: str):
        self.vbs_path = vbs_path

    def run_export(self, code_path: str, out_dir: str, timeout: int = 1800) -> int:
        """
        Runs the VBS script to export data from Pan Rolling.
        Returns exit code.
        """
        if not os.path.exists(self.vbs_path):
            logger.error(f"VBS script not found: {self.vbs_path}")
            return -1

        # Locate cscript
        sys_root = os.environ.get("SystemRoot", "C:\\Windows")
        cscript = os.path.join(sys_root, "SysWOW64", "cscript.exe")
        if not os.path.exists(cscript):
             cscript = os.path.join(sys_root, "System32", "cscript.exe")

        cmd = [cscript, "//nologo", self.vbs_path, code_path, out_dir]
        logger.info(f"Executing VBS: {cmd}")

        try:
            # For simplicity in this new infra, we might just use subprocess.run
            # But the legacy code had complex stdout parsing for progress.
            # We will keep it simple for now, can enhance if needed.
            result = subprocess.run(
                cmd, 
                timeout=timeout, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT,
                text=True, 
                encoding="cp932", 
                errors="replace",
                **_hidden_process_kwargs(),
            )
            print(f"[PanRolling] {result.stdout}")
            return result.returncode
        except subprocess.TimeoutExpired:
            logger.error("VBS execution timed out")
            return -1
        except Exception as e:
            logger.error(f"VBS execution failed: {e}")
            return -1
