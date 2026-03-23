from __future__ import annotations

from dataclasses import dataclass


SERVICE_NAME = "MeeMeeScreener.AIExplain"


@dataclass(frozen=True)
class AiExplainSecretRef:
    credential_name: str

    @property
    def target_name(self) -> str:
        name = str(self.credential_name or "").strip() or "default"
        return f"{SERVICE_NAME}/{name}"


class AiExplainSecretStore:
    def read_secret(self, credential_name: str) -> str | None:
        raise NotImplementedError

    def write_secret(self, credential_name: str, secret: str) -> None:
        raise NotImplementedError

    def delete_secret(self, credential_name: str) -> None:
        raise NotImplementedError

    def has_secret(self, credential_name: str) -> bool:
        return self.read_secret(credential_name) is not None


class WindowsCredentialStore(AiExplainSecretStore):
    def _load_modules(self):
        try:
            import pywintypes  # type: ignore
            import win32cred  # type: ignore
        except Exception as exc:  # pragma: no cover - exercised only on non-Windows / missing pywin32
            raise RuntimeError("win32cred_unavailable") from exc
        return win32cred, pywintypes

    def _target(self, credential_name: str) -> AiExplainSecretRef:
        return AiExplainSecretRef(credential_name=str(credential_name or "").strip() or "default")

    def read_secret(self, credential_name: str) -> str | None:
        win32cred, pywintypes = self._load_modules()
        target = self._target(credential_name).target_name
        try:
            credential = win32cred.CredRead(target, win32cred.CRED_TYPE_GENERIC, 0)
        except pywintypes.error as exc:
            if getattr(exc, "winerror", None) == 1168:
                return None
            raise
        blob = credential.get("CredentialBlob")
        if blob is None:
            return None
        if isinstance(blob, bytes):
            return blob.decode("utf-16-le")
        if isinstance(blob, str):
            return blob
        return str(blob)

    def write_secret(self, credential_name: str, secret: str) -> None:
        win32cred, _ = self._load_modules()
        target = self._target(credential_name).target_name
        credential = {
            "Type": win32cred.CRED_TYPE_GENERIC,
            "TargetName": target,
            "CredentialBlob": str(secret or ""),
            "Persist": win32cred.CRED_PERSIST_LOCAL_MACHINE,
            "UserName": str(credential_name or "").strip() or "default",
        }
        win32cred.CredWrite(credential, 0)

    def delete_secret(self, credential_name: str) -> None:
        win32cred, pywintypes = self._load_modules()
        target = self._target(credential_name).target_name
        try:
            win32cred.CredDelete(target, win32cred.CRED_TYPE_GENERIC, 0)
        except pywintypes.error as exc:
            if getattr(exc, "winerror", None) == 1168:
                return
            raise


def get_default_ai_explain_secret_store() -> AiExplainSecretStore:
    return WindowsCredentialStore()
