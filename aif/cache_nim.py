# cache_nim.py
from __future__ import annotations
import os, json, hashlib, time, tempfile
from pathlib import Path
from typing import Callable, Any, Tuple, Optional

def _stable_json_dumps(obj: Any) -> str:
    # Deterministic JSON (no spaces) for hashing
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)

def _hash_key(endpoint: str, payload: dict, version: Optional[str]) -> str:
    material = {
        "endpoint": endpoint,
        "payload": payload,   # assume already JSON-serializable
        "version": version or "",
    }
    return hashlib.sha1(_stable_json_dumps(material).encode("utf-8")).hexdigest()

def _atomic_write(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)

def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))

def _write_json(path: Path, obj: Any):
    _atomic_write(path, _stable_json_dumps(obj).encode("utf-8"))

def query_nim_cached(
    *,
    step: str,
    endpoint: str,
    payload: dict,
    fetch_fn: Callable[[dict], Tuple[int, Any]],
    cache_dir: str = ".nim_cache",
    version: Optional[str] = None,   # e.g., model tag, container image, git SHA
    ttl_seconds: Optional[int] = None,
    refresh: bool = False,
    save_extra: Optional[Callable[[Any, Path], None]] = None,  # hook to write PDBs, etc.
) -> Tuple[int, Any]:
    """
    Returns (rc, response) like your existing query_nim. On cache hit, rc=200 and
    response gets "_from_cache"=True. On miss, calls fetch_fn(payload), saves result, returns it.
    """
    base = Path(cache_dir)
    key = _hash_key(endpoint, payload, version)
    outdir = base / step / key
    result_path = outdir / "result.json"
    meta_path = outdir / "meta.json"
    payload_path = outdir / "payload.json"

    # 1) Load from cache if allowed
    if not refresh and result_path.exists():
        try:
            # TTL check (based on meta["created_at"])
            if ttl_seconds is not None and meta_path.exists():
                meta = _read_json(meta_path)
                age = time.time() - float(meta.get("created_at", 0))
                if age > ttl_seconds:
                    # stale -> recompute
                    pass
                else:
                    resp = _read_json(result_path)
                    if isinstance(resp, dict):
                        resp["_from_cache"] = True
                    return (200, resp)
            else:
                resp = _read_json(result_path)
                if isinstance(resp, dict):
                    resp["_from_cache"] = True
                return (200, resp)
        except Exception:
            # Corrupt cache => fall through to recompute
            pass

    # 2) Cache miss -> fetch
    rc, response = fetch_fn(payload)

    # 3) Persist atomically
    meta = {
        "step": step,
        "key": key,
        "endpoint": endpoint,
        "version": version,
        "created_at": time.time(),
    }
    try:
        outdir.mkdir(parents=True, exist_ok=True)
        _write_json(meta_path, meta)
        _write_json(payload_path, payload)
        _write_json(result_path, response)
        if save_extra:
            # Optional: write PDBs or other artifacts alongside result.json
            save_extra(response, outdir)
    except Exception as e:
        # Don't fail the call if writing cache has issues
        print(f"[cache_nim] Warning: failed to write cache: {e}")

    return rc, response
