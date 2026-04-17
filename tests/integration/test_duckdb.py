"""Integration + unit tests for fdl duckdb.

Spec (docs/reference/cli.md#duckdb):
  fdl duckdb TARGET [--read-only] [--force] [--dry-run] [--duckdb-bin PATH]
  - Resolves target, checks staleness, builds ATTACH SQL
  - --dry-run prints shlex.join(argv) and exits
  - Otherwise os.execvp()s into duckdb so TTY/signals/exit-code are inherited

os.execvp would replace the pytest process, so every CLI test either:
 (a) uses --dry-run (no exec),
 (b) monkeypatches os.execvp, or
 (c) passes a nonexistent --duckdb-bin so FileNotFoundError fires before exec.
"""

import json
import re
import shlex
from pathlib import Path

from typer.testing import CliRunner

from fdl.cli import app
from fdl.ducklake import build_attach_sql


def _parse_dry_run(out: str) -> tuple[str, list[str]]:
    """Parse a --dry-run stdout line into (bin, [statements])."""
    argv = shlex.split(out.strip())
    bin_ = argv[0]
    stmts: list[str] = []
    i = 1
    while i < len(argv):
        assert argv[i] == "-cmd", argv
        stmts.append(argv[i + 1])
        i += 2
    return bin_, stmts


def _init_local(fdl_project_dir: Path) -> Path:
    """Init a project with a local target. Returns the storage directory."""
    storage = fdl_project_dir / "storage"
    CliRunner().invoke(
        app,
        [
            "init",
            "test_ds",
            "--public-url",
            "http://localhost:4001",
            "--target-url",
            str(storage),
            "--target-name",
            "default",
        ],
    )
    return storage


# --- unit tests: build_attach_sql -------------------------------------------


def test_helper_local_target_generates_expected_sql(fdl_project_dir: Path):
    """Local target produces INSTALL/ATTACH/USE and no S3-specific statements."""
    _init_local(fdl_project_dir)

    stmts = build_attach_sql("default")

    assert stmts[0] == "INSTALL ducklake; LOAD ducklake;"
    assert stmts[-1] == "USE test_ds"
    joined = "\n".join(stmts)
    assert "httpfs" not in joined
    assert "CREATE SECRET" not in joined

    attach = next(s for s in stmts if s.startswith("ATTACH "))
    assert "ducklake:" in attach
    assert "AS test_ds" in attach
    assert "OVERRIDE_DATA_PATH true" in attach
    assert "READ_ONLY" not in attach


def test_helper_read_only_appends_option(fdl_project_dir: Path):
    """--read-only appends READ_ONLY to the ATTACH options."""
    _init_local(fdl_project_dir)

    stmts = build_attach_sql("default", read_only=True)
    attach = next(s for s in stmts if s.startswith("ATTACH "))
    assert "READ_ONLY" in attach


def test_helper_s3_target_includes_httpfs_and_secret(s3_project: Path):
    """S3 target adds httpfs load + CREATE SECRET, and DATA_PATH points at S3."""
    stmts = build_attach_sql("default")

    assert "INSTALL httpfs; LOAD httpfs;" in stmts
    secret = next(s for s in stmts if s.startswith("CREATE SECRET"))
    assert "TYPE s3" in secret
    assert "KEY_ID 'testing'" in secret
    assert "SECRET 'testing'" in secret
    assert "URL_STYLE 'path'" in secret

    attach = next(s for s in stmts if s.startswith("ATTACH "))
    assert "DATA_PATH 's3://test-bucket/test_ds/ducklake.duckdb.files/'" in attach


def test_helper_escapes_single_quote_in_paths(
    fdl_project_dir: Path, monkeypatch, tmp_path
):
    """Paths with single quotes are escaped (' -> '') in ATTACH."""
    _init_local(fdl_project_dir)

    quoted_dir = tmp_path / "foo's bar"
    quoted_dir.mkdir()
    catalog = quoted_dir / "ducklake.duckdb"
    catalog.touch()
    monkeypatch.setenv("FDL_CATALOG", str(catalog))

    stmts = build_attach_sql("default")
    attach = next(s for s in stmts if s.startswith("ATTACH "))

    # Doubled quote appears inside the 'ducklake:...' literal.
    assert re.search(r"ducklake:[^']*foo''s bar", attach)
    # And no unescaped single quote inside the ducklake:... literal.
    match = re.search(r"'ducklake:(.*?)' AS ", attach)
    assert match is not None
    inner = match.group(1)
    assert "''" in inner
    assert "'" not in inner.replace("''", "")


# --- CLI integration --------------------------------------------------------


def test_dry_run_local_prints_shlex_argv(fdl_project_dir: Path):
    """--dry-run prints a shell-reexecutable duckdb invocation."""
    _init_local(fdl_project_dir)

    result = CliRunner().invoke(app, ["duckdb", "default", "--dry-run"])
    assert result.exit_code == 0, result.output

    bin_, stmts = _parse_dry_run(result.output)
    assert bin_ == "duckdb"
    assert stmts[0] == "INSTALL ducklake; LOAD ducklake;"
    assert stmts[-1] == "USE test_ds"
    attach = next(s for s in stmts if s.startswith("ATTACH "))
    assert attach.startswith("ATTACH 'ducklake:")


def test_dry_run_read_only_includes_option(fdl_project_dir: Path):
    """--dry-run with --read-only includes READ_ONLY in the ATTACH."""
    _init_local(fdl_project_dir)

    result = CliRunner().invoke(
        app, ["duckdb", "default", "--dry-run", "--read-only"]
    )
    assert result.exit_code == 0, result.output
    _, stmts = _parse_dry_run(result.output)
    attach = next(s for s in stmts if s.startswith("ATTACH "))
    assert "READ_ONLY" in attach


def test_dry_run_custom_duckdb_bin(fdl_project_dir: Path):
    """--duckdb-bin is reflected in the argv."""
    _init_local(fdl_project_dir)

    result = CliRunner().invoke(
        app, ["duckdb", "default", "--dry-run", "--duckdb-bin", "/opt/duckdb"]
    )
    assert result.exit_code == 0, result.output
    bin_, _ = _parse_dry_run(result.output)
    assert bin_ == "/opt/duckdb"


def test_dry_run_s3_target_includes_secret(s3_project: Path):
    """S3 target's dry-run output embeds httpfs load and CREATE SECRET."""
    result = CliRunner().invoke(app, ["duckdb", "default", "--dry-run"])
    assert result.exit_code == 0, result.output

    _, stmts = _parse_dry_run(result.output)
    assert "INSTALL httpfs; LOAD httpfs;" in stmts
    secret = next(s for s in stmts if s.startswith("CREATE SECRET"))
    assert "TYPE s3" in secret
    assert "URL_STYLE 'path'" in secret


def test_stale_catalog_is_rejected(fdl_project_dir: Path):
    """Stale local catalog is rejected with the same error as fdl sql."""
    storage = _init_local(fdl_project_dir)
    cli = CliRunner()
    cli.invoke(app, ["push", "default"])

    remote_meta = storage / "test_ds" / ".fdl" / "meta.json"
    remote_meta.write_text(json.dumps({"pushed_at": "2099-01-01T00:00:00+00:00"}))

    result = cli.invoke(app, ["duckdb", "default", "--dry-run"])
    assert result.exit_code != 0
    assert "fdl pull" in result.output.lower()


def test_force_skips_freshness_check(fdl_project_dir: Path):
    """--force bypasses the stale catalog check."""
    storage = _init_local(fdl_project_dir)
    cli = CliRunner()
    cli.invoke(app, ["push", "default"])

    remote_meta = storage / "test_ds" / ".fdl" / "meta.json"
    remote_meta.write_text(json.dumps({"pushed_at": "2099-01-01T00:00:00+00:00"}))

    result = cli.invoke(
        app, ["duckdb", "default", "--dry-run", "--force"]
    )
    assert result.exit_code == 0, result.output


def test_unknown_target_gives_bad_parameter(fdl_project_dir: Path):
    """Unknown target exits non-zero with a 'not found' message."""
    _init_local(fdl_project_dir)

    result = CliRunner().invoke(
        app, ["duckdb", "nonexistent", "--dry-run"]
    )
    assert result.exit_code != 0
    assert "not found" in result.output


def test_execvp_not_invoked_on_dry_run(fdl_project_dir: Path, monkeypatch):
    """--dry-run must not call os.execvp."""
    _init_local(fdl_project_dir)

    calls: list[tuple] = []

    def _fake(file, args):
        calls.append((file, args))

    monkeypatch.setattr("os.execvp", _fake)

    result = CliRunner().invoke(app, ["duckdb", "default", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert calls == []


def test_missing_duckdb_binary_reports_error(fdl_project_dir: Path):
    """Nonexistent --duckdb-bin exits 127 with a friendly message.

    This test deliberately does NOT pass --dry-run, so os.execvp runs.
    Because the path does not exist, FileNotFoundError fires before
    the process is actually replaced — safe to run under pytest.
    """
    _init_local(fdl_project_dir)

    result = CliRunner().invoke(
        app,
        [
            "duckdb",
            "default",
            "--duckdb-bin",
            "/definitely/not/here/duckdb-xyz",
        ],
    )
    assert result.exit_code == 127
    assert "not found" in result.output
