$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$ts = Get-Date -Format "yyyyMMdd-HHmmss"

$venvPy = Join-Path $repoRoot ".venv\\Scripts\\python.exe"
if (Test-Path $venvPy) {
  $py = $venvPy
} else {
  $py = "python"
}

$stage = Join-Path $repoRoot ("_tmp\\audit_" + $ts)
New-Item -ItemType Directory -Force $stage | Out-Null

$pytestOut = Join-Path $stage "pytest.txt"
& $py -m pytest -q *>&1 | Out-File -FilePath $pytestOut -Encoding utf8

$replayCmd = @(
  "-m", "sublimine.run",
  "--mode", "replay",
  "--config", "config/sublimine.yaml",
  "--replay", "tests/data/replay.jsonl"
)

$replay1 = Join-Path $stage "replay_1.txt"
& $py @replayCmd *>&1 | Out-File -FilePath $replay1 -Encoding utf8

$replay2 = Join-Path $stage "replay_2.txt"
& $py @replayCmd *>&1 | Out-File -FilePath $replay2 -Encoding utf8

$hash1 = Get-FileHash -Algorithm SHA256 $replay1
$hash2 = Get-FileHash -Algorithm SHA256 $replay2

$hashOut = Join-Path $stage "replay_hashes.txt"
@(
  ("replay_1_sha256=" + $hash1.Hash),
  ("replay_2_sha256=" + $hash2.Hash),
  ("match=" + ($hash1.Hash -eq $hash2.Hash))
) | Out-File -FilePath $hashOut -Encoding utf8

$bundleOut = Join-Path $stage "bundle.txt"
& $py (Join-Path $repoRoot "tools\\audit_bundle.py") --out $bundleOut

$zipPath = Join-Path $repoRoot ("_AUDIT_" + $ts + ".zip")
Compress-Archive -Path (Join-Path $stage "*") -DestinationPath $zipPath -Force

if (Get-Command Set-Clipboard -ErrorAction SilentlyContinue) {
  Set-Clipboard -Value $zipPath
}

Write-Host ("Wrote " + $zipPath)
