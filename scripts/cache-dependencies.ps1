$ErrorActionPreference = 'Stop'
$pythonRuntime = Join-Path $PSScriptRoot '..\.venv\Scripts\python.exe'
& $pythonRuntime -m pip download -r "$PSScriptRoot/../backend/requirements.txt" --dest "$PSScriptRoot/../backend/wheels" --only-binary=:all: --platform manylinux_2_28_x86_64 --platform manylinux2014_x86_64 --python-version 3.12 --implementation cp --abi cp312
exit $LASTEXITCODE
