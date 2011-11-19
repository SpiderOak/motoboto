#!/bin/bash
CODEBASE="${HOME}/motoboto"
export PYTHONPATH="${CODEBASE}"

pushd "${CODEBASE}/docs"
make html
popd
