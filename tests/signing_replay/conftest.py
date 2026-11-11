"""Local config for the signing-replay package.

The repository-wide ``tests/conftest.py`` pulls in plugins (notably
pytest-xdist) that are optional in some environments. This package only needs
the pytest-aiohttp server/client fixtures, which register automatically via
entry points, so the tests stay runnable in restricted sandboxes using::

    pytest tests/signing_replay --confcutdir=tests/signing_replay \
        -o addopts=""
"""
