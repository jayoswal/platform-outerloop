# Backend service template

The four initial services are generated from the same FastAPI shape. Keep
configuration, auth, event transport, tests, linting, migrations, and
container entrypoints structurally identical. Domain routers and consumers
are the intended variation points.

Generate another service from the platform repository:

```bash
uv run --group dev cookiecutter templates/service
```
