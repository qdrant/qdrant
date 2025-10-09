# Python Bindings for Qdrant Edge

Setup environment

```bash
python -m venv .venv
source .venv/bin/activate

pip install --user maturin
```

Build and install the package

```bash
cd lib/edge/python
maturin develop --no-default-features
```

Run example

```bash
cd examples
mkdir shard

python qdrant-edge.py
```
