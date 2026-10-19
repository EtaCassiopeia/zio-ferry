"""Writes every animation to docs/assets. Run from the repository root: python3 scripts/visuals/build.py"""

import importlib
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

SCENES = {
    "ferry": "ferry",
    "single-queue": "single_queue",
    "lanes": "lanes",
    "ordering": "ordering",
    "fold-write": "fold_write",
    "fleet": "fleet",
    "projected-write": "projected_write",
    "projected-rows": "projected_rows",
}

if __name__ == "__main__":
    out = os.path.join(HERE, "..", "..", "docs", "assets")
    wanted = sys.argv[1:] or list(SCENES)
    for name in wanted:
        scene = importlib.import_module(SCENES[name]).build()
        scene.write(os.path.join(out, f"{name}.svg"))
        print(f"docs/assets/{name}.svg")
