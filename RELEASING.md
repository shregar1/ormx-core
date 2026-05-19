# Releasing `ormx-core`

`ormx-core` is the Rust crate that powers `ormx`. It ships to PyPI as
a standalone wheel (`ormx_core`) and is also vendored into the `ormx`
PyPI package via maturin's sibling-checkout build. Both packages
publish on every `v*` tag push.

Releases are automated by `.github/workflows/release.yml`.

## Prerequisites (one-time)

### 1. Configure PyPI trusted publishing

1. Create the `ormx-core` project on PyPI before the first release
   (manual bootstrap with `maturin publish` + a PyPI token, then
   delete the token).

2. PyPI project's **Settings → Publishing** → "Add a new publisher":

   * **PyPI Project Name:** `ormx-core`
   * **Owner:** `shregar1`
   * **Repository:** `ormx-core`
   * **Workflow:** `release.yml`
   * **Environment:** `pypi`

3. GitHub repo **Settings → Environments** → create `pypi`. Optional
   but recommended: add a "Required reviewers" rule.

### 2. crates.io publication (optional)

`ormx-core` is also a regular Rust crate. If you want it on
crates.io:

```bash
# One-time login (token from https://crates.io/me)
cargo login

# From a clean main checkout at the right tag:
cargo publish
```

Not automated in the release workflow today — crates.io doesn't
support trusted publishing yet (as of 2024). Wire it up by hand when
needed, or add a step using the `CARGO_REGISTRY_TOKEN` secret.

## Cutting a release

1. **Bump the version** in `Cargo.toml` (`[package] version`). The
   Python sibling `ormx-python` pins to a specific tag via its
   `ORMX_CORE_REF` env var — coordinate so both repos can release
   together.

   * `0.2.1` for bugfix-only
   * `0.3.0` for new public API surface (additive)
   * `1.0.0` when we're ready to commit to stable semver

2. **Commit + push** the bump on `main`:

   ```bash
   git commit -am "chore: bump version to 0.X.Y"
   git push origin main
   ```

3. **Annotate + push the tag** — the annotation message becomes the
   GitHub Release notes:

   ```bash
   git tag -a v0.X.Y -m "ormx-core v0.X.Y — short summary

   API additions
   -------------
   * Bullet

   Pairs with ormx vM.N.O."

   git push origin v0.X.Y
   ```

4. **Watch the workflow** at
   `https://github.com/shregar1/ormx-core/actions`. Five wheel jobs
   run in parallel (~5 min total); sdist runs in ~2 min; publish and
   GitHub-release jobs run after.

5. **Verify** by installing from PyPI:

   ```bash
   python -m venv /tmp/verify
   source /tmp/verify/bin/activate
   pip install ormx-core==0.X.Y
   python -c "import ormx_core; print(dir(ormx_core.Engine))"
   ```

## Coordinating with `ormx-python`

The user-facing flow:

1. Release `ormx-core` first (bump version, tag, push).
2. Wait for ormx-core's release workflow to finish (wheel + sdist on
   PyPI).
3. Bump `ormx-python/.github/workflows/release.yml`'s
   `ORMX_CORE_REF` env var to the new tag.
4. Bump `ormx-python`'s version, tag, push.

The order matters: if you tag ormx-python first, its release workflow
will try to checkout `ormx-core` at the new ref — and fail because
that tag doesn't exist yet.

## What gets built

For each release the workflow produces (per `Cargo.toml`'s `name`):

| File | Platform |
|---|---|
| `ormx_core-X.Y.Z-cp37-abi3-manylinux_2_17_x86_64.whl` | Linux x86_64 |
| `ormx_core-X.Y.Z-cp37-abi3-manylinux_2_17_aarch64.whl` | Linux ARM |
| `ormx_core-X.Y.Z-cp37-abi3-macosx_10_12_x86_64.whl` | macOS Intel |
| `ormx_core-X.Y.Z-cp37-abi3-macosx_11_0_arm64.whl` | macOS Apple Silicon |
| `ormx_core-X.Y.Z-cp37-abi3-win_amd64.whl` | Windows x86_64 |
| `ormx_core-X.Y.Z.tar.gz` | Source distribution |

## Yanking a bad release

Same flow as ormx-python — yank from PyPI, fix forward with
`0.X.Y+1`. Don't try to re-upload the same version.

If a bad ormx-core release got pulled into a published ormx-python
build, yank that too (the wheel bundles ormx-core's compiled code,
so the bug is in the user's installed ormx package).
