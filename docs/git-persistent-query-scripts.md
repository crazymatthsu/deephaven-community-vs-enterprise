# Deploying Persistent Query Scripts from Git in Deephaven Enterprise

This document describes the recommended strategy for managing Persistent Query (PQ) Python (`.py`) and Groovy (`.groovy`) scripts using Deephaven Enterprise’s built-in Git integration. It covers both **EKS / Kubernetes** deployments and **Podman / on-prem** deployments.

Deephaven Enterprise acts as a **consumer** of Git repositories. The Persistent Query Controller clones or pulls the repository and reads the script file when a PQ starts or when the script is viewed/edited in the UI. Scripts cannot be edited inside the Deephaven UI when Git is the source — all changes happen in your normal Git workflow.

## Overview of Supported Script Sources

| Source | Description | Best For |
|--------|-------------|----------|
| **Inline** | Script content stored directly in the PQ configuration (etcd) | Small / one-off queries |
| **Git repository** | Controller pulls via SSH and reads the file | Version-controlled query logic (recommended) |
| **Local repository** | Plain directory on the filesystem (or shared volume) | External pipeline drops files onto a volume |

There is **no native support** for pulling scripts directly from JFrog/Artifactory or other artifact repositories. Use Git (or a local volume populated by CI) as the source of truth.

## Where to Save the `.py` Files

Store the scripts in a Git repository that the Deephaven Controller can reach via SSH.

- Organize them under a path you will configure with `iris.scripts.repo.<repo_name>.paths` (e.g., `queries/python/`, `IrisQueries/python/`).
- Only files under the configured `paths` are visible to Deephaven.
- Use clear, stable filenames. The path relative to the repo root becomes the selectable script path in the UI.

Example repository layout:
```
my-deephaven-queries/
├── queries/
│   └── python/
│       ├── equity_volume.py
│       └── risk_summary.py
└── README.md
```

## Global and Per-Repository Configuration

Configure these properties with `dhconfig properties` (export → edit → import) or the equivalent configuration management process. After changing properties, validate with:

```bash
dhconfig pq check-repo
```

Then restart the Controller (non-leaders first in multi-controller setups).

### Global Properties

- `iris.scripts.repos` — comma-separated list of repository names (e.g., `myqueries,shared`).
- `PersistentQueryController.scriptUpdateEnabled=true` — enables once-per-minute rescan of all configured repositories (both Git and local). Required for automatic discovery of new/removed files when combined with per-repo `updateEnabled`.
- `PersistentQueryController.useLocalGit` — affects the default of `scriptUpdateEnabled`. Explicitly set `scriptUpdateEnabled` instead of relying on the default.

### Per-Repository Properties (`iris.scripts.repo.<repo_name>.*`)

| Property | Purpose | Notes |
|----------|---------|-------|
| `uri` | SSH URI of the Git repo (e.g., `ssh://git@github.com/org/my-queries.git` or `git@github.com:org/my-queries.git`) | Leave empty for a **local repository** |
| `paths` | Comma-separated paths relative to repo root to expose (e.g., `queries/python`) | Only these paths are visible |
| `groups` | Deephaven groups allowed to access the repo | Control visibility via ACLs |
| `branch` | Branch to track | Defaults to `master` or `PersistentQueryController.defaultBranch` |
| `root` | Local directory where the repo is cloned (or where files live for local repos) | Must be unique per repo; use a persistent location |
| `updateEnabled` | `true` to periodically refresh (git pull + rescan) | Requires global `scriptUpdateEnabled=true` |
| `prefixDisplayPathsWithRepoName` | `true` to show repo name in the UI dropdown | Helpful with multiple repos |

**Important behavior**:
- Script **content** is always read fresh from disk when a PQ starts or the UI requests it.
- The periodic refresh mainly keeps the **list of available scripts** up to date and pulls new commits.
- A running PQ does **not** automatically pick up script changes. You must **restart** the PQ (or let a scheduled restart occur).

## EKS / Kubernetes Configuration

1. **Create an SSH key pair and `known_hosts`** outside the cluster (or from a workstation that can reach the Git server):
   ```bash
   ssh-keygen -t ed25519 -f id_ed25519 -N ""   # or id_rsa
   ssh-keyscan -t rsa,ed25519 github.com >> known_hosts   # adjust host/port as needed
   ```
   Verify fingerprints with your Git administrator.

2. **Create a Kubernetes Secret** containing the private key (named `id_ed25519` or `id_rsa`) and `known_hosts`.

3. **Helm values** (override file):
   ```yaml
   controller:
     gitCreateVolume: true
     gitSshSecretName: "my-git-ssh-secret"
     # optional: gitMountPath: /db/TempFiles/irisadmin/git
   ```

4. Set `iris.scripts.repo.<repo_name>.root` to a path on the persistent Git volume (default mount is `/db/TempFiles/irisadmin/git`). Prefer a path that survives pod restarts (avoid the controller workspace `emptyDir`). Example relative path: `../git/myqueries`.

5. Add the public key to the Git server (GitHub deploy key, GitLab, etc.) with **read-only** access.

6. After Helm upgrade, test SSH from a controller pod:
   ```bash
kubectl exec -it <controller-pod> -- ssh -T -o BatchMode=yes git@github.com
   ```

The chart mounts the secret read-only at `/db/TempFiles/irisadmin/.ssh`.

## Podman / On-Prem Configuration

The logical configuration is identical. Differences are mainly around how you provision the SSH credentials and persistent storage.

1. **SSH credentials for `irisadmin`**:
   - On a traditional or Podman deployment, the controller runs as the `irisadmin` user.
   - Create the key pair and `known_hosts` under the `irisadmin` home (typically `/db/TempFiles/irisadmin/.ssh`).
   - In Podman, you can `podman exec` into the infra/controller container (or use a persistent volume mount under `VOLUME_BASE_DIR`) to perform the setup, or mount a host directory that already contains the `.ssh` material.
   - Ensure the private key has no passphrase and appropriate permissions (`600`).

2. **Persistent clone location**:
   - Set `iris.scripts.repo.<repo_name>.root` to a directory that survives container/pod restarts. In Podman this is usually under a host-mounted volume derived from `VOLUME_BASE_DIR`.
   - Avoid pure in-container paths that are lost on restart.

3. Configure the same properties (`iris.scripts.repos`, per-repo `uri`, `paths`, `updateEnabled`, etc.) via `dhconfig properties`.

4. Validate with `dhconfig pq check-repo` and restart the controller process.

**Tip**: For multi-node Podman clusters, ensure the Git clone root (or a local-repo directory) is on shared storage if multiple controllers need to see the same files, or let each controller manage its own clone.

## Using Git Scripts in the UI

1. Open the Query Monitor → New or Edit a Persistent Query.
2. On the **Script** tab:
   - Choose Runtime = Python (or Groovy).
   - Check **Use GIT for source**.
   - Select the script from the dropdown (populated from the configured repositories and paths).
3. The script editor becomes read-only.
4. Configure other settings (heap, server, ACLs, etc.) and **Save**.

## Refresh / Reload After Changing Scripts

| Action | What Happens | What You Need to Do |
|--------|--------------|---------------------|
| Push new commit to the tracked branch | Controller will pull on next refresh cycle (if `updateEnabled=true`) or on next controller restart | Nothing for the file list; restart the PQ to run the new code |
| Edit an existing file that the controller already knows about | Content is read fresh on next PQ start / UI view | Restart the PQ |
| Add a brand-new `.py` file | Appears in the dropdown after the next rescan (requires `scriptUpdateEnabled` + `updateEnabled`) or controller restart | Select it in a new/edited PQ and start |
| Delete a file | Disappears from the list after rescan | Existing PQs that referenced it will fail on next start |

**Critical**: Date-sensitive logic (`today()`, `previousBusinessDay()`, partition formulas evaluated at init, etc.) is captured once when the script runs. Even if the file on disk changes, a long-running PQ keeps the old values until it is restarted.

You can force a controller rescan/restart or use `dhconfig pq` commands to stop/restart individual queries.

## Scheduling Persistent Queries

Configure the schedule on the **Scheduling** tab of the PQ editor (or programmatically via `PersistentQueryConfigMessage`).

Common options:

- **Continuous** — runs indefinitely (Live Queries). Optional daily restart time.
- **Daily** — start/stop times, days of week or business days, timezone, overnight ranges. Option to always restart or skip if already running.
- **Monthly** — similar, with month/day selection.
- **Dependent** — run after one or more other queries succeed/fail.
- **Range** — specific start and stop date/time.
- **Disabled** — manual start/stop only.
- **Temporary** — for batch-style queries that are cleaned up.

Additional controls:
- Initialization timeout / maximum run time.
- Error restart attempts and delay.
- Queue (default vs temporary).
- For date-partitioned data, schedule a daily restart at a quiet time after the previous day’s data is complete.

After saving, the controller starts/stops the PQ according to the schedule. You can also start/stop/restart manually from the Query Monitor or via `dhconfig pq`.

## Modularizing Scripts

You can import other scripts from the same Git (or local) repository as modules:

```python
from deephaven_enterprise.controller_import import ...
# or
from controller.queries.python.helpers import some_function
```

See the official “Modularizing Queries (Python)” documentation for exact import syntax and path mapping based on your `paths` setting.

## Best Practices

- Treat the Git repository as the source of truth for query logic.
- Use CI to test scripts (unit tests or `run_local_script` against a non-production cluster) before merging.
- Restart PQs (or rely on scheduled daily restarts) after script changes, especially those involving date functions.
- Keep the deploy key read-only.
- Prefer the persistent Git volume on EKS and host-mounted volumes on Podman so clones survive restarts.
- Export PQ configurations (`dhconfig pq export`) and keep the XML under version control if you want the query definitions themselves to be infrastructure-as-code.
- Avoid storing large binary dependencies only inside scripts — manage Python packages via custom worker images or `venv.install` when appropriate.

## References

- Persistent Query Controller – Script Repository Configuration
- Query Monitor / UI Query Management
- Kubernetes troubleshooting – Git SSH credentials
- Modularizing Queries (Python)
- Managing Persistent Queries (scheduling and daily restarts)
