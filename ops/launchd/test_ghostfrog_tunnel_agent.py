import os
from pathlib import Path
import plistlib
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ops.launchd import ghostfrog_tunnel_agent as agent


class LaunchAgentTests(unittest.TestCase):
    def test_manifest_is_login_scoped_and_uses_absolute_paths(self):
        home = Path("/test home A&B")
        config = plistlib.loads(plistlib.dumps(agent.agent_definition(home)))
        self.assertEqual(config["Label"], agent.LABEL)
        self.assertEqual(config["LimitLoadToSessionType"], "Aqua")
        self.assertTrue(config["RunAtLoad"])
        self.assertTrue(config["KeepAlive"])
        self.assertEqual(config["ThrottleInterval"], 60)
        self.assertEqual(config["Umask"], "077")
        self.assertTrue(all(Path(path).is_absolute() for path in config["ProgramArguments"][:2]))
        self.assertEqual(Path(config["StandardErrorPath"]).parent, agent.log_dir(home))
        self.assertNotIn("EnvironmentVariables", config)

    def test_existing_process_and_insecure_key_prevent_start(self):
        with TemporaryDirectory() as temp:
            home = Path(temp)
            agent.log_dir(home).mkdir(parents=True, mode=0o700)
            for relative in (".config/tunnel-client/ghostfrog.yaml", ".config/ghostfrog-mcp/platform-runtime-key", ".pgpass"):
                path = home / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch(mode=0o600)

            with patch.object(agent.os, "execve") as launch, patch.object(agent.subprocess, "run") as pgrep:
                pgrep.return_value.returncode = 0
                with self.assertRaisesRegex(SystemExit, "already running"):
                    agent.run_tunnel(home)
                launch.assert_not_called()

                key = home / ".config/ghostfrog-mcp/platform-runtime-key"
                key.chmod(0o644)
                with self.assertRaisesRegex(SystemExit, "private, user-owned"):
                    agent.run_tunnel(home)
                launch.assert_not_called()

                key.chmod(0o600)
                pgrep.return_value.returncode = 1
                with patch.dict(os.environ, {"CONTROL_PLANE_API_KEY": "test-value", "PGPASSWORD": "test-value"}):
                    agent.run_tunnel(home)
                client, args, environment = launch.call_args.args
                self.assertEqual(args[:4], [str(client), "run", "--profile", "ghostfrog"])
                self.assertIn("--log.file=", args)
                self.assertIn("--log.http-raw-unsafe=false", args)
                self.assertIn("--harpoon.capture-payloads=false", args)
                self.assertIn("--tracing.enabled=false", args)
                self.assertEqual(set(environment), {"HOME", "PATH"})
