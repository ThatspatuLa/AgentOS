import unittest

import kcos_stage3_launch_v2 as launch


class Stage3SecretRedactionTests(unittest.TestCase):
    def test_redacts_worker_password_without_losing_local_identity(self):
        args = [
            "metatester64.exe",
            "/local",
            "/address:127.0.0.1:3001",
            "/password:sensitive-value",
        ]

        redacted = launch.redact_process_command(args)

        self.assertEqual(
            ["metatester64.exe", "/local", "/address:127.0.0.1:3001", "/password:[REDACTED]"],
            redacted,
        )
        self.assertNotIn("sensitive-value", " ".join(redacted))

    def test_redacts_case_insensitive_password_equals_argument(self):
        args = ["MetaTester64.exe", "-PASSWORD=sensitive-value", "/local"]

        redacted = launch.redact_process_command(args)

        self.assertEqual("-PASSWORD=[REDACTED]", redacted[1])
        self.assertNotIn("sensitive-value", " ".join(redacted))


if __name__ == "__main__":
    unittest.main()
