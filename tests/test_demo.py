import contextlib
import importlib.util
import io
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


class DemoTests(unittest.TestCase):
    def test_demo_runs_and_shows_expected_behaviour(self):
        spec = importlib.util.spec_from_file_location("run_demo", os.path.join(HERE, "..", "demo", "run_demo.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            mod.main()
        out = buf.getvalue()
        self.assertIn("Demo 1a", out)
        self.assertIn("Demo 2", out)
        self.assertIn("NOT loading", out)          # ETL blocked on bad data
        self.assertIn("loading 8 rows", out)       # ETL continues on clean data
        self.assertIn("status=DRY_RUN can_proceed=False", out)


if __name__ == "__main__":
    unittest.main()
