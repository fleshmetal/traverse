import re
import traverse as tm

def test_version_semver():
    assert isinstance(tm.__version__, str)
    assert re.match(r"^\d+\.\d+\.\d+$", tm.__version__)