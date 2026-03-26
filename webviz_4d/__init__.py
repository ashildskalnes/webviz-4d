import sys

# Catch the missing pkg_resources and patch it dynamically
try:
    import pkg_resources
except ImportError:
    import importlib.metadata

    class _MockPkgResources:
        class DistributionNotFound(Exception):
            pass

        @staticmethod
        def get_distribution(package_name):
            from collections import namedtuple

            Distribution = namedtuple("Distribution", ["version"])
            try:
                version = importlib.metadata.version(package_name)
                return Distribution(version=version)
            except importlib.metadata.PackageNotFoundError:
                raise _MockPkgResources.DistributionNotFound()

    # Inject our mock into the system modules
    sys.modules["pkg_resources"] = _MockPkgResources()
