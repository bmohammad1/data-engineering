"""Root conftest — stubs awsglue into sys.modules so Glue job scripts can be
imported in a standard Python environment without the AWS Glue runtime."""

import sys
import types


def _stub_module(name: str) -> types.ModuleType:
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    return mod


# Stub awsglue and every submodule the Glue scripts import.
_awsglue = _stub_module("awsglue")
_awsglue_utils = _stub_module("awsglue.utils")
_awsglue_context = _stub_module("awsglue.context")
_awsglue_job = _stub_module("awsglue.job")
_awsglue_dynamicframe = _stub_module("awsglue.dynamicframe")

# getResolvedOptions is called at module level by the Glue scripts — provide a
# stub so the import succeeds. Tests patch it to return their own GLUE_ARGS.
_awsglue_utils.getResolvedOptions = lambda argv, keys: {k: "" for k in keys}

_awsglue_context.GlueContext = object
_awsglue_job.Job = object
_awsglue_dynamicframe.DynamicFrame = object

_awsglue.utils = _awsglue_utils
_awsglue.context = _awsglue_context
_awsglue.job = _awsglue_job
_awsglue.dynamicframe = _awsglue_dynamicframe

# Pre-import the Glue job modules so they are in sys.modules before any test
# calls patch("glue_jobs.scripts.transform_job.*"). unittest.mock.patch resolves
# the dotted target via getattr on the package, which requires the module to
# have already been imported and bound as an attribute of glue_jobs.scripts.
import glue_jobs.scripts.transform_job  # noqa: E402, F401
import glue_jobs.scripts.validation_job  # noqa: E402, F401
