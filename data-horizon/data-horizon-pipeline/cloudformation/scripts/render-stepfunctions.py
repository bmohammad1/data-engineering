#!/usr/bin/env python3
"""
Render the Step Functions CloudFormation template by inlining the ASL JSON
files from ../statemachine/ into DefinitionString blocks with Fn::Sub
substitution variables.

Terraform uses templatefile() with ${var_name} placeholders; the same ASL
files are reused unchanged here because Fn::Sub uses the same ${name} syntax.

Output: nested/stepfunctions.yaml (overwritten on each run).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


# Mapping: ASL file name -> (CFN logical resource ID, state machine logical name suffix, Fn::Sub variables to expose).
# Variable names MUST match the ${...} placeholders in the .asl.json files.
STATE_MACHINES = [
    {
        "asl_file": "config_loader.asl.json",
        "logical_id": "ConfigLoaderStateMachine",
        "name_suffix": "config-loader",
        "sub_vars": [
            "orchestrator_lambda_arn",
        ],
    },
    {
        "asl_file": "data_extractor.asl.json",
        "logical_id": "DataExtractorStateMachine",
        "name_suffix": "data-extractor",
        "sub_vars": [
            "map_state_processor_lambda_arn",
            "orchestration_bucket_name",
            "extraction_failures_queue_url",
            "pipeline_state_table",
        ],
    },
    {
        "asl_file": "transformation.asl.json",
        "logical_id": "TransformationStateMachine",
        "name_suffix": "transformation",
        "sub_vars": [
            "transform_glue_job_name",
            "validation_glue_job_name",
            "sns_topic_arn",
        ],
    },
    {
        "asl_file": "redshift_load.asl.json",
        "logical_id": "RedshiftLoadStateMachine",
        "name_suffix": "redshift-load",
        "sub_vars": [
            "redshift_cluster_id",
            "redshift_database",
            "redshift_master_username",
            "validated_bucket_name",
            "redshift_iam_role_arn",
            "pipeline_state_table",
        ],
    },
    {
        "asl_file": "modular_orchestrator.asl.json",
        "logical_id": "ModularOrchestratorStateMachine",
        "name_suffix": "modular-orchestrator",
        "sub_vars": [
            "config_loader_arn",
            "data_extractor_arn",
            "transformation_arn",
            "redshift_load_arn",
            "sns_topic_arn",
        ],
    },
]


PARAM_TO_SUBVAR = {
    "orchestrator_lambda_arn":         "ConfigLoaderLambdaArn",
    "map_state_processor_lambda_arn":  "MapStateProcessorLambdaArn",
    "orchestration_bucket_name":       "OrchestrationBucketName",
    "extraction_failures_queue_url":   "ExtractionFailuresQueueUrl",
    "pipeline_state_table":            "PipelineStateTable",
    "transform_glue_job_name":         "TransformGlueJobName",
    "validation_glue_job_name":        "ValidationGlueJobName",
    "sns_topic_arn":                   "SnsTopicArn",
    "redshift_cluster_id":             "RedshiftClusterId",
    "redshift_database":               "RedshiftDatabase",
    "redshift_master_username":        "RedshiftMasterUsername",
    "validated_bucket_name":           "ValidatedBucketName",
    "redshift_iam_role_arn":           "RedshiftIamRoleArn",
    "config_loader_arn":               "ConfigLoaderStateMachineArn",
    "data_extractor_arn":              "DataExtractorStateMachineArn",
    "transformation_arn":              "TransformationStateMachineArn",
    "redshift_load_arn":               "RedshiftLoadStateMachineArn",
}

# These ASL vars resolve to sibling state-machine resources in this same
# nested template — they are NOT external CFN parameters. The rendered
# DefinitionSubstitutions block must !Ref the resource (not a parameter).
INTERNAL_SUBVAR_TO_RESOURCE = {
    "config_loader_arn":   "ConfigLoaderStateMachine",
    "data_extractor_arn":  "DataExtractorStateMachine",
    "transformation_arn":  "TransformationStateMachine",
    "redshift_load_arn":   "RedshiftLoadStateMachine",
}


def all_sub_vars() -> set[str]:
    """All terraform var names that any ASL file references, used to declare CFN parameters."""
    s: set[str] = set()
    for sm in STATE_MACHINES:
        s.update(sm["sub_vars"])
    return s


def indent_block(text: str, spaces: int) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line else "" for line in text.splitlines())


def rewrite_asl_placeholders(asl: str, sub_vars: list[str]) -> str:
    """
    Rewrite the ASL JSON so that ${terraform_var} becomes ${CfnParamName}.
    Any literal ${...} that is NOT in our mapping is escaped as ${!...} so
    CFN's Fn::Sub leaves it alone (e.g. Step Functions JSONPath expressions).
    """
    import re

    def replace(match: "re.Match[str]") -> str:
        name = match.group(1)
        if name in PARAM_TO_SUBVAR and name in sub_vars:
            return "${" + PARAM_TO_SUBVAR[name] + "}"
        # Escape any other ${...} so Fn::Sub doesn't interpret it.
        return "${!" + name + "}"

    return re.sub(r"\$\{([^}]+)\}", replace, asl)


def render() -> str:
    repo_root = Path(__file__).resolve().parents[3]
    asl_dir = repo_root / "data-horizon-pipeline" / "statemachine"

    if not asl_dir.is_dir():
        sys.exit(f"ASL directory not found: {asl_dir}")

    lines: list[str] = [
        "AWSTemplateFormatVersion: '2010-09-09'",
        "Description: Data Horizon — 5 Step Function state machines (generated by render-stepfunctions.py).",
        "",
        "Parameters:",
        "  NamePrefix:",
        "    Type: String",
        "  StepFunctionsRoleArn:",
        "    Type: String",
    ]

    # Declare CFN parameters for every external substitution variable.
    # Internal vars (child state-machine ARNs) are NOT parameters — they
    # resolve to resources in this same template.
    for tf_var in sorted(all_sub_vars()):
        if tf_var in INTERNAL_SUBVAR_TO_RESOURCE:
            continue
        cfn_param = PARAM_TO_SUBVAR[tf_var]
        lines.append(f"  {cfn_param}:")
        lines.append("    Type: String")

    lines.append("")
    lines.append("Resources:")
    lines.append("")

    for sm in STATE_MACHINES:
        asl_path = asl_dir / sm["asl_file"]
        if not asl_path.is_file():
            sys.exit(f"ASL file missing: {asl_path}")

        raw = asl_path.read_text(encoding="utf-8")
        # Validate it is JSON so we fail fast on bad input.
        json.loads(raw)
        rewritten = rewrite_asl_placeholders(raw, sm["sub_vars"])

        # Build the Fn::Sub variable map for this state machine. Internal
        # vars reference sibling state-machine resources; external vars
        # reference CFN parameters.
        var_map_lines: list[str] = []
        for tf_var in sm["sub_vars"]:
            cfn_param = PARAM_TO_SUBVAR[tf_var]
            ref_target = INTERNAL_SUBVAR_TO_RESOURCE.get(tf_var, cfn_param)
            var_map_lines.append(f"            {cfn_param}: !Ref {ref_target}")

        lines.append(f"  {sm['logical_id']}:")
        lines.append("    Type: AWS::StepFunctions::StateMachine")

        # State machines that reference sibling state machines must wait for
        # them to exist first.
        internal_deps = [
            INTERNAL_SUBVAR_TO_RESOURCE[v]
            for v in sm["sub_vars"]
            if v in INTERNAL_SUBVAR_TO_RESOURCE
        ]
        if internal_deps:
            lines.append(f"    DependsOn: [{', '.join(internal_deps)}]")

        lines.append("    Properties:")
        lines.append(f"      StateMachineName: !Sub ${{NamePrefix}}-{sm['name_suffix']}")
        lines.append("      RoleArn: !Ref StepFunctionsRoleArn")
        lines.append("      DefinitionString:")
        lines.append("        Fn::Sub:")
        lines.append("          - |")
        lines.append(indent_block(rewritten, 12))
        if var_map_lines:
            lines.append("          - " + var_map_lines[0].lstrip())
            for v in var_map_lines[1:]:
                lines.append(v)
        else:
            lines.append("          - {}")
        lines.append("      Tags:")
        lines.append(f"        - {{ Key: Name, Value: !Sub \"${{NamePrefix}}-{sm['name_suffix']}\" }}")
        lines.append("")

    lines.append("Outputs:")
    lines.append("  ConfigLoaderStateMachineArn:")
    lines.append("    Value: !Ref ConfigLoaderStateMachine")
    lines.append("  DataExtractorStateMachineArn:")
    lines.append("    Value: !Ref DataExtractorStateMachine")
    lines.append("  TransformationStateMachineArn:")
    lines.append("    Value: !Ref TransformationStateMachine")
    lines.append("  RedshiftLoadStateMachineArn:")
    lines.append("    Value: !Ref RedshiftLoadStateMachine")
    lines.append("  ModularOrchestratorStateMachineArn:")
    lines.append("    Value: !Ref ModularOrchestratorStateMachine")
    lines.append("    Description: Parent state machine, target of the EventBridge schedule rule.")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    out = Path(__file__).resolve().parent.parent / "nested" / "stepfunctions.yaml"
    out.write_text(render(), encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
