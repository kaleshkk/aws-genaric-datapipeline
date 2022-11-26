import argparse
import sys
import json
import os
from pathlib import Path
from utils.builder import Builder
from utils.init import Init


##getting arguments from cli
parser = argparse.ArgumentParser(description='Pipeline information')
parser.add_argument('--pipeline', dest='pipeline', type=str, help='Name of pipeline')
parser.add_argument('--template', dest='template', type=str, help="Template style: cds_view/odata")
parser.add_argument('--action', dest='action', type=str, help=""""Name of action: 1. init - init sample JSON config template
2. validate - Validate JSON config 3. build - CloudFormation template for pipeline""")

args = parser.parse_args()
predefinedActions = ["init", "validate", "build"]
predefinedTemplate = ["cds_view", "odata"]



if args.action not in predefinedActions:
    print("You entered wrong Action or not specified any action. Please use following action after --action")
    print("init - init sample JSON config template")
    print("validate - Validate JSON config")
    print("build - CloudFormation template for pipeline")
    sys.exit(0)

if args.template not in predefinedTemplate:
    print("You entered wrong template or not specified any template. Please use following template after --template")
    print("cds_view - For creating or managing CDS VIEW data pipelines")
    print("odata - For creating or managing Odata pipelines")
    sys.exit(0)

## init sample JSON config template
if args.action == 'init':
    Init(args)

## init sample JSON config template
if args.action == 'build':
    Builder(args)
