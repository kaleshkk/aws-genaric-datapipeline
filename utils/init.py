import sys
import json
import os
from pathlib import Path


def Init(args):
    pipelinePath = Path(f"./pipelines/{args.pipeline}/config.json")
    if pipelinePath.is_file():
        print("Pipeline is already existing. Please create pipeline with a unique name")
        print(args.template)
        sys.exit(0)
    else:
        # Opening JSON template file
        with open(f"templates/{args.template}/config.json") as f:
            # returns JSON object as
            configData = json.load(f)

        configData["pipeline"] = args.pipeline
        filePath = f"./pipelines/{args.pipeline}"

        ## create new folder, if not exist
        if not os.path.exists(filePath):
            os.mkdir(filePath)

        ## writing sample file into pipelines directory
        with open(os.path.join(filePath + "/config.json"), "w") as f:
            json.dump(configData, f, ensure_ascii=False, indent=4)
            print(f"Sample config file created in /pipelines/{args.pipeline}")

        sys.exit(0)
