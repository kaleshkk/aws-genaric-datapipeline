import sys
import json
import os
import copy


def Builder(args):
    filePath = f"./pipelines/{args.pipeline}/CloudFormation"

    if os.path.exists(filePath):
        print("Resources are already existing. terminating activities")
        sys.exit(0)
    else:
        print(f"Rendering CloudFormation template for {args.pipeline}")
        # Opening JSON template file
        # Opening JSON template file
        with open(f"pipelines/{args.pipeline}//config.json") as config:
            # returns JSON object as
            configData = json.load(config)

            with open(f"templates/{args.template}/CloudFormation/cloudformation.json") as f:
                # returns JSON object as
                cloudFormationData = json.load(f)

                ## updating on-demand trigger
                cloudFormationData["Resources"]["GlueTriggerRawJob"]["Properties"]["Description"] = \
                    f"customer_demand_sap_raw on-demand trigger for {args.pipeline}"
                cloudFormationData["Resources"]["GlueTriggerRawJob"]["Properties"]["Name"] = \
                    f"tr_ondemand_{args.pipeline.lower()}"
                ## updating incremental trigger
                cloudFormationData["Resources"]["GlueTriggerRawJobIncr"]["Properties"]["Description"] = \
                    f"customer_demand_sap_raw incremental trigger for {args.pipeline}"
                cloudFormationData["Resources"]["GlueTriggerRawJobIncr"]["Properties"]["Schedule"] = \
                    configData["projectConfig"]["schedule"]
                cloudFormationData["Resources"]["GlueTriggerRawJobIncr"]["Properties"]["Name"] = \
                    f"tr_incr_{args.pipeline.lower()}"
                ## updating athena incremental table
                cloudFormationData["Resources"]["AthenaETLTable"]["Properties"]["TableInput"]["Description"] = \
                    f"Glue catalog for {args.pipeline}"
                cloudFormationData["Resources"]["AthenaETLTable"]["Properties"]["TableInput"]["Name"] = \
                    configData["catalog"]["athenaIncrementalTable"]
                cloudFormationData["Resources"]["AthenaETLTable"]["Properties"]["TableInput"]["StorageDescriptor"][
                    "Columns"] = \
                    configData["catalog"]["schema"]

                ## updating athena compacted table
                cloudFormationData["Resources"]["AthenaCompactedTable"]["Properties"]["TableInput"]["Description"] = \
                    f"Glue catalog for {args.pipeline}"
                cloudFormationData["Resources"]["AthenaCompactedTable"]["Properties"]["TableInput"]["Name"] = \
                    configData["catalog"]["athenaCompactedTable"]
                cloudFormationData["Resources"]["AthenaCompactedTable"]["Properties"]["TableInput"][
                    "StorageDescriptor"][
                    "Columns"] = \
                    configData["catalog"]["schema"]

                ## updating athena compacted table
                cloudFormationData["Resources"]["AthenaCompactedPIITable"]["Properties"]["TableInput"]["Description"] = \
                    f"Glue catalog for {args.pipeline}"
                cloudFormationData["Resources"]["AthenaCompactedPIITable"]["Properties"]["TableInput"]["Name"] = \
                    configData["catalog"]["athenaCompactedTable"]
                cloudFormationData["Resources"]["AthenaCompactedPIITable"]["Properties"]["TableInput"][
                    "StorageDescriptor"][
                    "Columns"] = \
                    configData["catalog"]["schema"]

                if "PartitionKeys" in configData["catalog"]:
                    cloudFormationData["Resources"]["AthenaETLTable"]["Properties"]["TableInput"]["PartitionKeys"] = \
                        configData["catalog"]["PartitionKeys"]
                    cloudFormationData["Resources"]["AthenaCompactedTable"]["Properties"]["TableInput"][
                        "PartitionKeys"] = \
                        configData["catalog"]["PartitionKeys"]
                    cloudFormationData["Resources"]["AthenaCompactedPIITable"]["Properties"]["TableInput"][
                        "PartitionKeys"] = \
                        configData["catalog"]["PartitionKeys"]

                ## creating cloudformation folders
                os.mkdir(filePath)
                os.mkdir(filePath + "/Config")

                ## writing sample file into pipelines directory
                with open(
                        os.path.join(f"pipelines/{args.pipeline}/CloudFormation/cloudformation.json"),
                        "w") as f:
                    json.dump(cloudFormationData, f, ensure_ascii=False, indent=4)
                    print(f"CloudFormation template is build in to /pipelines/{args.pipeline}")

                buildConfigFiles(args, configData)
                updateCodePipelineStacks(args, "dev")
                updateCodePipelineStacks(args, "test")
                updateCodePipelineStacks(args, "prod")


def buildConfigFiles(args, configData):
    with open(f"templates/{args.template}/CloudFormation/Config/params.json") as config:
        # returns JSON object as
        envConfig = json.load(config)

        envs = ["dev", "test", "prod"]

        for env in envs:
            envConfig["Parameters"]["athenaDB"] = configData["catalog"][env]["athenaDatabase"]
            envConfig["Parameters"]["AthenaTableLocation"] = f"s3://{configData['preparedLayer'][env]['preparedBucket']}/Incremental{configData['rawLayer']['folder']}"
            envConfig["Parameters"]["athenaCompactedDB"] = configData["catalog"][env]["athenaCompactedDatabase"]
            envConfig["Parameters"]["AthenaTableCompactedLocation"] = f"s3://{configData['preparedLayer'][env]['preparedBucket']}/Compacted{configData['rawLayer']['folder']}"

            ## writing sample file into pipelines directory
            with open(
                    os.path.join(f"pipelines/{args.pipeline}/CloudFormation/Config/params.{env}.json"),
                    "w") as f:
                json.dump(envConfig, f, ensure_ascii=False, indent=4)
                print(f"CloudFormation config for {env} is created in to /pipelines/{args.pipeline}")


def updateCodePipelineStacks(args, env):
    print(f"updating {env} codepipeline for {args.pipeline}...")

    with open(f"code-pipelines/datahub-aws-customer-demand-codepipeline-{env}.json") as config:
        # returns JSON object as
        codePipeline = json.load(config)
        listOfActions = codePipeline["Resources"]["DatahubAwsCD"]["Properties"]["Stages"][2]["Actions"]
        nameOfAction = f"DLake-Customer-Demand-{args.pipeline}-Pipeline"
        newAction = copy.deepcopy(listOfActions[1])
        newAction["Name"] = nameOfAction
        newAction["Configuration"]["StackName"] = f"DLake-Customer-Demand-{args.pipeline}-Pipeline"
        newAction["Configuration"]["TemplateConfiguration"] = f"SourceArtifact::pipelines/{args.pipeline}/CloudFormation/Config/params.{env}.json"
        newAction["Configuration"]["TemplatePath"] = f"SourceArtifact::pipelines/{args.pipeline}/CloudFormation/cloudformation.json"

        ## check config already existing or not
        actionIndex = next((index for (index, action) in enumerate(listOfActions) if action["Name"] == nameOfAction), None)

        if actionIndex is None:
            print("New action detected..")
            listOfActions.append(newAction)
        else:
            print("Action existing on code-pipeline and updating")
            del listOfActions[actionIndex]
            listOfActions.insert(actionIndex, newAction)

        codePipeline["Resources"]["DatahubAwsCD"]["Properties"]["Stages"][2]["Actions"] = listOfActions



        with open(os.path.join(f"code-pipelines/datahub-aws-customer-demand-codepipeline-{env}.json"), "w") as f:
            json.dump(codePipeline, f, ensure_ascii=False, indent=4)
            print(f"Code-pipeline for {env} updated with {args.pipeline} Action..")