
class EmrOperatorsConfiguration:
    JOB_FLOW_OVERRIDES = {
        "Name": "NYC Citi bike trip data ETL",
        "ReleaseLabel": "emr-5.29.0",
        'LogUri': 's3://{{ params.s3_input_bucket_name }}/{{ params.s3_script_folder }}',
        "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
        "Configurations": [
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                    }
                ],
            }
        ],
        "Instances": {
            "InstanceGroups": [
                {
                    "Name": "Master node",
                    "Market": "SPOT",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m4.xlarge",
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core nodes",
                    "Market": "SPOT",  # Spot instances are a "use as available" instances
                    "InstanceRole": "CORE",
                    "InstanceType": "m4.xlarge",
                    "InstanceCount": 2,
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,  # this lets us programmatically terminate the cluster
        },
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "ServiceRole": "EMR_DefaultRole",
    }

    SPARK_STEPS = [
        {
            "Name": "Run etl script from s3",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "client",
                    #"--master",
                    #"yarn",
                    "--conf",
                    "spark.yarn.submit.waitAppCompletion=true",
                    "s3://{{ params.s3_input_bucket_name }}/{{ params.s3_script_folder }}/{{ params.s3_script }}",
                    "--input",
                    "s3://{{ params.s3_input_bucket_name }}/",
                    "--output",
                    "s3://{{ params.s3_input_bucket_name }}/{{ params.transformed_table }}/{{ params.year }}/"
                ],
            },
        }
    ]