import boto3, json, datetime, os
import codecs
import boto3
from os.path import isfile
from botocore.exceptions import ClientError

emr = boto3.client('emr', region_name='us-east-1')


def lambda_handler(event, context):

    response = emr.run_job_flow(
    Name='consolida-detran',
    LogUri='s3://modelagem/omni-datalake/infrastructure/logs/emr/testeDetran',
    ReleaseLabel='emr-6.3.0',
    Instances={
        'MasterInstanceType': 'm5.xlarge',
        'SlaveInstanceType': 'm5.xlarge',
        'InstanceCount': 2,
        'EmrManagedSlaveSecurityGroup': 'sg-0d91939504dc11210',
        'EmrManagedMasterSecurityGroup': 'sg-0452edec6c990f2fb',
        'Ec2SubnetId': 'subnet-01c22397eb70ecd57',
        "ServiceAccessSecurityGroup": "sg-0225c2a4d6fa4fa87",
        'TerminationProtected': False
    },
	Applications=[
		{ "Name": "Hadoop" }, 
		{ "Name": "Spark" }, 
		{ "Name": "Hive" }, 
		{ "Name": "Ganglia" }
	
	],
	BootstrapActions=[
		{
			'Name': 'Install Python packages',
			'ScriptBootstrapAction': {
				'Path': 's3://modelagem/omni-datalake/jobs/python_libraries_detran.sh'
			}
		}
	],
	Steps=[
	{
		'Name': 'consolida',
		'ActionOnFailure': 'CONTINUE',
		'HadoopJarStep': {
			'Jar': 'command-runner.jar',
			'Args': ['spark-submit', '--py-files',
			    's3://modelagem/detran/detran.zip',
				's3://modelagem/detran/consolida_batch_alone.py',
                '-i', '1'
				]
			}
		}
		],
	VisibleToAllUsers=True,
	JobFlowRole='EMR_EC2_DefaultRole',
	ServiceRole='EMR_DefaultRole',
	Configurations=[
		{
			'Classification': 'spark',
			'Properties': {
				'maximizeResourceAllocation': 'true'
				}
			},
		],
	)

    return {
        'statusCode': 200,
        'body': json.dumps('Sucesso!')
    }
