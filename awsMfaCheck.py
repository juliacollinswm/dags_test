import boto3
from datetime import datetime

import airflow.utils
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import DAG

def mfaNotEnabled_check():
    client                  = boto3.client('iam',
                                            aws_access_key_id=Variable.get('aws_access_key_id'),
                                            aws_secret_access_key=Variable.get('aws_secret_access_key'))
    allUsers                = client.list_users()
    userVirtualMfa          = client.list_virtual_mfa_devices()
    mfaNotEnabled           = []
    virtualEnabled          = []
    physicalString          = ''
    whitelist               = ['data-pipeline', 'dt', 'etluser']

    #loop through virtual mfa to find users that actually have it
    for virtual in userVirtualMfa['VirtualMFADevices']:
        virtualEnabled.append(virtual['User']['UserName'])
              
    #loop through users to find physical MFA
    for user in allUsers['Users']:
        userMfa  = client.list_mfa_devices(UserName=user['UserName'])
        #print userMfa
        if len(userMfa['MFADevices']) == 0:
            if user['UserName'] not in virtualEnabled and user['UserName'] not in whitelist:
                mfaNotEnabled.append(user['UserName'])  
    
    task_instance.xcom_push(
    key='mfaNotEnabled_check',
    value=mfaNotEnabled)
 
default_args = {
    'email': ['juliaw@workmarket.com'],
    'start_date': airflow.utils.dates.days_ago(2),
}

with DAG(
        dag_id='security__github_twofa_check',
        schedule_interval='@hourly',
        catchup=False,
        default_args = default_args) as dag:
    
    mfa_check=PythonOperator(
        task_id = 'mfaNotEnabled_check',
        python_callable = mfaNotEnabled_check)

    html_content = """
        <p>
            The following users do not have MFA enable on the AWS console:
            <br>
            {{ task_instance.xcom_pull(task_ids='mfaNotEnabled_check', key='mfaNotEnabled_check')}}.
            <br>
            To whitelist an AWS user, please submit a request to the data team.
        </p>
        """

    email_about_users = EmailOperator(
        task_id='send_email_about_non_twofa_users',
        to=["julia@workmarket.com"],
        subject='AWS 2-Factor Noncompliance List',
        html_content=html_content)

dag >> mfa_check >> email_about_users


