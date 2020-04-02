import boto3
import configparser
import os


def teardown_redshift():
    """Teardown the Redshift cluster: Deletes the IAM role,
    and then deletes the cluster."""
    print('Starting Redshift deletion...')

    config = _load_config()

    _delete_iam_role(config)

    _delete_redshift_cluster(config)

    with open('../dwh.cfg', 'w') as configfile:
        config.write(configfile)

    print('Redshift deletion complete')


def _load_config():
    """Loads the config from dwh.cfg"""
    print('Loading Redshift parameters from config...')

    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_path = dir_path + '/../dwh.cfg'

    config = configparser.ConfigParser()
    config.read(config_path)

    return config


def _delete_redshift_cluster(config):
    # Use boto3 to delete the cluster
    print('Deleting Redshift cluster...')
    redshift = boto3.client(
        'redshift',
        region_name=config['REDSHIFT']['REGION'],
        aws_access_key_id=os.environ['AWS_KEY'],
        aws_secret_access_key=os.environ['AWS_SECRET']
    )

    redshift.delete_cluster(
        ClusterIdentifier=config['REDSHIFT']['CLUSTER_IDENTIFIER'],
        SkipFinalClusterSnapshot=True
    )


def _delete_iam_role(config):
    # Use boto3 to delete the Redshift IAM role
    print('Deleting IAM role...')
    iam = boto3.client(
        'iam',
        aws_access_key_id=os.environ['AWS_KEY'],
        aws_secret_access_key=os.environ['AWS_SECRET'],
        region_name=config['REDSHIFT']['REGION']
    )

    iam.detach_role_policy(
        RoleName=config['IAM']['ROLE_NAME'],
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    iam.delete_role(RoleName=config['IAM']['ROLE_NAME'])


def main():
    teardown_redshift()


if __name__ == '__main__':
    main()
