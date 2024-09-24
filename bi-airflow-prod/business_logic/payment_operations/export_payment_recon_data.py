import datetime
import logging
from tempfile import NamedTemporaryFile

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ssh.hooks.ssh import SSHHook

from plugins.slack_utils import send_notification

logger = logging.getLogger(__name__)

SLACK_CONN = 'slack'


def s3_to_sftp(s3_bucket: str, s3_prefix: str, remote_filepath: str):
    ssh_hook = SSHHook(ssh_conn_id='actuals_sftp')
    s3_hook = S3Hook('aws_default')
    s3_client = s3_hook.get_conn()
    sftp_client = ssh_hook.get_conn().open_sftp()

    with NamedTemporaryFile("w") as f:
        s3_client.download_file(s3_bucket, s3_prefix, f.name)

        sftp_client.put(localpath=f.name, remotepath=remote_filepath)
        f.close()

    message = (f"*Payment Recon Data export:* "
               f"\nExported the file to Actuals SFTP server : "
               f"`{remote_filepath}` ")

    send_notification(message, 'slack')


def export_recon_data():
    dt = datetime.datetime.now().strftime("%Y%m%d%H%M")
    recon_remote_filepath = f"prod/source.3/CRM_{dt}.csv.gz"
    ixopay_remote_filepath = f"prod/source.1/IXOPAY_{dt}.csv.gz"

    s3_bucket = Variable.get('s3_bucket_curated_bi',
                             default_var='grover-eu-central-1-production-data-bi-curated')

    s3_to_sftp(s3_bucket, "staging/actuals/unload_recon.csv000.gz", recon_remote_filepath)
    s3_to_sftp(s3_bucket, "staging/actuals/unload_ixo.csv000.gz", ixopay_remote_filepath)
