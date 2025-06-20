from airflow.utils.email import send_email_smtp
from airflow.models import Variable
from airflow.configuration import conf
from urllib.parse import quote_plus
import logging
import pytz
from typing import Dict
from datetime import datetime


def send_dag_notification(context: Dict):
    try:
        dag_run = context['dag_run']
        task_instance = context.get('task_instance')
        dag_id = dag_run.dag_id
        execution_date = dag_run.execution_date

        # Konversi UTC ke WIB (UTC+7)
        utc_time = execution_date.replace(tzinfo=pytz.UTC)
        wib_time = utc_time.astimezone(pytz.timezone("Asia/Jakarta"))
        waktu_eksekusi_str = wib_time.strftime("%Y-%m-%d %H:%M:%S") + " WIB"

        # Ambil base_url Airflow dari config, fallback ke localhost jika kosong
        webserver_url = conf.get('webserver', 'base_url', fallback=None)
        if not webserver_url or not webserver_url.startswith(('http://', 'https://')):
            logging.warning(f"webserver_url tidak valid atau kosong: '{webserver_url}', fallback ke 'http://localhost:8080'")
            webserver_url = 'http://localhost:8080'
        else:
            webserver_url = webserver_url.rstrip('/')

        # URL dashboard DAG
        log_url = (
            f"{webserver_url}/dags/{dag_id}/grid"
            f"?dag_run_id={dag_run.run_id}"
            f"&highlight_tasks={task_instance.task_id if task_instance else ''}"
        )

        # Ambil semua task instances
        tasks = dag_run.get_task_instances()

        # Kategorikan task berdasarkan status
        success_tasks = [ti for ti in tasks if ti.state == 'success']
        failed_tasks = [ti for ti in tasks if ti.state in ('failed', 'upstream_failed')]
        other_tasks = [ti for ti in tasks if ti.state not in ('success', 'failed', 'upstream_failed')]

        dag_status = dag_run.state.upper()
        subject = f"[Airflow] DAG {dag_id} - {dag_status}"

        # Template HTML email
        html_content = f"""
        <h1>Status DAG: <span style="color: {'#28a745' if dag_status == 'SUCCESS' else '#dc3545'}">{dag_status}</span></h1>
        <p><b>DAG:</b> {dag_id}</p>
        <p><b>Execution Time:</b> {waktu_eksekusi_str}</p>

        <h4>Task Detail:</h4>
        <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%;">
            <thead>
                <tr>
                    <th style="background-color: #f8f9fa;">Status</th>
                    <th style="background-color: #f8f9fa;">Task Name</th>
                    <th style="background-color: #f8f9fa;">Log</th>
                </tr>
            </thead>
            <tbody>
                {"".join([
                    f'''
                    <tr>
                        <td style="color: {'#28a745' if task.state == 'success' else '#dc3545'}">
                            {'✅ Success' if task.state == 'success' else '❌ Failed'}
                        </td>
                        <td>{task.task_id}</td>
                        <td>
                            <a href="{webserver_url}/log?dag_id={dag_id}&task_id={task.task_id}&execution_date={quote_plus(execution_date.isoformat())}"
                               target="_blank">See log</a>
                        </td>
                    </tr>
                    ''' for task in success_tasks + failed_tasks
                ])}
            </tbody>
        </table>

        {f"<p><b>Another task ({len(other_tasks)}):</b> {', '.join([ti.task_id for ti in other_tasks])}</p>" if other_tasks else ""}

        <p style="margin-top: 20px;">
            <a href="{log_url}" style="text-decoration: none; padding: 8px 15px; background-color: #007bff; color: white; border-radius: 4px;">
                View DAG Dashboard
            </a>
        </p>
        """
        variable_name = "email_recipients"
        # Kirim email notifikasi
        send_email_smtp(
            to=Variable.get(variable_name, default_var=None),
            subject=subject,
            html_content=html_content,
            smtp_host=conf.get('smtp', 'SMTP_HOST'),
            smtp_port=conf.getint('smtp', 'SMTP_PORT'),
            smtp_user=conf.get('smtp', 'SMTP_USER'),
            smtp_password=conf.get('smtp', 'SMTP_PASSWORD'),
            smtp_starttls=conf.getboolean('smtp', 'SMTP_STARTTLS'),
            smtp_ssl=conf.getboolean('smtp', 'SMTP_SSL'),
            from_email=conf.get('smtp', 'SMTP_MAIL_FROM')
        )

        logging.info("Email notifikasi berhasil dikirim!")

    except Exception as e:
        logging.error(f"Gagal mengirim notifikasi: {str(e)}")
        logging.exception("Detail error:")
