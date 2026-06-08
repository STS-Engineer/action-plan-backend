import os
import logging
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


logger = logging.getLogger(__name__)


def get_smtp_config_diagnostics():
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = os.getenv("SMTP_PORT")
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")

    return {
        "smtp_host_present": bool(smtp_host),
        "smtp_port_present": bool(smtp_port),
        "smtp_user_present": bool(smtp_user),
        "smtp_password_present": bool(smtp_password),
        "sender_email": smtp_user or None,
    }


def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    attachments: list[tuple[str, bytes]] | None = None,
):
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port_raw = os.getenv("SMTP_PORT", "25")
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")

    if not to_email:
        logger.warning("Email send skipped because recipient is empty.")
        return False

    if not smtp_host or not smtp_user:
        logger.error(
            "Email send skipped because SMTP is incomplete. diagnostics=%s",
            get_smtp_config_diagnostics(),
        )
        return False

    try:
        smtp_port = int(smtp_port_raw)
    except (TypeError, ValueError):
        logger.error("Email send skipped because SMTP_PORT is invalid.")
        return False

    message = MIMEMultipart("mixed")
    message["Subject"] = subject
    message["From"] = smtp_user
    message["To"] = to_email

    body_part = MIMEMultipart("alternative")
    body_part.attach(MIMEText(html_body, "html"))
    message.attach(body_part)

    for filename, file_content in attachments or []:
        attachment = MIMEApplication(file_content, _subtype="pdf")
        attachment.add_header(
            "Content-Disposition",
            "attachment",
            filename=filename,
        )
        message.attach(attachment)

    try:
        logger.info(
            "Sending email recipient=%s attachment_count=%s",
            to_email,
            len(attachments or []),
        )

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()

            if smtp_password:
                server.starttls()
                server.login(smtp_user, smtp_password)

            server.sendmail(smtp_user, [to_email], message.as_string())

        logger.info("SMTP send success recipient=%s", to_email)
    except Exception:
        logger.exception("SMTP send failure recipient=%s", to_email)
        return False

    return True
