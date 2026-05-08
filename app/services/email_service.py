import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    attachments: list[tuple[str, bytes]] | None = None,
):
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "25"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")

    if not to_email:
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

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()

        if smtp_password:
            try:
                server.starttls()
                server.login(smtp_user, smtp_password)
            except Exception:
                pass

        server.sendmail(smtp_user, [to_email], message.as_string())

    return True