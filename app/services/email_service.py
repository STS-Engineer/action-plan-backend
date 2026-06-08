import logging
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


logger = logging.getLogger(__name__)
DEFAULT_SMTP_TIMEOUT_SECONDS = 30


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)

    if raw_value is None:
        return default

    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)

    if raw_value is None:
        return default

    try:
        return int(raw_value)
    except ValueError:
        return default


def _get_smtp_port() -> int:
    return _parse_int_env("SMTP_PORT", 25)


def _get_smtp_timeout() -> int:
    return _parse_int_env("SMTP_TIMEOUT_SECONDS", DEFAULT_SMTP_TIMEOUT_SECONDS)


def _get_smtp_use_ssl(port: int) -> bool:
    return _parse_bool_env("SMTP_USE_SSL", default=port == 465)


def _get_smtp_use_tls(port: int, use_ssl: bool, password_present: bool) -> bool:
    return _parse_bool_env(
        "SMTP_USE_TLS",
        default=(port == 587 or (password_present and not use_ssl)),
    )


def _get_smtp_auth_enabled(password_present: bool) -> bool:
    return _parse_bool_env("SMTP_AUTH_ENABLED", default=password_present)


def _safe_decode_smtp_response(value) -> str | None:
    if value is None:
        return None

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    return str(value)


def _sanitize_error_detail(detail: str | None) -> str | None:
    if not detail:
        return detail

    smtp_password = os.getenv("SMTP_PASSWORD")

    if smtp_password:
        detail = detail.replace(smtp_password, "[redacted]")

    return detail[:1000]


def _build_failure_suggestion(port: int, error_type: str | None) -> str | None:
    if port == 25:
        return (
            "Port 25 can be blocked or unreliable from Azure App Service. "
            "Try SMTP_HOST=smtp.office365.com, SMTP_PORT=587, "
            "SMTP_USE_TLS=true, SMTP_USE_SSL=false, SMTP_AUTH_ENABLED=true. "
            "For Microsoft 365 relay on port 25, use SMTP_AUTH_ENABLED=false "
            "only if the relay allows your app outbound IP."
        )

    if error_type == "SMTPAuthenticationError":
        return (
            "Check the SMTP username/password and whether SMTP AUTH is enabled "
            "for this Microsoft 365 mailbox."
        )

    if error_type in {"SMTPConnectError", "TimeoutError", "gaierror", "OSError"}:
        return "Check SMTP host, port, firewall, and TLS/SSL mode."

    return None


def get_smtp_config_diagnostics():
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = _get_smtp_port()
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    smtp_timeout = _get_smtp_timeout()
    smtp_use_ssl = _get_smtp_use_ssl(smtp_port)
    smtp_auth_enabled = _get_smtp_auth_enabled(bool(smtp_password))
    smtp_use_tls = _get_smtp_use_tls(
        smtp_port,
        smtp_use_ssl,
        bool(smtp_password) and smtp_auth_enabled,
    )

    return {
        "smtp_host_present": bool(smtp_host),
        "smtp_host": smtp_host or None,
        "smtp_port_present": bool(os.getenv("SMTP_PORT")),
        "smtp_port": smtp_port,
        "smtp_user_present": bool(smtp_user),
        "smtp_password_present": bool(smtp_password),
        "smtp_use_tls": smtp_use_tls,
        "smtp_use_ssl": smtp_use_ssl,
        "smtp_auth_enabled": smtp_auth_enabled,
        "smtp_timeout_seconds": smtp_timeout,
        "sender_email": smtp_user or None,
        "recommended_microsoft_365": {
            "SMTP_HOST": "smtp.office365.com",
            "SMTP_PORT": 587,
            "SMTP_USE_TLS": True,
            "SMTP_USE_SSL": False,
            "SMTP_AUTH_ENABLED": True,
        },
        "alternative_relay": {
            "SMTP_HOST": "avocarbon-com.mail.protection.outlook.com",
            "SMTP_PORT": 25,
            "SMTP_USE_TLS": "false or true depending relay",
            "SMTP_USE_SSL": False,
            "SMTP_AUTH_ENABLED": False,
        },
    }


def _build_message(
    to_email: str,
    subject: str,
    html_body: str,
    attachments: list[tuple[str, bytes]] | None = None,
):
    smtp_user = os.getenv("SMTP_USER")
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

    return message


def _failure_result(
    message: str,
    error_type: str,
    error_detail: str | None = None,
    smtp_code: int | None = None,
    smtp_response: str | None = None,
):
    diagnostics = get_smtp_config_diagnostics()
    smtp_port = diagnostics["smtp_port"]

    return {
        "success": False,
        "message": message,
        "error_type": error_type,
        "error_detail": _sanitize_error_detail(error_detail),
        "smtp_code": smtp_code,
        "smtp_response": _sanitize_error_detail(smtp_response),
        "diagnostics": diagnostics,
        "suggestion": _build_failure_suggestion(smtp_port, error_type),
    }


def _success_result():
    return {
        "success": True,
        "message": "SMTP send succeeded",
        "error_type": None,
        "error_detail": None,
        "smtp_code": None,
        "smtp_response": None,
        "diagnostics": get_smtp_config_diagnostics(),
        "suggestion": None,
    }


def send_email_with_diagnostics(
    to_email: str,
    subject: str,
    html_body: str,
    attachments: list[tuple[str, bytes]] | None = None,
):
    diagnostics = get_smtp_config_diagnostics()
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = diagnostics["smtp_port"]
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    smtp_use_tls = diagnostics["smtp_use_tls"]
    smtp_use_ssl = diagnostics["smtp_use_ssl"]
    smtp_auth_enabled = diagnostics["smtp_auth_enabled"]
    smtp_timeout = diagnostics["smtp_timeout_seconds"]

    if not to_email:
        logger.warning("Email send skipped because recipient is empty.")
        return _failure_result(
            "SMTP send failed",
            "MissingRecipient",
            "Recipient email is empty.",
        )

    if not smtp_host:
        logger.error("Email send skipped because SMTP_HOST is missing.")
        return _failure_result(
            "SMTP send failed",
            "MissingSmtpHost",
            "SMTP_HOST is not configured.",
        )

    if not smtp_user:
        logger.error("Email send skipped because SMTP_USER is missing.")
        return _failure_result(
            "SMTP send failed",
            "MissingSmtpUser",
            "SMTP_USER is not configured.",
        )

    message = _build_message(to_email, subject, html_body, attachments)
    smtp_class = smtplib.SMTP_SSL if smtp_use_ssl else smtplib.SMTP

    try:
        logger.info(
            "SMTP send start recipient=%s host=%s port=%s use_tls=%s "
            "use_ssl=%s auth_enabled=%s sender=%s attachment_count=%s",
            to_email,
            smtp_host,
            smtp_port,
            smtp_use_tls,
            smtp_use_ssl,
            smtp_auth_enabled,
            smtp_user,
            len(attachments or []),
        )

        with smtp_class(smtp_host, smtp_port, timeout=smtp_timeout) as server:
            server.ehlo()

            if smtp_use_tls and not smtp_use_ssl:
                server.starttls()
                server.ehlo()

            if smtp_auth_enabled and smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)

            server.sendmail(smtp_user, [to_email], message.as_string())

        logger.info("SMTP send success recipient=%s", to_email)
        return _success_result()
    except smtplib.SMTPResponseException as exc:
        smtp_response = _safe_decode_smtp_response(exc.smtp_error)
        logger.exception(
            "SMTP send failure recipient=%s host=%s port=%s use_tls=%s "
            "use_ssl=%s auth_enabled=%s sender=%s error_type=%s "
            "smtp_code=%s smtp_response=%s",
            to_email,
            smtp_host,
            smtp_port,
            smtp_use_tls,
            smtp_use_ssl,
            smtp_auth_enabled,
            smtp_user,
            type(exc).__name__,
            exc.smtp_code,
            smtp_response,
        )
        return _failure_result(
            "SMTP send failed",
            type(exc).__name__,
            str(exc),
            smtp_code=exc.smtp_code,
            smtp_response=smtp_response,
        )
    except Exception as exc:
        logger.exception(
            "SMTP send failure recipient=%s host=%s port=%s use_tls=%s "
            "use_ssl=%s auth_enabled=%s sender=%s error_type=%s",
            to_email,
            smtp_host,
            smtp_port,
            smtp_use_tls,
            smtp_use_ssl,
            smtp_auth_enabled,
            smtp_user,
            type(exc).__name__,
        )
        return _failure_result(
            "SMTP send failed",
            type(exc).__name__,
            str(exc),
        )


def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    attachments: list[tuple[str, bytes]] | None = None,
):
    return send_email_with_diagnostics(
        to_email=to_email,
        subject=subject,
        html_body=html_body,
        attachments=attachments,
    )["success"]


def send_smtp_test_email(to_email: str):
    return send_email_with_diagnostics(
        to_email=to_email,
        subject="[Action Plan] SMTP configuration test",
        html_body="""
        <div style="font-family: Arial, sans-serif; color: #1f2937;">
          <h2>Action Plan SMTP test</h2>
          <p>This is a minimal SMTP test email from the Action Plan backend.</p>
        </div>
        """,
    )
