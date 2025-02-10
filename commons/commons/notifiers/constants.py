from enum import Enum


class NotificationChannel(str, Enum):
    SLACK = "slack"
    EMAIL = "email"


# Default Email templates
class TemplateType(str, Enum):
    METRIC_ALERT = "METRIC_ALERT"


EMAIL_TEMPLATES: dict = {
    TemplateType.METRIC_ALERT: {
        "subject": "{name} - {grain} report",
        "body": """
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                .metric-row {
                    margin: 10px 0;
                    padding: 8px;
                    border-radius: 4px;
                }
                .positive {
                    background-color: #e6f4ea;
                    color: #137333;
                }
                .negative {
                    background-color: #fce8e6;
                    color: #c5221f;
                }
                .metric-value {
                    font-size: 18px;
                    font-weight: bold;
                }
                .change {
                    font-size: 14px;
                    margin-left: 10px;
                }
            </style>
        </head>
        <body>
            <h2>{{ name }} - Performance Report</h2>
            <p>Period: {{ period }}</p>

            {% for metric in metrics %}
            <div class="metric-row {% if metric.percentage_change >= 0 %}positive{% else %}negative{% endif %}">
                <div>
                    <strong>{{ metric.name }}</strong>
                    <span class="metric-value">{{ metric.value }}</span>
                    <span class="change">({{ metric.percentage_change }}% / {{ metric.absolute_change }})</span>
                </div>
            </div>
            {% endfor %}

            <p style="margin-top: 20px;">
                <a href="{{ report_link }}" style="background-color: #1a73e8; color: white;
                padding: 10px 20px; text-decoration: none; border-radius: 4px;">
                    View full report →
                </a>
            </p>

            <p style="color: #666; font-size: 12px; margin-top: 20px;">
                This is an automated report. Please do not reply to this email.
            </p>
        </body>
        </html>
        """,
    }
}
