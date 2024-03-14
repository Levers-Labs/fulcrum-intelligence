from app.utilities.async_http_client import AsyncHttpClient


class QueryManagerClient(AsyncHttpClient):
    METRICS_VALUES_BASE_URL = "/metrics/values"
    DATE_FORMAT = "%Y-%m-%d"

    async def get_metric_values(self, metric_ids, start_date, end_date, dimensions=None):
        start_date = start_date.strftime(self.DATE_FORMAT)
        end_date = end_date.strftime(self.DATE_FORMAT)
        data = {
            "metric_ids": metric_ids,
            "start_date": start_date,
            "end_date": end_date,
            "dimensions": dimensions,
        }
        return await self.post(endpoint=self.METRICS_VALUES_BASE_URL, data=data)
