"""SNS publisher helper used by adapters for event fan-out."""

from typing import Any, Dict

import boto3
import simplejson as json

from . import logger


def publish(**kwargs: Any) -> None:
    if not kwargs.get("arn") or not kwargs.get("data"):
        return
    try:
        sns_client = boto3.client(
            "sns", region_name=kwargs.get("region"), endpoint_url=kwargs.get("endpoint")
        )
        publish_kwargs: Dict[str, Any] = {
            "TopicArn": kwargs["arn"],
            "Message": json.dumps(kwargs["data"]),
            "MessageAttributes": kwargs.get("attributes", {}),
        }
        if kwargs.get("fifo_group_id"):
            publish_kwargs["MessageGroupId"] = kwargs["fifo_group_id"]
        if kwargs.get("fifo_duplication_id"):
            publish_kwargs["MessageDeduplicationId"] = kwargs["fifo_duplication_id"]
        sns_client.publish(**publish_kwargs)
    except Exception as exc:  # pylint: disable=broad-except
        logger.log(level="WARN", log={"error": f"publish_sns_error: {exc}"})
