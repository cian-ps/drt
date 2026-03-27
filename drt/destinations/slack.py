"""Slack destination — Incoming Webhooks.

Sends messages to a Slack channel via Incoming Webhook URL.
Supports plain text and Block Kit payloads via Jinja2 templates.

No extra dependencies required (uses httpx from core).

Example sync YAML:

    destination:
      type: slack
      webhook_url_env: SLACK_WEBHOOK_URL
      message_template: "New signup: {{ row.name }} ({{ row.email }})"

Block Kit example:

    destination:
      type: slack
      webhook_url_env: SLACK_WEBHOOK_URL
      block_kit: true
      message_template: |
        {
          "blocks": [
            {
              "type": "section",
              "text": {
                "type": "mrkdwn",
                "text": "*New user:* {{ row.name }}\n{{ row.email }}"
              }
            }
          ]
        }
"""

from __future__ import annotations

import json
import os

import httpx

from drt.config.models import SlackDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.templates.renderer import render_template


class SlackDestination:
    """Send records as Slack messages via Incoming Webhook."""

    def load(
        self,
        records: list[dict],
        config: SlackDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        webhook_url = config.webhook_url or (
            os.environ.get(config.webhook_url_env) if config.webhook_url_env else None
        )
        if not webhook_url:
            raise ValueError(
                "Slack destination: provide 'webhook_url' or set 'webhook_url_env'."
            )

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)

        with httpx.Client() as client:
            for record in records:
                rate_limiter.acquire()
                try:
                    rendered = render_template(config.message_template, record)
                    if config.block_kit:
                        payload = json.loads(rendered)
                    else:
                        payload = {"text": rendered}

                    response = client.post(webhook_url, json=payload)
                    response.raise_for_status()
                    result.success += 1
                except Exception as e:
                    result.failed += 1
                    result.errors.append(str(e))

        return result
