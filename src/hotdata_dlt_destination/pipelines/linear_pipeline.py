from __future__ import annotations

import json
import os
from typing import Any
from urllib import request

import dlt

from hotdata_dlt_destination.destination import hotdata_destination

LINEAR_GRAPHQL_URL = "https://api.linear.app/graphql"


def fetch_linear_issues(
    *,
    api_key: str,
    issue_limit: int,
    team_key: str | None,
) -> list[dict[str, Any]]:
    if team_key:
        query = """
        query Issues($first: Int!, $teamKey: String!) {
          issues(
            first: $first
            orderBy: updatedAt
            filter: {team: {key: {eq: $teamKey}}}
          ) {
            nodes {
              id
              identifier
              title
              priority
              createdAt
              updatedAt
              url
              state { name type }
              team { id key name }
            }
          }
        }
        """
        variables: dict[str, Any] = {"first": issue_limit, "teamKey": team_key}
    else:
        query = """
        query Issues($first: Int!) {
          issues(first: $first, orderBy: updatedAt) {
            nodes {
              id
              identifier
              title
              priority
              createdAt
              updatedAt
              url
              state { name type }
              team { id key name }
            }
          }
        }
        """
        variables = {"first": issue_limit}

    payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    req = request.Request(
        LINEAR_GRAPHQL_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": api_key,
        },
        method="POST",
    )
    with request.urlopen(req) as response:
        body = json.loads(response.read().decode("utf-8"))
    return body["data"]["issues"]["nodes"]


@dlt.resource(name="linear_issues", write_disposition="merge", primary_key="id")
def linear_issues_resource(
    linear_api_key: str,
    linear_issue_limit: int = 25,
    linear_team_key: str | None = None,
) -> list[dict[str, Any]]:
    return fetch_linear_issues(
        api_key=linear_api_key,
        issue_limit=linear_issue_limit,
        team_key=linear_team_key,
    )


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="hotdata_linear",
        destination=hotdata_destination(write_disposition="upsert", database_name="linear"),  # type: ignore[call-arg]
        dataset_name="hotdata_linear",
    )
    linear_api_key = os.environ["LINEAR_API_KEY"]
    linear_team_key = os.environ.get("LINEAR_TEAM_KEY")
    issue_limit = int(os.environ.get("LINEAR_ISSUE_LIMIT", "25"))
    load_info = pipeline.run(
        linear_issues_resource(
            linear_api_key=linear_api_key,
            linear_team_key=linear_team_key,
            linear_issue_limit=issue_limit,
        )
    )
    print(load_info)


if __name__ == "__main__":
    main()
