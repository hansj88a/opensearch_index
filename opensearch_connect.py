"""
로컬 환경에서 Amazon OpenSearch Service 도메인 연결을 점검한다.
(TCP/HTTPS, SigV4 서명, IAM·FGAC 권한 단계별로 실패 지점을 구분하는 데 유리하다.)

  pip install opensearch-py boto3

  아래 상수에 값을 넣은 뒤:
  python opensearch_connect.py

주의: 액세스 키가 들어간 채로 Git 에 커밋하지 마세요.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Callable

from shilladfs_opensearch_products import get_opensearch_client

# ---------------------------------------------------------------------------
# 로컬 테스트 전용 — 실제 값으로 수정 후 사용
# ---------------------------------------------------------------------------

TEST_OPENSEARCH_ENDPOINT = "search-your-domain.ap-northeast-2.es.amazonaws.com"
TEST_AWS_REGION = "ap-northeast-2"

TEST_AWS_ACCESS_KEY_ID = ""
TEST_AWS_SECRET_ACCESS_KEY = ""
# STS 임시 자격 증명일 때만 입력, 아니면 빈 문자열
TEST_AWS_SESSION_TOKEN = ""


def apply_test_constants_to_environ() -> None:
    """상수를 os.environ 에 반영한다. 빈 문자열인 항목은 설정하지 않는다."""
    if TEST_OPENSEARCH_ENDPOINT.strip():
        os.environ["OPENSEARCH_ENDPOINT"] = TEST_OPENSEARCH_ENDPOINT.strip()
    if TEST_AWS_REGION.strip():
        os.environ["AWS_REGION"] = TEST_AWS_REGION.strip()
    if TEST_AWS_ACCESS_KEY_ID.strip():
        os.environ["AWS_ACCESS_KEY_ID"] = TEST_AWS_ACCESS_KEY_ID.strip()
    if TEST_AWS_SECRET_ACCESS_KEY.strip():
        os.environ["AWS_SECRET_ACCESS_KEY"] = TEST_AWS_SECRET_ACCESS_KEY.strip()
    if TEST_AWS_SESSION_TOKEN.strip():
        os.environ["AWS_SESSION_TOKEN"] = TEST_AWS_SESSION_TOKEN.strip()


def _format_exc(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _run_step(fn: Callable[[], Any]) -> tuple[bool, Any]:
    """실행 함수를 받아 (성공 여부, 결과 또는 예외 문자열)을 반환한다."""
    try:
        out = fn()
        return True, out
    except BaseException as e:
        return False, _format_exc(e)


def test_ping(client) -> bool:
    if not client.ping():
        raise RuntimeError("ping 이 False 를 반환했습니다(연결·인증 이전 단계 가능).")
    return True


def test_info(client) -> dict[str, Any]:
    return client.info()


def test_cluster_health(client) -> dict[str, Any]:
    return client.cluster.health(request_timeout=30)


def print_summary(
    endpoint: str,
    region: str,
    steps: list[tuple[str, bool, Any]],
) -> None:
    print("--- OpenSearch 연결 테스트 ---")
    print(f"endpoint: {endpoint}")
    print(f"SigV4 region: {region}")
    print()
    for name, ok, detail in steps:
        status = "OK" if ok else "FAIL"
        print(f"[{status}] {name}")
        if ok and detail is not None:
            if isinstance(detail, dict):
                print(json.dumps(detail, indent=2, ensure_ascii=False, default=str))
            elif isinstance(detail, bool):
                print(f"  -> {detail}")
            else:
                print(f"  -> {detail}")
        elif not ok:
            print(f"  -> {detail}")
        print()


def main() -> int:
    apply_test_constants_to_environ()

    endpoint = TEST_OPENSEARCH_ENDPOINT.strip() or None
    region = TEST_AWS_REGION.strip() or None

    try:
        client = get_opensearch_client(
            opensearch_endpoint=endpoint,
            aws_region=region,
        )
    except RuntimeError as e:
        print(_format_exc(e), file=sys.stderr)
        return 1

    resolved_host = endpoint or (os.environ.get("OPENSEARCH_ENDPOINT") or "").strip()
    resolved_region = region or (os.environ.get("AWS_REGION") or "ap-northeast-2").strip()

    steps: list[tuple[str, bool, Any]] = []
    steps.append(("ping (HEAD /)", *_run_step(lambda: test_ping(client))))
    steps.append(("GET / (info)", *_run_step(lambda: test_info(client))))
    steps.append(("GET /_cluster/health", *_run_step(lambda: test_cluster_health(client))))

    print_summary(resolved_host or "(OPENSEARCH_ENDPOINT)", resolved_region, steps)

    if all(ok for _, ok, _ in steps):
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
