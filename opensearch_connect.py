"""
로컬 환경에서 Amazon OpenSearch Service 도메인 연결을 점검한다.
(TCP/HTTPS, SigV4 서명, IAM·FGAC 권한 단계별로 실패 지점을 구분하는 데 유리하다.)

이 파일만으로 동작하며 다른 프로젝트 모듈을 import 하지 않는다.

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

# ---------------------------------------------------------------------------
# 로컬 테스트 전용 — 실제 값으로 수정 후 사용
# ---------------------------------------------------------------------------

TEST_OPENSEARCH_ENDPOINT = "search-your-domain.ap-northeast-2.es.amazonaws.com"
TEST_AWS_REGION = "ap-northeast-2"

TEST_AWS_ACCESS_KEY_ID = ""
TEST_AWS_SECRET_ACCESS_KEY = ""
# STS 임시 자격 증명일 때만 입력, 아니면 빈 문자열
TEST_AWS_SESSION_TOKEN = ""

# AWS OpenSearch Service SigV4 서비스 이름
OPENSEARCH_SERVICE = "es"


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


def get_aws_boto_session():
    """명시적 키가 환경에 있으면 그것으로, 없으면 기본 자격 증명 체인으로 Session 을 만든다."""
    import boto3

    key = (os.environ.get("AWS_ACCESS_KEY_ID") or "").strip()
    secret = (os.environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()
    token = (os.environ.get("AWS_SESSION_TOKEN") or "").strip()
    if key and secret:
        kw: dict[str, str] = {"aws_access_key_id": key, "aws_secret_access_key": secret}
        if token:
            kw["aws_session_token"] = token
        return boto3.Session(**kw)
    return boto3.Session()


def create_opensearch_client():
    """TEST_* 상수·환경 변수를 바탕으로 SigV4 인증 OpenSearch 클라이언트를 만든다."""
    from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

    apply_test_constants_to_environ()

    session = get_aws_boto_session()
    credentials = session.get_credentials()
    if credentials is None:
        raise RuntimeError(
            "AWS 자격 증명을 찾을 수 없습니다. "
            "TEST_AWS_ACCESS_KEY_ID·TEST_AWS_SECRET_ACCESS_KEY 를 채우거나 "
            "~/.aws/credentials·IAM 역할 등 기본 체인을 설정하세요."
        )

    region = (TEST_AWS_REGION.strip() or "ap-northeast-2").strip()
    raw = TEST_OPENSEARCH_ENDPOINT.strip()
    if not raw:
        raise RuntimeError("TEST_OPENSEARCH_ENDPOINT 가 비어 있습니다. 도메인 호스트를 입력하세요.")

    auth = AWSV4SignerAuth(credentials, region, OPENSEARCH_SERVICE)
    host = raw.replace("https://", "").replace("http://", "").rstrip("/")

    return OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=60,
    )


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
    try:
        client = create_opensearch_client()
    except RuntimeError as e:
        print(_format_exc(e), file=sys.stderr)
        return 1

    resolved_host = TEST_OPENSEARCH_ENDPOINT.strip()
    resolved_region = (TEST_AWS_REGION.strip() or "ap-northeast-2").strip()

    steps: list[tuple[str, bool, Any]] = []
    steps.append(("ping (HEAD /)", *_run_step(lambda: test_ping(client))))
    steps.append(("GET / (info)", *_run_step(lambda: test_info(client))))
    steps.append(("GET /_cluster/health", *_run_step(lambda: test_cluster_health(client))))

    print_summary(resolved_host, resolved_region, steps)

    if all(ok for _, ok, _ in steps):
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
