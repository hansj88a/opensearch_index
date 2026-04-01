"""
쇼핑몰 상품 자연어 검색용 OpenSearch 인덱스 생성, 샘플 데이터 등록, product_id 삭제.

필요 패키지:
  pip install opensearch-py boto3

환경 변수(또는 아래 상수 수정):
  OPENSEARCH_ENDPOINT  예: search-xxx.ap-northeast-2.es.amazonaws.com (https 제외)
  AWS_REGION           OpenSearch·SigV4용, 기본 ap-northeast-2
  BEDROCK_REGION       Bedrock 호출용(미설정 시 AWS_REGION과 동일)
  BEDROCK_EMBEDDING_MODEL_ID  기본 amazon.titan-embed-text-v2:0
  TITAN_EMBED_NORMALIZE       true/false, 기본 true (Titan v2 normalize 옵션)
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN(선택)
    명시 시 OpenSearch·Bedrock 모두 해당 키 사용. 미설정 시 boto3 기본 체인(profile, IAM 역할 등)
  IAM: OpenSearch + bedrock:InvokeModel(해당 모델 리소스)
"""

from __future__ import annotations

import os
import sys
from typing import Any

# ---------------------------------------------------------------------------
# 공통 상수
# ---------------------------------------------------------------------------

OPENSEARCH_ENDPOINT = os.environ.get(
    "OPENSEARCH_ENDPOINT", "search-your-domain.ap-northeast-2.es.amazonaws.com"
)
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
BEDROCK_REGION = os.environ.get("BEDROCK_REGION", AWS_REGION)
INDEX_NAME = "shilladfs-products"
OPENSEARCH_SERVICE = "es"  # AWS 서명용 서비스 이름

# 상품 통합 임베딩: Amazon Titan Text Embeddings V2 (Bedrock)
# dimensions는 256 | 512 | 1024 — 인덱스 dense_vector dims와 반드시 일치
BEDROCK_EMBEDDING_MODEL_ID = os.environ.get(
    "BEDROCK_EMBEDDING_MODEL_ID",
    "amazon.titan-embed-text-v2:0",
)
EMBEDDING_DIMENSION = 1024
TITAN_EMBED_NORMALIZE = os.environ.get("TITAN_EMBED_NORMALIZE", "true").lower() in (
    "1",
    "true",
    "yes",
)

# Nori 플러그인이 없는 도메인에서는 인덱스 생성이 실패할 수 있습니다.
# (Amazon OpenSearch Service에서 Nori는 일반적으로 사용 가능)
INDEX_BODY: dict[str, Any] = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
        }
    },
    "mappings": {
        "properties": {
            "product_id": {"type": "keyword"},
            "product_name": {
                "type": "text",
                "fields": {
                    "ko": {"type": "text", "analyzer": "nori"},
                    "en": {"type": "text", "analyzer": "standard"},
                },
            },
            "category": {
                "type": "text",
                "fields": {
                    "ko": {"type": "text", "analyzer": "nori"},
                    "en": {"type": "text", "analyzer": "standard"},
                },
            },
            "brand": {
                "properties": {
                    "ko": {"type": "keyword"},
                    "en": {"type": "keyword"},
                    "aliases": {"type": "keyword"},
                }
            },
            "price": {"type": "float"},
            "created_at": {"type": "date"},
            "gender": {"type": "keyword"},
            "colors": {"type": "keyword"},
            "age": {"type": "keyword"},
            "size_capacity": {"type": "float"},
            "size_shoes": {
                "properties": {
                    "kr": {"type": "float"},
                    "eu": {"type": "float"},
                    "uk": {"type": "float"},
                    "us": {"type": "float"},
                }
            },
            "description": {"type": "text", "analyzer": "nori"},
            "usage_effects": {"type": "text", "analyzer": "nori"},
            "keywords": {"type": "keyword"},
            "visual_features": {"type": "text", "analyzer": "nori"},
            "auditory_features": {"type": "text", "analyzer": "nori"},
            "olfactory_features": {"type": "text", "analyzer": "nori"},
            "gustatory_features": {"type": "text", "analyzer": "nori"},
            "tactile_features": {"type": "text", "analyzer": "nori"},
            "embedding": {"type": "dense_vector", "dims": 1024},
        }
    },
}


def _explicit_aws_credential_kwargs() -> dict[str, str]:
    """환경 변수에 액세스 키·시크릿이 있으면 boto3.Session에 넘길 인자 dict, 없으면 빈 dict."""
    key = (os.environ.get("AWS_ACCESS_KEY_ID") or "").strip()
    secret = (os.environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()
    token = (os.environ.get("AWS_SESSION_TOKEN") or "").strip()
    if key and secret:
        out: dict[str, str] = {
            "aws_access_key_id": key,
            "aws_secret_access_key": secret,
        }
        if token:
            out["aws_session_token"] = token
        return out
    return {}


def get_aws_boto_session():
    """명시적 키가 있으면 그것으로, 없으면 기본 자격 증명 체인으로 Session을 만든다."""
    import boto3

    explicit = _explicit_aws_credential_kwargs()
    if explicit:
        return boto3.Session(**explicit)
    return boto3.Session()


def prompt_aws_credentials_to_environ() -> None:
    """콘솔에서 액세스 키·시크릿(·선택 세션 토큰)을 입력받아 환경 변수에 설정한다."""
    import getpass

    if not (os.environ.get("AWS_ACCESS_KEY_ID") or "").strip():
        os.environ["AWS_ACCESS_KEY_ID"] = input("AWS Access Key ID: ").strip()
    if not (os.environ.get("AWS_SECRET_ACCESS_KEY") or "").strip():
        os.environ["AWS_SECRET_ACCESS_KEY"] = getpass.getpass("AWS Secret Access Key: ").strip()
    token = input("AWS Session Token (임시 자격증명만 해당, 없으면 Enter): ").strip()
    if token:
        os.environ["AWS_SESSION_TOKEN"] = token


def apply_cli_aws_credentials_to_environ(
    access_key_id: str | None,
    secret_access_key: str | None,
    session_token: str | None,
) -> None:
    """CLI로 넘긴 키를 환경 변수에 반영한다(None·빈 문자열은 무시)."""
    if access_key_id and str(access_key_id).strip():
        os.environ["AWS_ACCESS_KEY_ID"] = str(access_key_id).strip()
    if secret_access_key and str(secret_access_key).strip():
        os.environ["AWS_SECRET_ACCESS_KEY"] = str(secret_access_key).strip()
    if session_token is not None:
        st = str(session_token).strip()
        if st:
            os.environ["AWS_SESSION_TOKEN"] = st
        elif "AWS_SESSION_TOKEN" in os.environ:
            del os.environ["AWS_SESSION_TOKEN"]


def get_bedrock_runtime_client():
    """Amazon Bedrock Runtime 클라이언트(Titan Text Embeddings v2 호출용)."""
    session = get_aws_boto_session()
    return session.client(service_name="bedrock-runtime", region_name=BEDROCK_REGION)


def build_embedding_text_for_product(doc: dict[str, Any]) -> str:
    """
    인덱스 설계에 맞춰 설명·감각 특징·키워드·브랜드·카테고리·상품명을 한 문자열로 묶는다.
    Titan Text Embeddings v2의 inputText로 사용한다.
    """
    parts: list[str] = []
    if doc.get("product_name"):
        parts.append(str(doc["product_name"]))
    if doc.get("category"):
        parts.append(str(doc["category"]))

    brand = doc.get("brand")
    if isinstance(brand, dict):
        for key in ("ko", "en"):
            v = brand.get(key)
            if v:
                parts.append(str(v))
        aliases = brand.get("aliases")
        if isinstance(aliases, list):
            parts.extend(str(a) for a in aliases if a)

    for key in (
        "description",
        "usage_effects",
        "visual_features",
        "auditory_features",
        "olfactory_features",
        "gustatory_features",
        "tactile_features",
    ):
        v = doc.get(key)
        if v:
            parts.append(str(v))

    keywords = doc.get("keywords")
    if isinstance(keywords, list):
        parts.extend(str(k) for k in keywords if k)

    text = "\n".join(parts).strip()
    if not text:
        raise ValueError("임베딩용 텍스트가 비었습니다. product_name 등 최소 필드를 채우세요.")
    return text


def invoke_titan_text_embedding_v2(text: str, bedrock_client=None) -> list[float]:
    """
    Amazon Titan Text Embeddings V2(Bedrock)로 단일 텍스트 임베딩을 생성한다.
    응답의 embedding 또는 embeddingsByType.float를 사용한다.
    """
    import json

    client = bedrock_client if bedrock_client is not None else get_bedrock_runtime_client()
    body: dict[str, Any] = {
        "inputText": text,
        "dimensions": EMBEDDING_DIMENSION,
        "normalize": TITAN_EMBED_NORMALIZE,
        "embeddingTypes": ["float"],
    }
    response = client.invoke_model(
        modelId=BEDROCK_EMBEDDING_MODEL_ID,
        body=json.dumps(body),
        accept="application/json",
        contentType="application/json",
    )
    response_body = json.loads(response["body"].read())
    vec = response_body.get("embedding")
    if not isinstance(vec, list) or not vec:
        by_type = response_body.get("embeddingsByType") or {}
        vec = by_type.get("float")
    if not isinstance(vec, list) or not vec:
        raise RuntimeError(f"Bedrock Titan v2 임베딩 벡터를 찾을 수 없습니다: {response_body}")
    if len(vec) != EMBEDDING_DIMENSION:
        raise RuntimeError(
            f"임베딩 차원 {len(vec)}이(가) 인덱스 설정 {EMBEDDING_DIMENSION}과 다릅니다. "
            "INDEX_BODY embedding.dims 또는 EMBEDDING_DIMENSION을 맞추세요."
        )
    return vec


def attach_titan_embeddings_to_documents(
    documents: list[dict[str, Any]],
    bedrock_client=None,
) -> list[dict[str, Any]]:
    """문서 리스트에 대해 건별로 Titan Text Embeddings v2 임베딩을 생성해 embedding 필드를 채운 복사본을 반환한다."""
    br = bedrock_client if bedrock_client is not None else get_bedrock_runtime_client()
    out: list[dict[str, Any]] = []
    for doc in documents:
        text = build_embedding_text_for_product(doc)
        vec = invoke_titan_text_embedding_v2(text, bedrock_client=br)
        merged = {**doc, "embedding": vec}
        out.append(merged)
    return out


def get_opensearch_client():
    """AWS SigV4로 OpenSearch 클라이언트를 생성한다."""
    from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

    session = get_aws_boto_session()
    credentials = session.get_credentials()
    if credentials is None:
        raise RuntimeError(
            "AWS 자격 증명을 찾을 수 없습니다. "
            "AWS_ACCESS_KEY_ID·AWS_SECRET_ACCESS_KEY 환경 변수, --prompt-aws-credentials, "
            "또는 ~/.aws/credentials·IAM 역할을 설정하세요."
        )

    auth = AWSV4SignerAuth(credentials, AWS_REGION, OPENSEARCH_SERVICE)

    host = OPENSEARCH_ENDPOINT.replace("https://", "").replace("http://", "").rstrip("/")

    return OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=60,
    )


def create_index(client, index_name: str = INDEX_NAME, body: dict | None = None) -> dict[str, Any]:
    """인덱스가 없으면 생성한다. 이미 있으면 스킵."""
    if client.indices.exists(index=index_name):
        return {"acknowledged": False, "message": f"index already exists: {index_name}"}
    payload = body if body is not None else INDEX_BODY
    return client.indices.create(index=index_name, body=payload)


def delete_index(client, index_name: str = INDEX_NAME) -> dict[str, Any]:
    """인덱스 전체 삭제(개발/테스트용)."""
    if not client.indices.exists(index=index_name):
        return {"acknowledged": False, "message": f"index not found: {index_name}"}
    return client.indices.delete(index=index_name)


def sample_products() -> list[dict[str, Any]]:
    """매핑 필드에 맞춘 테스트 상품 문서 목록(embedding 없음). 색인 전 attach_titan_embeddings_to_documents 호출."""
    return [
        {
            "product_id": "SKU-10001",
            "product_name": "구찌 에이스 레더 스니커즈",
            "category": "신발 스니커즈 남성",
            "brand": {
                "ko": "구찌",
                "en": "Gucci",
                "aliases": ["구짜", "guccui", "GUCCI"],
            },
            "price": 890000.0,
            "created_at": "2025-03-15T10:00:00Z",
            "gender": "남",
            "colors": ["화이트", "그린"],
            "age": "성인",
            "size_capacity": None,
            "size_shoes": {"kr": 270.0, "eu": 42.0, "uk": 8.0, "us": 9.0},
            "description": "클래식한 가죽 스니커즈. 뒤꿈치 시그니처 웹 디테일이 돋보입니다.",
            "usage_effects": "일상 착용, 캐주얼 룩 완성",
            "keywords": ["스니커즈", "남성신발", "럭셔리", "가죽"],
            "visual_features": "흰색 가죽, 뒤쪽 그린·레드 웹 스트라이프",
            "auditory_features": "",
            "olfactory_features": "신 가죽 냄새",
            "gustatory_features": "",
            "tactile_features": "부드러운 가죽, 안정적인 착화감",
        },
        {
            "product_id": "SKU-20002",
            "product_name": "설화수 윤조 에센스 90ml",
            "category": "스킨케어 에센스",
            "brand": {
                "ko": "설화수",
                "en": "Sulwhasoo",
                "aliases": ["雪花秀"],
            },
            "price": 120000.0,
            "created_at": "2025-03-20T09:30:00Z",
            "gender": "공용",
            "colors": [],
            "age": "성인",
            "size_capacity": 90.0,
            "size_shoes": None,
            "description": "한방 성분 기반의 대표 에센스. 피부 결을 정돈하고 다음 단계 흡수를 돕습니다.",
            "usage_effects": "피부 보습, 유·수분 밸런스, 광채",
            "keywords": ["에센스", "한방", "보습", "윤조"],
            "visual_features": "갈색 유리병, 골드 캡",
            "auditory_features": "",
            "olfactory_features": "은은한 한방 향",
            "gustatory_features": "",
            "tactile_features": "물타입 제형, 빠른 흡수",
        },
        {
            "product_id": "SKU-30003",
            "product_name": "Sony WH-1000XM5 Wireless Headphones",
            "category": "가전 헤드폰 노이즈캔슬링",
            "brand": {
                "ko": "소니",
                "en": "Sony",
                "aliases": ["SONY", "쏘니"],
            },
            "price": 459000.0,
            "created_at": "2025-04-01T12:00:00Z",
            "gender": "공용",
            "colors": ["블랙", "실버"],
            "age": "성인",
            "size_capacity": None,
            "size_shoes": None,
            "description": "업계 선도 노이즈 캔슬링과 고해상도 사운드를 제공하는 무선 헤드폰입니다.",
            "usage_effects": "집중력 향상, 피로 완화, 몰입감 있는 음악 감상",
            "keywords": ["헤드폰", "노이즈캔슬링", "블루투스", "여행"],
            "visual_features": "미니멀한 오버이어 디자인, 매트 마감",
            "auditory_features": "풍부한 저음, 선명한 보컬, 주변음 모드",
            "olfactory_features": "",
            "gustatory_features": "",
            "tactile_features": "가벼운 착용감, 부드러운 이어패드",
        },
    ]


def index_sample_documents(
    client,
    documents: list[dict[str, Any]] | None = None,
    index_name: str = INDEX_NAME,
) -> dict[str, Any]:
    """전달된 문서를 bulk로 색인한다. 문서 _id는 product_id와 동일. embedding 필드는 호출 전에 채워야 한다."""
    docs = documents if documents is not None else sample_products()
    lines: list[str] = []
    import json

    for doc in docs:
        pid = doc.get("product_id")
        if not pid:
            continue
        lines.append(json.dumps({"index": {"_index": index_name, "_id": str(pid)}}))
        lines.append(json.dumps(doc, ensure_ascii=False))
    if not lines:
        return {"errors": False, "items": [], "message": "no documents"}
    body = "\n".join(lines) + "\n"
    return client.bulk(body=body)


def delete_product_by_id(
    client,
    product_id: str,
    index_name: str = INDEX_NAME,
) -> dict[str, Any]:
    """
    product_id로 문서 존재 여부를 확인한 뒤 삭제한다.
    색인 시 _id를 product_id로 맞추었으므로 단건 delete를 사용한다.
    """
    if not product_id or not str(product_id).strip():
        return {"deleted": False, "reason": "empty product_id"}

    pid = str(product_id).strip()
    if not client.exists(index=index_name, id=pid):
        return {"deleted": False, "reason": "not_found", "product_id": pid}

    result = client.delete(index=index_name, id=pid, refresh=True)
    return {
        "deleted": result.get("result") == "deleted",
        "product_id": pid,
        "raw": result,
    }


def delete_product_by_id_query(
    client,
    product_id: str,
    index_name: str = INDEX_NAME,
) -> dict[str, Any]:
    """
    _id와 무관하게 product_id 필드로 삭제(delete_by_query).
    중복 문서가 있으면 모두 삭제된다.
    """
    if not product_id or not str(product_id).strip():
        return {"deleted": False, "reason": "empty product_id"}

    pid = str(product_id).strip()
    body = {
        "query": {"term": {"product_id": pid}},
    }
    result = client.delete_by_query(
        index=index_name,
        body=body,
        refresh=True,
        conflicts="proceed",
    )
    deleted = int(result.get("deleted", 0))
    return {
        "deleted": deleted > 0,
        "deleted_count": deleted,
        "product_id": pid,
        "raw": result,
    }


def get_product_by_id(
    client,
    product_id: str,
    index_name: str = INDEX_NAME,
) -> dict[str, Any] | None:
    """product_id(_id)로 단건 조회."""
    pid = str(product_id).strip()
    if not client.exists(index=index_name, id=pid):
        return None
    return client.get(index=index_name, id=pid)


def main() -> None:
    """데모: 인덱스 생성 → 샘플 등록 → (선택) 삭제."""
    import argparse

    parser = argparse.ArgumentParser(description="ShillaDFS products OpenSearch index tools")
    parser.add_argument(
        "command",
        choices=["create", "seed", "delete", "delete-query", "recreate", "show"],
        help="create=인덱스만 생성, seed=BEDROCK Titan Text Embeddings v2 후 샘플 색인, delete=product_id로 삭제, "
        "delete-query=필드 기준 삭제, recreate=인덱스 삭제 후 재생성+Titan v2 임베딩 샘플, show=문서 조회",
    )
    parser.add_argument("--product-id", dest="product_id", default="", help="delete/show 시 사용")
    parser.add_argument(
        "--aws-access-key-id",
        default=None,
        help="AWS 액세스 키 ID(미지정 시 AWS_ACCESS_KEY_ID 환경 변수 또는 기본 체인)",
    )
    parser.add_argument(
        "--aws-secret-access-key",
        default=None,
        help="AWS 시크릿 액세스 키(미지정 시 AWS_SECRET_ACCESS_KEY 환경 변수 또는 기본 체인)",
    )
    parser.add_argument(
        "--aws-session-token",
        default=None,
        help="임시 자격 증명용 세션 토큰(선택)",
    )
    parser.add_argument(
        "--prompt-aws-credentials",
        action="store_true",
        help="키가 비어 있으면 콘솔에서 Access Key / Secret Key(·Session Token) 입력",
    )
    args = parser.parse_args()

    apply_cli_aws_credentials_to_environ(
        args.aws_access_key_id,
        args.aws_secret_access_key,
        args.aws_session_token,
    )
    if args.prompt_aws_credentials:
        prompt_aws_credentials_to_environ()

    client = get_opensearch_client()

    if args.command == "create":
        print(create_index(client))
        return

    if args.command == "recreate":
        print(delete_index(client))
        print(create_index(client))
        br = get_bedrock_runtime_client()
        docs = attach_titan_embeddings_to_documents(sample_products(), bedrock_client=br)
        print(index_sample_documents(client, documents=docs))
        return

    if args.command == "seed":
        print(create_index(client))
        br = get_bedrock_runtime_client()
        docs = attach_titan_embeddings_to_documents(sample_products(), bedrock_client=br)
        print(index_sample_documents(client, documents=docs))
        return

    if args.command == "delete":
        if not args.product_id:
            print("error: --product-id 필요", file=sys.stderr)
            sys.exit(1)
        print(delete_product_by_id(client, args.product_id))
        return

    if args.command == "delete-query":
        if not args.product_id:
            print("error: --product-id 필요", file=sys.stderr)
            sys.exit(1)
        print(delete_product_by_id_query(client, args.product_id))
        return

    if args.command == "show":
        if not args.product_id:
            print("error: --product-id 필요", file=sys.stderr)
            sys.exit(1)
        doc = get_product_by_id(client, args.product_id)
        print(doc if doc else {"found": False})


if __name__ == "__main__":
    main()
