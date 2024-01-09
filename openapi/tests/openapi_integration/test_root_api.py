from .helpers.helpers import request_with_validation

def test_root_api():
    response = request_with_validation(
        api='/',
        method="GET",
    )
    assert response.ok

    body = response.json()
    assert body.keys() == {'title', 'version','commit'}
    assert body['title'] == 'qdrant - vector search engine'
    assert len(body['version']) > 0
    assert len(body['commit']) == 40
