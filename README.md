# cnu_crawler

FAISS 기반 검색을 위한 인덱스 갱신과 질의 함수가 포함된 크롤러입니다.
다른 프로젝트에서 다음과 같이 사용할 수 있습니다.

```python
from cnu_crawler import update_index, search_links

# 인덱스 갱신은 처리 결과를 문자열로 돌려줍니다.
msg = update_index()
print(msg)

# 검색 역시 문자열을 반환하여 다른 프로젝트에서 손쉽게 활용 가능합니다.
print(search_links("컴퓨터"))
```
