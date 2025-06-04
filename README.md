# cnu_crawler

FAISS 기반 검색을 위한 인덱스 갱신과 질의 함수가 포함된 크롤러입니다.
다른 프로젝트에서 다음과 같이 사용할 수 있습니다.

```python
from cnu_crawler import update_index, search_links
update_index()
print(search_links("컴퓨터"))
```
