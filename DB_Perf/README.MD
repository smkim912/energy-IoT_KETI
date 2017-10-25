## 정량평가를 위한 DB 성능평가
### KCL에 시험평가 의뢰
### 1차년도 시험 (2017.10.25)
  - 1차년도 버전은 CHUNK_SIZE 단위로 insert를 진행
    => 성능조정을 위함, 한방에 넣으면 최고 성능    
  - Dict단위로 insert하기에 timestamp를 넣을 수 없었음
    => dict의 key를 timestamp로 쓰는 방법을 찾고 싶음
