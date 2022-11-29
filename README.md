# 동네어때 – MZ 세대를 위한 서울시 동네찾기 추천 서비스
팀장 : 심정림, Data Engineer 파트장 : 정현진
팀원 : 김대환, 민홍기, 정서연, 정주연
## 1.	프로젝트 개요

### 1-1. 주제 선정 배경
![image](https://user-images.githubusercontent.com/75618206/201855266-0a1952a9-69d9-44e6-b2af-f921a367274f.png)
- 최근 우리 사회에는 MZ세대가 등장하고 있다. 이들은 디지털 원주민으로 글로벌 소비문화의 중심으로 부각되기 떄문에, 이들의 독특한 특성과 행태를 주목하고 있다.
- 이들은 유년시절부터 디지털환경에서 자라온 세대로 디지털기술을 소비활동에 적극적으로 활용하며 기성세대와는 차별화된 구매행동 양상을 보이고 있다.
- 정보화 시대의 질적 수준의 향상과 생활양식의 변화에 따라 Z세대의 특성은 획일화된 삶을 거부하며 각자 스스로의 개성을 살려 자신의 일과 여가를 즐기고 그 속에서 삶의 의미를 찾고자 하는 경향이 있다.
- 한국 주거 환경학회에 따르면,  이런 Z세대들의 주거문화를 포함한 공간의 문제, 주거의 기능은 지금까지와는 다른 라이프스타일에 맞게 디지털 정보통신이 주거기능과 결합된 새로운 주거기능으로 변모하게 될 것이다.

- 또한 주거는 우리 실생활에 높은 비중을 차지하기 때문에 다양하고 복합적인 요인에 의해 선택이 이루어진다. 한국 주거 환경학회에 따르면 향후 미래의 주거는 기존의 획일화에서 벗어나 소비자의 개성과 생활상이 적극 반영되어 지금까지와는 다른 주거의 문화가 될 것이다.
- 반면 Z세대에 대한 다양한 연구가 마케팅 분야에서 활용되고 있지만, 주택분야에서의 연구는 미비한 실정. 
- 그래서 MZ세대의 라이프 트렌드에 대한 서비스가 부족하다고 생각하여 이것이 반영된 서비스를 만들고자 했다.

### 1-2. 프로젝트 주제
- MZ 세대의 라이프 트렌드를 반영한 서울시 동네 찾기 추천 서비스

### 1-3. 수집 데이터
![image](https://user-images.githubusercontent.com/75618206/201856073-6a952435-8a5c-4d39-b959-cb63ee66b211.png)
- 공공데이터 OPEN API, 인스타 크롤링 등을 이용한 데이터 
### 1-4. 협업에 사용한 툴
- Git
    - Data Pipeline : https://github.com/cyless-hj/RESIDENCE_PJT,
    - Web Service : https://github.com/Jungrim/-FINAL_PJT_4
- Slack
- Trello : https://trello.com/b/TtIJ4wbS/finpjt4
- Docker : https://hub.docker.com/r/jhjzmdk/residence_pjt
### 1-5. 기술 스택
- DE
    - ETL 파이프라인
        - Data Lake : <img src="https://img.shields.io/badge/AWS S3-569A31?style=flat-square&logo=Amazon AWS&logoColor=white">
        - Data Warehouse, Data Mart, Operate DB : <img src="https://img.shields.io/badge/Oracle ATP-F80000?style=flat-square&logo=oracle&logoColor=white">
        - 데이터 가공 및 분산처리엔진 : <img src="https://img.shields.io/badge/Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white">
        - 배치도구 : <img src="https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=black">
- DS
    - 추천시스템 모델링 : <img src="https://img.shields.io/badge/Jupyter-F37626?style=flat-square&logo=Jupyter&logoColor=white">
- WEB SERVER
    - Web Framework : <img src="https://img.shields.io/badge/Django-092E20?style=flat-square&logo=django&logoColor=white">
    - 언어 : <img src="https://img.shields.io/badge/html5-E34F26?style=flat-square&logo=html5&logoColor=white"> <img src="https://img.shields.io/badge/css-1572B6?style=flat-square&logo=css3&logoColor=white"> <img src="https://img.shields.io/badge/javascript-F7DF1E?style=flat-square&logo=javascript&logoColor=black"> 
    - 프레임워크 : <img src="https://img.shields.io/badge/bootstrap-7952B3?style=flat-square&logo=bootstrap&logoColor=white">

### 1-6. 데이터 처리 프로세스
- ETL 파이프라인
- Data Mart
- Operate DB
- Web Service

### 1-7. Workflow

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/201856713-90d03e5c-3797-4b42-b199-f6dd243bb45a.png">
</p>

## 2. 프로젝트 수행 결과 – 데이터 파이프라인
### 2-1. 계층형 카테고리

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/201859483-ba928d8c-782d-4195-9673-00a445885b30.png">
</p>

- 관리가 용이한 형태의 카테고리 형태 : 각 객체의 카테고리가 달라지고 확장될 수 있는 가능성을 고려하여 3 Depth의 계층형으로 설계
- 서비스적으로 나눈 카테고리가 아닌 데이터 관리 측면을 고려한 카테고리
- 라이프스타일 카테고리처럼 세부카테고리로 나뉘는 경우도 있지만, 교통 카테고리처럼 세부 카테고리가 없는 경우에는 최상위 카테고리를 계승하여 DEPTH가 동일하도록 설계

### 2-2. 데이터 전처리
- 서비스에서 가장 중요한, 공통적으로 갖는 데이터는 구, 동, 좌표 데이터

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204562302-4e93af06-c0d2-498d-a41a-afb68def9dd5.png">
</p>

- 각 요소 별 위치정보 유형
    1. 지번주소 : 구, 동 Split이 가능한 형태로 간단히 구, 동 추출
    2. 도로명 주소 : 도로명 주소는 동 정보를 가지고 있지 않아 Naver API, Kakao API를 사용하여 지번주소로 변환하거나 동 데이터를 받아 사용, 혹은 위경도 좌표를 Reverse Geocoder를 이용해 주소로 변환하여 추출
    3. 좌표정보가 일반적으로 위경도를 표현하는 WGS84 좌표계가 아닌 경우 : 도로명 주소 혹은 지번주소를 Geocoder를 통해 위경도 좌표를 반환하여 사용
iii.	법정동 행정동 변환 : DS 측 모델링 행정동 기준, 동코드 맵핑을 통해 변환

### 2-3. Data Lake - Extract(추출)
- AWS S3 : 본 프로젝트는 Data Lake로 HDFS 대신 AWS S3를 사용함으로서 로컬 환경이 아닌 클라우드 환경에 Raw Data를 추출해 데이터가 공유될 수 있는 환경 구축
- IAM : Bucket 접근 권한을 부여한 IAM을 팀원 간 공유하여 클라우드 서비스를 더욱 안전하게 사용할 수 있도록 함

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204563263-4f38525c-d728-4442-b507-a67acc2b7c89.png">
</p>

- Bucket : 각 데이터 별로 디렉토리를 설정하여 저장하고 관리되도록 구성

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204563520-19996d21-a8e1-4da8-903b-a3f75cfeb19a.png">
</p>

### 2-4. Transform (가공)
- 추출한 Raw Data를 Spark를 사용해 구축한 RDB 테이블에 맞게 가공

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204563966-2eaa49c4-cd33-4484-85e7-1dc494a75257.png">
</p>

### 2-5. Data Warehouse

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204564294-c65115b9-a956-4fa1-9d4e-5e2392ffd6d4.png">
</p>

- ERD Cloud : https://www.erdcloud.com/d/QDAzQR7T8xhNF2bbS
- 데이터 웨어하우스 테이블 설계 시 고려한 점은 중복값 없이 이상현상이 생기지 않게 적재해야 한다는 것. 3정규화까지 고려하여 설계
- 각 요소가 공통적으로 갖는 시도, 구, 동 데이터를 LOC 테이블로 구성해 스타스키마 형태로 설계

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204564515-258cf732-da93-444f-81cc-14986a826fcd.png">
</p>

- 피쳐 별 테이블 유형
	1. PK,장소 명,카테고리 코드, 위경도,도로명주소, 지역코드(FK)로 구성된 가장 일반적인 테이블 유형
<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204564560-4e7513f9-7827-47c0-b2b3-14082fa07e03.png">
</p>

	2. PK,장소 명,카테고리 코드, 위경도, 도로명주소, 지역코드(FK)를 공통으로 가지며, 공통된 인스턴스 외에 특성을 살릴 수 있는 요소를 확장성을 고려하여 설계 예) 병원 – 평일 진료시간, 주말 진료시간
        - 프로젝트를 프로토타이핑 방식으로 진행했을 뿐만 아니라 추후의 확장을 고려했을 때 특성을 살릴 수 있는 데이터는 적재할 수 있도록 설계하는 것이 맞다고 판단, 각 요소의 기본 정보와 위치정보가 아닌 다른 의미를 가진 데이터는 테이블을 분할하여 설계

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204564642-e60e537f-022f-46bc-bce6-73f33e51cb9b.png">
</p>

### 2-6. Data Mart

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204565487-a1a942e7-7565-493f-8752-909c2f0d42ab.png">
</p>

- ERD Cloud : https://www.erdcloud.com/d/sQpL83KENqP9Wezdd
- 데이터마트를 설계할 때 고려한 점은 데이터가 중복이 되더라도 하나의 테이블에서 원하는 정보를 다 얻을 수 있도록 설계하는 것
- 각 요소에 대한 데이터를 데이터 사이언스 파트 측에서 요청할 경우 바로 제공할 수 있도록 각 요소별 테이블을 독립적으로 설계

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204565651-b2f57e85-26ba-4b3a-a078-d572405f0423.png">
</p>

- 통계 테이블 : 카테고리 별 동네 통계 자료로, 각 요소들이 각 동에 위치한 ‘개수’ 데이터를 적재한 테이블로 사이언스 측 모델링에 사용
    1. 카테고리 별 각 동에 위치한 개수
    2. 카테고리 별 각 동 면적 대비 위치한 개수
    3. 카테고리 별 각 동 인구수 대비 위치한 개수

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204565780-86290280-1bc1-4945-b7c4-83af43c44059.png">
</p>

### 2-7. 운영 DB

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204568028-e7d9620d-f997-41aa-ac95-a52727b1fdab.png">
</p>

- ERD Cloud : https://www.erdcloud.com/d/uRqc5p8NpvKRBinaE
- 웹서비스에 필요한 데이터를 적재한 웹서비스 관련 테이블들과, 유저 정보를 받는 유저 관련 테이블들로 구성
- 유저 관련 테이블 : 회원가입 시 받는 데이터를 적재하는 회원정보 테이블과 추천 시스템 사용 시 입력 받는 가중치 값을 적재하는 테이블로 구성
- 웹서비스 관련 테이블 : 웹서비스 제공에 필요한 데이터를 적재한 테이블들
    1. DONG_CNT : DM의 통계 테이블을 가져온 것으로 각 요소들이 각 동에 위치한 개수 데이터 적재하여 통계 그래프에 사용
    2. INFRA_ADMIN : 각 요소의 카테고리, 이름, 위치정보 데이터로 지도 시각화에 사용
    3. DONG_COORD : 각 행정동의 중심좌표 데이터로 지도의 초기 시점에 사용

### 2-8. Airflow
- 데이터의 최신화에 따라 서비스 신뢰도가 결정
    - 철거되거나 새로 생긴 요소들의 반영이 중요
    - 1달을 주기로 Airflow를 통해 데이터가 최신화 되도록 구성

- 초기 구성
    - 모든 extract class들을 하나의 그룹으로, 그리고 모든 transform, datamart, 운영DB 스크립트들 또한 각각 하나의 그룹으로 구성
    - 하지만 병렬처리 되는 작업의 수가 많아 성능 문제 동작 불가

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204566587-4f863317-ddfb-4539-9a3c-9a44b272fcf3.png">
</p>

- 재구성
    - 각 단계의 병렬처리 수를 줄여 Airflow가 문제없이 동작하도록 재구성
    - 데이터 카테고리를 기준으로 나누어 3-4개의 스크립트로 병렬처리 구성
<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204566841-9b4880ff-0e7e-4e63-9ff3-1067b9b24446.png">
	<img src="https://user-images.githubusercontent.com/75618206/204566908-8d744027-3d29-4ad1-84c8-5aba0531aed7.png">
</p>

### 2-9. Kafka

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204568372-f06025d6-bf61-4825-be20-d06cb4c551ce.png">
</p>

- Kafka - Django 연동을 통해 웹 서비스에서 유저 로그 데이터를 수집
- S3에 로그 데이터 적재

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204569001-4c28a4bc-f98e-4b26-9d62-f5ae932e107f.png">
</p>

- 수집한 데이터를 이용해 실시간 인기 동네 및 인기 카테고리 순위 구현 예정

## 3. 프로젝트 수행 결과 – 데이터 모델링

### 3-1. 추천 시스템을 위한 모델링 적용 알고리즘 및 공식

1. 컨텐츠 기반 필터링(Contents-based Filtering) : 아이템에 대한 프로필 데이터를 이용해 사용자가 선호하는 아이템과 비슷한 유형의 아이템을 추천
	1. 행정동에 대한 피쳐 별 데이터 추출
	2. 행정동의 특징을 벡터로 변환
	3. 군집화 알고리즘을 이용 : 선택한 동네의 특징과 유사한 아이템들을 선별
	4. 아이템들을 군집으로 나눈 후 아이템 A와 동일한 군집에 있는 아이템 A', B, C, D를 추천할 아이템 후보로 선정

2. K-means clustering
	1. 2차 K-means 클러스터링 진행
	2. 군집의 의미 파악
	3. 426개의 행정동 → 약 10개의 클러스터로 분류

3. Cosine similarity
	- 사용자 선호 동네 특성에 맞는 군집 추천
	- 군집 내에서 유사한 거리를 갖는 동네 우선적으로 추천
	- 거리 계산시 코사인 유사도 사용
	<br>
	- 동네 특성의 전체 방향성이 비슷하도록 사용자 선호 동네 특성과 유사한 동을 추가적으로 추천 

### 3-2. 데이터 전처리

1. MZ 세대 트렌드와 피처 가중치
	- MZ 세대가 가장 많이 사용하는 SNS인 인스타그램 크롤링을 통해 가중치 결정
	- 각 요소를 검색해 나오는 글의 해시태그를 이용
	- 각 트렌드 키워드 끼리의 연관성을 찾아내어, 지역 추천시 사용자가 선택하지 않은 요소의 가중치도 파악하여 추천 가능

![image](https://user-images.githubusercontent.com/75618206/204572864-d907c424-f6e4-4bca-b754-7155d8752c64.png)


<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204572368-4a457a6c-8a3f-4a9b-8617-ddfd08a1663a.png">
	<img src="https://user-images.githubusercontent.com/75618206/204572864-d907c424-f6e4-4bca-b754-7155d8752c64.png">
</p>

2. 카테고리 분류

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204573187-f25384ec-7504-47db-b6b7-4585beb1d4a5.png">
</p>

	1. 주거지 선정에 기본적으로 필요로 하는 부분에 대한 범주
	2. 타겟층인 MZ 세대에 대해 고려하여 라이프 스타일 등 개인 취향에 대한 범주

3. 스케일링
	1. Min-Max Scaler
		- 데이터의 분포가 min-max 맞지 않음.
		- 0에 가까운 데이터 다수 존재.
		- 왜도, 첨도 수치가 매우 높은 편.
		- 군집화 형태가 이상적이지 않음

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204574079-542d5c0b-a77d-449e-a474-66d88fd0e46b.png">
</p>

	2. Robust Scaler
		- 중앙값 0, 사분위수의 제3사분위수에서 제1사분위수를 뺀값인 IQR이 1이 되도록 변환
		- 다른 Scaler에 비해 표준화 진행 후의 데이터 형태는 더 넓게 나타남
		- Robust Scaler 이용

<p align="center">
	<img src="https://user-images.githubusercontent.com/75618206/204574147-d1497d71-b3c6-4469-a22a-ee51584f21d3.png">
</p>
