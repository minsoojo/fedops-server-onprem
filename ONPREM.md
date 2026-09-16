# fedops-server-onprem

온프레미스용 정제 소스와 설정 외부화 작업본입니다. 기존 Git 이력의 fork가 아닌 새 초기 이력입니다. 원본 라이선스와 저작자 표기를 유지합니다.

- 원본 범위·파일 해시: [SOURCE_PROVENANCE.json](SOURCE_PROVENANCE.json). 전달본은 일부 저장소의 부분 export입니다.
- 포함된 Dockerfile은 로컬 검증에 사용한 digest 기반 레시피입니다. .env·실제 자격정보는 포함하지 않습니다.
- Manager는 명시적인 v3 Runtime Release의 core commit `ff5f44ddea2705c8d901a54a0272f517822da8f4`와 FedOps `1.1.30.19+onprem.20260916` 조합을 허용합니다. 기존 profile의 기본 선택과 고정 commit은 유지합니다.
- Linux Manager 이미지에서 profile/bootstrap 11개와 모의 Kubernetes Deployment 검사 6개 통과. 새 버전·commit 환경변수 전달을 확인했습니다. F 비공개 Git 인증·실제 Task/FL 실행은 별도입니다.
- 기존 upstream 자동 게시 workflow는 실행하지 않도록 이관에서 제외했습니다.
- 배포 설정: https://github.com/minsoojo/fedops-deployment
