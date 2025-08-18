import streamlit as st
from datetime import datetime, timedelta
import requests
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# ──────────────────────────────────────────────────────────
# 1. 페이지 설정
# ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Jira Worklog UI",
    layout="wide",
)

st.title("Jira 업무 로그 시각화")

# ──────────────────────────────────────────────────────────
# 2. 전역 설정
# ──────────────────────────────────────────────────────────
JIRA_EMAIL     = st.secrets["jira_email"]
JIRA_API_TOKEN = st.secrets["jira_token"]
JIRA_DOMAIN    = "auto-jira.atlassian.net"
AUTH           = (JIRA_EMAIL, JIRA_API_TOKEN)
HEADERS        = {"Accept": "application/json"}

DEFAULT_CATEGORIES = ["테스트", "개발", "회의", "세미나", "기타"]
ASSIGNEES = ["Jinseop Kim 김진섭", "Jaewon HUH", "서준", "권혁용", "SEOYEON KIM", "박한비"]

# ──────────────────────────────────────────────────────────
# 3. CSS 오버라이드: 흰색 배경 & 표 스타일
# ──────────────────────────────────────────────────────────


# ──────────────────────────────────────────────────────────
# 4. 헬퍼 함수
# ──────────────────────────────────────────────────────────
def parse_date(s: str):
    """'YYYY-MM-DD' 형식 문자열을 date 객체로 변환"""
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except:
        return None

def extract_text(adf: dict) -> str:
    """
    Atlassian Document Format 내 bulletList 등을
    plain text로 변환 (줄바꿈 + '-' 처리)
    """
    if not isinstance(adf, dict):
        return ""
    out = []
    def walk(nodes):
        for n in nodes:
            t = n.get("type")
            if t == "text" and "text" in n:
                out.append(n["text"])
            elif t == "bulletList":
                for li in n.get("content", []):
                    out.append("- " + extract_text(li))
            elif "content" in n:
                walk(n["content"])
    walk(adf.get("content", []))
    return "\n".join(out).strip()

def parse_comment(c):
    """
    '[분류] 내용' 포맷을 분해해서 (분류, 내용) 반환.
    매칭되지 않으면 ('기타', 전체 내용).
    """
    if not c:
        return "기타", ""
    if isinstance(c, dict):
        c = extract_text(c)
    m = re.match(r"^\s*\[(.*?)\]\s*(.*)", c)
    return (m.group(1), m.group(2)) if m else ("기타", c)

def secs_to_hms(sec: int) -> str:
    """
    초(sec)를 'Hh Mm' 형식 문자열로 변환합니다.
    (timedelta 사용, days → hours 환산, 초 단위는 생략)
    """
    td = timedelta(seconds=sec or 0)
    total_hours = td.days * 24 + td.seconds // 3600
    minutes     = (td.seconds % 3600) // 60

    parts = []
    if total_hours:
        parts.append(f"{total_hours}h")
    # 분 단위가 0이어도 최소한 "0m" 표시
    parts.append(f"{minutes}m")

    return " ".join(parts)


# ──────────────────────────────────────────────────────────
# 5. Jira API 호출: 이슈 & 워크로그
# ──────────────────────────────────────────────────────────
def get_issues(project: str, author: str, start: str, end: str):
    """
    worklogAuthor + worklogDate 조건까지 포함한 JQL로
    해당 이슈만 한 번에 조회
    """
    url = f"https://{JIRA_DOMAIN}/rest/api/3/search"
    jql = (
        f'project="{project}" '
        f'AND worklogAuthor="{author}" '
        f'AND worklogDate >= "{start}" '
        f'AND worklogDate <= "{end}"'
    )
    params = {
        "jql":       jql,
        "startAt":   0,
        "maxResults": 100,
        "fields":    "key,summary,parent"
    }
    resp = requests.get(url, auth=AUTH, headers=HEADERS, params=params)
    data = resp.json()
    # print(json.dumps(data, ensure_ascii=False, indent=2))
    return data.get("issues", [])

def get_worklogs(issue_key: str):
    """특정 이슈의 모든 워크로그 항목 조회"""
    url = f"https://{JIRA_DOMAIN}/rest/api/3/issue/{issue_key}/worklog"
    resp = requests.get(url, auth=AUTH, headers=HEADERS)
    return resp.json().get("worklogs", [])

# ──────────────────────────────────────────────────────────
# 6. 데이터 처리: 작성자 기준 워크로그 집계
# ──────────────────────────────────────────────────────────
def process_by_author(project, author_name, start_s, end_s):
    sd = parse_date(start_s)
    ed = parse_date(end_s)
    records, daily = [], {}

    # 1) 이슈는 프로젝트만 JQL로 필터 (worklog filtering은 아래에서)
    issues = get_issues(project, author_name, start_s, end_s)
    issue_keys = [it["key"] for it in issues]

    # 2) ThreadPool 으로 워크로그 API 병렬 호출
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(get_worklogs, key): key for key in issue_keys}
        for future in as_completed(futures):
            key = futures[future]
            wl_list = future.result()  # 해당 이슈의 워크로그 리스트

            # find summary+parent for this issue
            it = next(filter(lambda x: x["key"]==key, issues))
            summary = it["fields"].get("summary","") or ""
            p = it["fields"].get("parent")
            if p:
                top = p
                while top.get("fields",{}).get("parent"):
                    top = top["fields"]["parent"]
                top_sum = top["fields"].get("summary","")
            else:
                top_sum = summary

            # 3) 각각의 워크로그 항목 처리 (기존 로직)
            for wl in wl_list:
                author = wl.get("author",{}).get("displayName","")
                if author != author_name: 
                    continue
                started = wl.get("started")
                if not started: 
                    continue
                dt = datetime.fromisoformat(started.replace("Z","+00:00"))
                if not (sd <= dt.date() <= ed):
                    continue
                cat, desc = parse_comment(wl.get("comment"))
                sec = wl.get("timeSpentSeconds",0)
                date_str = dt.strftime("%Y-%m-%d")

                records.append({
                    "날짜":      date_str,
                    "업무 분류": cat,
                    # "상위 항목": top_sum,
                    "티켓":      summary,
                    "업무 내용": desc.replace("\n","<br>"),
                    "소요 시간": secs_to_hms(sec),
                    "링크":      f'https://{JIRA_DOMAIN}/browse/{key}'
                })

                daily.setdefault(date_str, {c:0 for c in DEFAULT_CATEGORIES})
                daily[date_str].setdefault("전체 총 시간", 0)
                key_cat = cat if cat in DEFAULT_CATEGORIES else "기타"
                daily[date_str][key_cat]      += sec
                daily[date_str]["전체 총 시간"] += sec

    # (3) 총합 계산
    total = {c: sum(daily[d].get(c, 0) for d in daily) for c in DEFAULT_CATEGORIES}
    total["전체 총 시간"] = sum(daily[d]["전체 총 시간"] for d in daily)

    # (4) 포맷된 딕셔너리 반환
    #     일별은 날짜 오름차순으로 정렬해서 반환
    daily_fmt = {
        d: {k: secs_to_hms(v) for k, v in daily[d].items()}
        for d in sorted(daily)
    }
    total_fmt = {k: secs_to_hms(v) for k, v in total.items()}

    return records, daily_fmt, total_fmt

# ──────────────────────────────────────────────────────────
# 7. UI: 사이드바 입력 및 조회 실행
# ──────────────────────────────────────────────────────────
st.sidebar.subheader("담당자 관리")
raw = st.sidebar.text_area(
    "담당자 목록 (콤마로 구분)", 
    value=",".join(ASSIGNEES), 
    help="콤마(,)로 구분하여 입력하세요. 변경 즉시 아래 ‘조회 설정’에 반영됩니다."
)
# 공백 제거 후 split
ASSIGNEES = [s.strip() for s in raw.split(",") if s.strip()]

project    = st.sidebar.text_input("프로젝트 키", value="VTS")
author_sel = st.sidebar.selectbox("담당자", options=ASSIGNEES)
col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.sidebar.date_input("시작 날짜", datetime.today() - timedelta(days=1))
with col2:
    end_date   = st.sidebar.date_input("종료 날짜", datetime.today())

if st.sidebar.button("조회 실행"):
    records, daily, total = process_by_author(
        project,
        author_sel,
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
    
    # 1) 일별 업무 내용 기록
    st.subheader("1. 일별 업무 내용 기록")

    # 1-1) records 가 비어 있으면 얼리 리턴
    if not records:
        st.info("조회된 업무 로그가 없습니다.")
    else:
        # 기대하는 컬럼 순서
        expected_cols = [
            "날짜","업무 분류",
            #"상위 항목",
            "티켓",
            "업무 내용","소요 시간","링크"
        ]
        
        # DataFrame 생성
        df = pd.DataFrame(records)
        
        # 누락된 컬럼이 있으면 빈 문자열로 추가
        for col in expected_cols:
            if col not in df.columns:
                df[col] = ""
        
        # 원하는 순서대로 재배치
        df = df[expected_cols]
        
        # 날짜 내림차순 정렬
        df["날짜"] = pd.to_datetime(df["날짜"])
        df = df.sort_values("날짜", ascending=True)
        df["날짜"] = df["날짜"].dt.strftime("%Y-%m-%d")
        
        # 업무 내용: '<br>' → 줄바꿈 리스트로 변환
        df["업무 내용"] = (
            df["업무 내용"]
              .str.replace("<br>", "\n")
              .str.split("\n")
        )
        
        # DataFrame 렌더링
        st.dataframe(
            df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "업무 내용": st.column_config.ListColumn(
                    "업무 내용",
                    width="large",
                    help="각 줄이 줄바꿈 리스트로 표시됩니다."
                ),
                "링크": st.column_config.LinkColumn(
                    "링크",
                    display_text="바로가기",
                ),
            }
        )
        
        # 업무 내용: '<br>' → 실제 개행문자로 (TextColumn 에서 wrap_text=True 로 보여줌)
        # df["업무 내용"] = df["업무 내용"].str.replace("<br>", "\n")
        # st.dataframe(
        #     df,
        #     hide_index=True,
        #     use_container_width=True,
        #     column_config={
        #         "업무 내용": st.column_config.TextColumn(
        #             "업무 내용",
        #             width="large"
        #         ),
        #         "링크": st.column_config.LinkColumn(
        #             "링크",
        #             display_text="바로가기"
        #         ),
        #     }
        # )
    # 2) 개인별 총 업무 시간 집계
    st.subheader("2. 개인별 총 업무 시간 집계")
    df2 = pd.DataFrame.from_dict(daily, orient="index")
    df2.index.name = "날짜"
    st.dataframe(df2)

    # 3) 총합
    st.subheader("3. 총합")
    st.dataframe(pd.DataFrame([total]))
