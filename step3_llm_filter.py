import os
import json
import logging
from dotenv import load_dotenv

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

logger = logging.getLogger("LLMFilter")

# Load environment variables from .env file
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if OPENAI_API_KEY and OpenAI:
    client = OpenAI(api_key=OPENAI_API_KEY)
else:
    logger.warning("OPENAI_API_KEY is not set or openai is not installed. LLM filtering will be disabled.")
    client = None

def check_relevance(text: str) -> dict:
    """
    Evaluates whether the given text contains practical caregiving guidelines or manuals 
    using Gemini LLM.
    """
    if not client:
        return {
            "is_relevant": True, 
            "reason": "Bypassed: No API key or openai library missing.",
            "confidence_score": 10
        }

    prompt = f"""[System]
당신은 요양 보호 및 간병 실무 데이터를 수집하는 전문 에이전트입니다. 당신의 목표는 입력된 학술 논문이나 문서 내에서 '**가족 간병인 및 요양 보호사에게 실질적으로 도움이 될 만한 돌봄 팁, 대처 방법, 또는 가이드라인이 단 한 문단이라도 포함되어 있는지**' 찾아내는 것입니다.

[Evaluation Criteria (평가 기준)]
주의: 우리가 수집하는 데이터 원본은 대부분 '학술 논문'입니다. 따라서 논문의 전반적인 주제가 통계, 역학 분석, 정책 제안이더라도, 그 본문 혹은 고찰(Discussion) 부분에 간병인을 위한 "실용적인 조언이나 대처법"이 숨어있다면 반드시 유효한 문서(PASS)로 판단해야 합니다. 너무 엄격하게 자르지 말고, 간병인에게 조금이라도 쓸모가 있다면 PASS 시키세요.

✅ [포함 기준 (Inclusion) - 이 중 하나라도 포함 또는 암시되어 있으면 무조건 PASS]
1. 돌봄 요령 및 팁 (Care Tips): 개인위생(목욕, 식사 등), 이동 보조(낙상 방지), 투약 관리 등 일상적인 간병 과정에서의 실용적인 조언.
2. 행동 및 심리적 증상(BPSD) 대처법: 환자의 초조함, 공격성, 배회, 수면 장애 등에 대해 보호자가 취할 수 있는 구체적인 행동 방식이나 환경 개선 팁.
3. 문제 해결을 위한 중재 프로그램: 특정 지원 프로그램이나 교육 과정의 내용이 간략하게라도 소개되어 있어, 타 간병인이 참고할 만한 정보가 있는 경우.
4. 간병인 부담 완화 및 의사소통 전략: 간병인 자신의 스트레스 관리법, 의료진 또는 가족과의 갈등 해결/의사소통 요령.

❌ [제외 기준 (Exclusion) - 아래에 100% 해당하고 실무 팁이 절대 1글자도 없는 경우에만 FAIL]
1. 100% 순수 통계/역학 연구: "치매 발병률은 N%이다", "간병인의 스트레스 코르티솔 수치가 N 증가했다" 등 숫자 분석에만 그치고, 그래서 간병인이 '어떻게 행동해야 하는지'에 대한 제언이 아예 없는 경우.
2. 기초 의학 연구: 세포, 유전자 분석, 신약 화합물 구조 분석 등 돌봄 실무와 완전히 무관한 연구.

[Input Document]
{text}

[Extraction & Translation Rules (추출 및 번역 규칙)]
문서가 PASS 판정을 받았다면, 본문에 있는 실무 가이드라인, 대처 방법, 행동 요령을 샅샅이 찾아내어 한국어로 번역해야 합니다.
1. 오직 [Input Document]에 명시된 내용만 기반으로 작성하십시오. 본인의 의학적 지식을 덧붙이거나 상상해서 지어내지 마십시오 (No Hallucination).
2. 원문 텍스트(original_english_text)와 번역된 한국어 가이드라인(translated_korean_guideline)을 1:1 쌍으로 매핑하여 반환하십시오.
3. 문서가 FAIL인 경우 `extracted_korean_guidelines` 필드는 빈 배열 `[]` 로 반환하십시오.

[Output Format]
반드시 아래의 JSON 형식으로만 응답하십시오. (다른 마크다운이나 부연 설명은 포함하지 마십시오)

{{
  "is_relevant": true / false,
  "confidence_score": 1~10 (10이 가장 확실함),
  "matched_criteria": [포함 기준에 해당하는 키워드 또는 카테고리 배열],
  "extracted_korean_guidelines": [
    {{
      "original_english_text": "LLM이 본문에서 발췌한 영어 원문 텍스트 (예: Caregivers should lock doors at night)",
      "translated_korean_guideline": "해당 원문을 한국어로 충실하게 번역/요약한 실무 가이드라인 텍스트 (예: 배회를 방지하기 위해 밤에 문을 잠가야 합니다.)"
    }}
  ],
  "reason": "왜 이 문서가 채택(또는 제외)되었는지 평가 기준에 근거하여 1~2문장으로 설명"
}}
"""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={ "type": "json_object" },
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.1
        )
        result = json.loads(response.choices[0].message.content)
        return result
    except Exception as e:
        logger.error(f"LLM filtering failed: {e}")
        return {
            "is_relevant": True, 
            "reason": f"Error during LLM call: {e}",
            "confidence_score": 0
        }
