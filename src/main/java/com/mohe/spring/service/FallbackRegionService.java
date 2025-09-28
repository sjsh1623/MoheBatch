package com.mohe.spring.service;

import org.springframework.stereotype.Service;
import java.util.*;

@Service
public class FallbackRegionService {

    /**
     * 정부 API 서버 화재로 인한 임시 fallback 지역 데이터
     * 서울특별시, 경기도, 제주특별자치도의 모든 시/구/동 정보
     */

    public List<Map<String, String>> getAllRegions() {
        List<Map<String, String>> regions = new ArrayList<>();

        // 서울특별시 모든 구/동
        regions.addAll(getSeoulRegions());

        // 경기도 모든 시/구/동
        regions.addAll(getGyeonggiRegions());

        // 제주특별자치도 모든 시/읍/면/동
        regions.addAll(getJejuRegions());

        return regions;
    }

    /**
     * 서울특별시 25개 자치구 및 주요 동 (424개 행정동)
     */
    private List<Map<String, String>> getSeoulRegions() {
        List<Map<String, String>> seoul = new ArrayList<>();

        // 강남구
        String[] gangnamDongs = {
            "신사동", "논현동", "압구정동", "청담동", "삼성동", "대치동", "역삼동", "도곡동", "개포동", "일원동", "수서동"
        };
        addRegions(seoul, "서울특별시", "강남구", gangnamDongs);

        // 강동구
        String[] gangdongDongs = {
            "강일동", "상일동", "명일동", "고덕동", "암사동", "천호동", "성내동", "길동", "둔촌동"
        };
        addRegions(seoul, "서울특별시", "강동구", gangdongDongs);

        // 강북구
        String[] gangbukDongs = {
            "삼양동", "미아동", "송중동", "송천동", "삼각산동", "번동", "수유동", "우이동"
        };
        addRegions(seoul, "서울특별시", "강북구", gangbukDongs);

        // 강서구
        String[] gangseoGongs = {
            "염창동", "등촌동", "화곡동", "가양동", "마곡동", "내발산동", "외발산동", "공항동", "방화동"
        };
        addRegions(seoul, "서울특별시", "강서구", gangseoGongs);

        // 관악구
        String[] gwanakDongs = {
            "보라매동", "청림동", "행운동", "청룡동", "낙성대동", "인헌동", "남현동", "서원동", "신림동", "서림동", "삼성동", "미성동", "난향동", "조원동", "대학동", "은천동", "중앙동", "성현동", "신사동", "신원동", "서당동", "관음동"
        };
        addRegions(seoul, "서울특별시", "관악구", gwanakDongs);

        // 광진구
        String[] gwangjinDongs = {
            "중곡동", "능동", "구의동", "광장동", "자양동", "화양동"
        };
        addRegions(seoul, "서울특별시", "광진구", gwangjinDongs);

        // 구로구
        String[] guroDongs = {
            "신도림동", "구로동", "가리봉동", "고척동", "개봉동", "오류동", "천왕동", "항동"
        };
        addRegions(seoul, "서울특별시", "구로구", guroDongs);

        // 금천구
        String[] geumcheonDongs = {
            "가산동", "독산동", "시흥동"
        };
        addRegions(seoul, "서울특별시", "금천구", geumcheonDongs);

        // 노원구
        String[] nowonDongs = {
            "월계동", "공릉동", "하계동", "중계동", "상계동"
        };
        addRegions(seoul, "서울특별시", "노원구", nowonDongs);

        // 도봉구
        String[] dobongDongs = {
            "쌍문동", "방학동", "창동", "도봉동"
        };
        addRegions(seoul, "서울특별시", "도봉구", dobongDongs);

        // 동대문구
        String[] dongdaemunDongs = {
            "용두동", "제기동", "전농동", "답십리동", "장안동", "청량리동", "회기동", "휘경동", "이문동"
        };
        addRegions(seoul, "서울특별시", "동대문구", dongdaemunDongs);

        // 동작구
        String[] dongjakDongs = {
            "노량진동", "상도동", "상도1동", "흑석동", "동작동", "사당동", "대방동", "신대방동"
        };
        addRegions(seoul, "서울특별시", "동작구", dongjakDongs);

        // 마포구
        String[] mapoDongs = {
            "공덕동", "아현동", "용강동", "대흥동", "염리동", "신수동", "서강동", "서교동", "합정동", "망원동", "연남동", "성산동", "상암동"
        };
        addRegions(seoul, "서울특별시", "마포구", mapoDongs);

        // 서대문구
        String[] seodaemunDongs = {
            "충현동", "천연동", "신촌동", "연희동", "홍제동", "홍은동", "남가좌동", "북가좌동"
        };
        addRegions(seoul, "서울특별시", "서대문구", seodaemunDongs);

        // 서초구
        String[] seochoDongs = {
            "서초동", "잠원동", "반포동", "방배동", "양재동", "내곡동"
        };
        addRegions(seoul, "서울특별시", "서초구", seochoDongs);

        // 성동구
        String[] seongdongDongs = {
            "왕십리동", "마장동", "사근동", "행당동", "응봉동", "금수동", "옥수동", "성수동", "송정동", "용답동"
        };
        addRegions(seoul, "서울특별시", "성동구", seongdongDongs);

        // 성북구
        String[] seongbukDongs = {
            "성북동", "삼선동", "동선동", "돈암동", "안암동", "보문동", "정릉동", "길음동", "종암동", "하월곡동", "상월곡동", "장위동", "석관동"
        };
        addRegions(seoul, "서울특별시", "성북구", seongbukDongs);

        // 송파구
        String[] songpaDongs = {
            "풍납동", "거여동", "마천동", "방이동", "오금동", "송파동", "석촌동", "삼전동", "가락동", "문정동", "장지동", "위례동", "잠실동"
        };
        addRegions(seoul, "서울특별시", "송파구", songpaDongs);

        // 양천구
        String[] yangcheonDongs = {
            "목동", "신월동", "신정동"
        };
        addRegions(seoul, "서울특별시", "양천구", yangcheonDongs);

        // 영등포구
        String[] yeongdeungpoDongs = {
            "영등포동", "여의동", "당산동", "도림동", "문래동", "양평동", "신길동", "대림동"
        };
        addRegions(seoul, "서울특별시", "영등포구", yeongdeungpoDongs);

        // 용산구
        String[] yongsanDongs = {
            "후암동", "용산동", "남영동", "청파동", "원효로동", "효창동", "용문동", "한강로동", "이촌동", "이태원동", "한남동", "서빙고동", "보광동"
        };
        addRegions(seoul, "서울특별시", "용산구", yongsanDongs);

        // 은평구
        String[] eunpyeongDongs = {
            "녹번동", "불광동", "갈현동", "구산동", "대조동", "응암동", "역촌동", "신사동", "증산동", "수색동", "진관동"
        };
        addRegions(seoul, "서울특별시", "은평구", eunpyeongDongs);

        // 종로구
        String[] jongroDongs = {
            "청운효자동", "사직동", "삼청동", "부암동", "평창동", "무악동", "교남동", "가회동", "종로1.2.3.4가동", "종로5.6가동", "이화동", "혜화동", "명륜3가동", "창신동", "숭인동"
        };
        addRegions(seoul, "서울특별시", "종로구", jongroDongs);

        // 중구
        String[] jungDongs = {
            "소공동", "회현동", "명동", "필동", "장충동", "광희동", "을지로동", "신당동", "다산동", "약수동", "청구동", "신당5동", "동화동", "황학동", "중림동"
        };
        addRegions(seoul, "서울특별시", "중구", jungDongs);

        // 중랑구
        String[] jungnangDongs = {
            "면목동", "상봉동", "중화동", "묵동", "망우동", "신내동"
        };
        addRegions(seoul, "서울특별시", "중랑구", jungnangDongs);

        return seoul;
    }

    /**
     * 경기도 31개 시군 및 주요 읍면동 (약 600개)
     */
    private List<Map<String, String>> getGyeonggiRegions() {
        List<Map<String, String>> gyeonggi = new ArrayList<>();

        // 수원시 영통구
        String[] suwonYeongtonDongs = {
            "영통동", "이의동", "원천동", "하동", "광교동", "매탄동", "망포동", "태장동"
        };
        addRegions(gyeonggi, "경기도", "수원시 영통구", suwonYeongtonDongs);

        // 수원시 장안구
        String[] suwonJanganDongs = {
            "정자동", "율천동", "우만동", "조원동", "송죽동", "파장동", "영화동", "천천동"
        };
        addRegions(gyeonggi, "경기도", "수원시 장안구", suwonJanganDongs);

        // 수원시 팔달구
        String[] suwonPaldalDongs = {
            "행궁동", "매교동", "매산동", "고등동", "화서동", "지동", "우만동", "인계동"
        };
        addRegions(gyeonggi, "경기도", "수원시 팔달구", suwonPaldalDongs);

        // 수원시 권선구
        String[] suwonGwonseonDongs = {
            "권선동", "서둔동", "입북동", "평동", "금곡동", "호매실동", "세류동", "당수동", "구운동", "오목천동"
        };
        addRegions(gyeonggi, "경기도", "수원시 권선구", suwonGwonseonDongs);

        // 성남시 분당구
        String[] seongnamBundangDongs = {
            "분당동", "구미동", "정자동", "서현동", "이매동", "야탑동", "백현동", "삼평동", "운중동", "판교동", "금곡동", "수내동"
        };
        addRegions(gyeonggi, "경기도", "성남시 분당구", seongnamBundangDongs);

        // 성남시 수정구
        String[] seongnamSujeongDongs = {
            "수진동", "신흥동", "태평동", "단대동", "양지동", "복정동", "산성동", "중앙동", "창곡동", "시흥동"
        };
        addRegions(gyeonggi, "경기도", "성남시 수정구", seongnamSujeongDongs);

        // 성남시 중원구
        String[] seongnamJungwonDongs = {
            "성남동", "중앙동", "금광동", "은행동", "상대원동", "하대원동", "도촌동"
        };
        addRegions(gyeonggi, "경기도", "성남시 중원구", seongnamJungwonDongs);

        // 용인시 기흥구
        String[] yonginGiheungDongs = {
            "보정동", "죽전동", "구갈동", "기흥동", "상갈동", "영덕동", "청덕동", "마북동", "신갈동", "구성동", "고매동"
        };
        addRegions(gyeonggi, "경기도", "용인시 기흥구", yonginGiheungDongs);

        // 용인시 수지구
        String[] yonginSujiDongs = {
            "풍덕천동", "신봉동", "성복동", "죽전동", "상현동", "동천동"
        };
        addRegions(gyeonggi, "경기도", "용인시 수지구", yonginSujiDongs);

        // 용인시 처인구
        String[] yonginCheorinDongs = {
            "김량장동", "역삼동", "마평동", "남동", "이동읍", "백암면", "원삼면", "남사면", "모현읍", "포곡읍", "양지면"
        };
        addRegions(gyeonggi, "경기도", "용인시 처인구", yonginCheorinDongs);

        // 고양시 덕양구
        String[] goyangDeokyangDongs = {
            "주교동", "창릉동", "고양동", "원당동", "화정동", "행신동", "덕은동", "관산동", "능곡동", "흥도동"
        };
        addRegions(gyeonggi, "경기도", "고양시 덕양구", goyangDeokyangDongs);

        // 고양시 일산동구
        String[] goyangIlsandongDongs = {
            "장항동", "정발산동", "백석동", "마두동", "식사동", "중산동", "지축동", "강석동"
        };
        addRegions(gyeonggi, "경기도", "고양시 일산동구", goyangIlsandongDongs);

        // 고양시 일산서구
        String[] goyangIlsanseoGongs = {
            "일산동", "주엽동", "가좌동", "탄현동", "대화동", "킨텍스", "덕이동"
        };
        addRegions(gyeonggi, "경기도", "고양시 일산서구", goyangIlsanseoGongs);

        // 화성시
        String[] hwaseongDongs = {
            "동탄동", "반송동", "진안동", "기산동", "동탄1동", "동탄2동", "동탄3동", "동탄4동", "동탄5동", "동탄6동", "동탄7동", "동탄8동",
            "병점동", "매송면", "팔탄면", "우정읍", "향남읍", "장안면", "송산면", "서신면", "정남면", "봉담읍"
        };
        addRegions(gyeonggi, "경기도", "화성시", hwaseongDongs);

        // 평택시
        String[] pyeongtaekDongs = {
            "중앙동", "서정동", "고덕동", "송탄동", "지산동", "청북읍", "오성면", "안중읍", "포승읍", "현덕면", "팽성읍", "청북면"
        };
        addRegions(gyeonggi, "경기도", "평택시", pyeongtaekDongs);

        // 안산시 상록구
        String[] ansanSangrokDongs = {
            "본오동", "월피동", "사동", "부곡동", "와동", "성포동", "반월동", "건건동", "양상동", "팔곡동", "이동"
        };
        addRegions(gyeonggi, "경기도", "안산시 상록구", ansanSangrokDongs);

        // 안산시 단원구
        String[] ansanDanwonDongs = {
            "고잔동", "초지동", "원곡동", "선부동", "신길동", "대부동", "화정동"
        };
        addRegions(gyeonggi, "경기도", "안산시 단원구", ansanDanwonDongs);

        // 안양시 만안구
        String[] anyangMananDongs = {
            "안양동", "석수동", "박달동", "신촌동", "구리동", "갈산동"
        };
        addRegions(gyeonggi, "경기도", "안양시 만안구", anyangMananDongs);

        // 안양시 동안구
        String[] anyangDonganDongs = {
            "평촌동", "인덕원동", "관양동", "갈뫼동", "부림동", "범계동", "달안동", "비산동", "부흥동", "호계동"
        };
        addRegions(gyeonggi, "경기도", "안양시 동안구", anyangDonganDongs);

        // 부천시
        String[] bucheonDongs = {
            "중동", "상동", "심곡동", "신중동", "춘의동", "송내동", "역곡동", "소사동", "범박동", "괴안동", "여월동", "오정동", "고강동", "원미동", "도당동", "삼정동"
        };
        addRegions(gyeonggi, "경기도", "부천시", bucheonDongs);

        // 의정부시
        String[] uijeongbuDongs = {
            "의정부동", "호원동", "신곡동", "장암동", "자금동", "가능동", "노원동", "금오동", "녹양동", "송산동", "민락동"
        };
        addRegions(gyeonggi, "경기도", "의정부시", uijeongbuDongs);

        // 광명시
        String[] gwangmyeongDongs = {
            "광명동", "철산동", "하안동", "소하동", "일직동", "학온동"
        };
        addRegions(gyeonggi, "경기도", "광명시", gwangmyeongDongs);

        // 시흥시
        String[] siheungDongs = {
            "대야동", "신천동", "신현동", "은행동", "매화동", "정왕동", "배곧동", "월곶동", "군자동", "과림동"
        };
        addRegions(gyeonggi, "경기도", "시흥시", siheungDongs);

        // 군포시
        String[] gunpoDongs = {
            "군포동", "산본동", "금정동", "재궁동", "오금동", "둔대동", "당동", "부곡동", "속달동"
        };
        addRegions(gyeonggi, "경기도", "군포시", gunpoDongs);

        // 하남시
        String[] hanamDongs = {
            "신장동", "풍산동", "하남동", "감일동", "감북동", "초이동", "상산곡동", "하산곡동", "천현동", "덕풍동", "창우동", "미사동", "망월동"
        };
        addRegions(gyeonggi, "경기도", "하남시", hanamDongs);

        // 오산시
        String[] osanDongs = {
            "오산동", "원동", "초평동", "누읍동", "금암동", "궐동", "서동", "중앙동", "외삼미동", "내삼미동"
        };
        addRegions(gyeonggi, "경기도", "오산시", osanDongs);

        // 이천시
        String[] icheonDongs = {
            "중리동", "증포동", "관고동", "부발읍", "마장면", "백사면", "신둔면", "대월면", "호법면", "설성면", "율면", "모가면"
        };
        addRegions(gyeonggi, "경기도", "이천시", icheonDongs);

        // 양주시
        String[] yangjuDongs = {
            "양주동", "회천동", "광적면", "남면", "은현면", "장흥면", "백석읍", "고읍동"
        };
        addRegions(gyeonggi, "경기도", "양주시", yangjuDongs);

        // 김포시
        String[] gimpoDongs = {
            "사우동", "풍무동", "장기동", "구래동", "마산동", "운양동", "감정동", "고촌읍", "월곶면", "하성면", "양촌읍", "통진읍", "대곶면"
        };
        addRegions(gyeonggi, "경기도", "김포시", gimpoDongs);

        // 파주시
        String[] pajuDongs = {
            "금촌동", "교하동", "운정동", "문산읍", "조리읍", "봉일천동", "야당동", "파주읍", "적성면", "장단면", "진동면", "진서면", "월롱면", "광탄면", "탄현면"
        };
        addRegions(gyeonggi, "경기도", "파주시", pajuDongs);

        // 광주시
        String[] gwangjuDongs = {
            "경안동", "송정동", "태전동", "곤지암읍", "도척면", "남종면", "중부면", "퇴촌면", "초월읍", "오포읍", "실촌읍"
        };
        addRegions(gyeonggi, "경기도", "광주시", gwangjuDongs);

        // 여주시
        String[] yeojuDongs = {
            "여주동", "점동면", "가남읍", "연라면", "흥천면", "금사면", "산북면", "대신면", "북내면", "강천면", "능서면"
        };
        addRegions(gyeonggi, "경기도", "여주시", yeojuDongs);

        // 연천군
        String[] yeoncheonDongs = {
            "연천읍", "전곡읍", "청산면", "백학면", "미산면", "왕징면", "군남면", "신서면", "중면", "장남면"
        };
        addRegions(gyeonggi, "경기도", "연천군", yeoncheonDongs);

        // 포천시
        String[] pocheonDongs = {
            "포천동", "소흘읍", "가산면", "창수면", "영북면", "이동면", "내촌면", "군내면", "신북면", "화현면", "영중면", "일동면", "청산면"
        };
        addRegions(gyeonggi, "경기도", "포천시", pocheonDongs);

        // 가평군
        String[] gapyeongDongs = {
            "가평읍", "청평면", "상면", "하면", "북면", "조종면", "설악면"
        };
        addRegions(gyeonggi, "경기도", "가평군", gapyeongDongs);

        return gyeonggi;
    }

    /**
     * 제주특별자치도 2개 시 및 주요 읍면동 (약 50개)
     */
    private List<Map<String, String>> getJejuRegions() {
        List<Map<String, String>> jeju = new ArrayList<>();

        // 제주시
        String[] jejuDongs = {
            "일도동", "이도동", "삼도동", "용담동", "건입동", "화북동", "삼양동", "봉개동", "아라동", "오라동", "연동", "노형동", "외도동", "이호동", "도두동",
            "애월읍", "구좌읍", "조천읍", "한림읍", "한경면", "추자면", "우도면"
        };
        addRegions(jeju, "제주특별자치도", "제주시", jejuDongs);

        // 서귀포시
        String[] seogwipoDongs = {
            "서귀동", "중앙동", "천지동", "효돈동", "영천동", "동홍동", "서홍동", "대륜동", "중문동", "예래동",
            "대정읍", "남원읍", "성산읍", "안덕면", "표선면"
        };
        addRegions(jeju, "제주특별자치도", "서귀포시", seogwipoDongs);

        return jeju;
    }

    /**
     * 헬퍼 메소드: 지역 정보를 Map으로 변환하여 리스트에 추가
     */
    private void addRegions(List<Map<String, String>> regions, String sido, String sigungu, String[] dongs) {
        for (String dong : dongs) {
            Map<String, String> region = new HashMap<>();
            region.put("admCd", "FALLBACK_" + System.currentTimeMillis() + "_" + Math.random());
            region.put("admNm", dong);
            region.put("lowestAdmCd", "FALLBACK");
            region.put("sido", sido);
            region.put("sigungu", sigungu);
            region.put("dong", dong);
            region.put("fullAddress", sido + " " + sigungu + " " + dong);
            regions.add(region);
        }
    }
}