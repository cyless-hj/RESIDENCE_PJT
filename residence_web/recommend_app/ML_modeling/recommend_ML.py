import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn.cluster import KMeans
from sklearn.preprocessing import RobustScaler,MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.decomposition import PCA

import warnings

path_str = "recommend_app/data/"
tmp_df = pd.DataFrame()

def merge_area_data():
    # 행정동별 데이터 로드
    df = pd.read_csv(path_str + "행정동_컬럼추가_최종ver.csv", index_col=0)
    df.rename(columns={"인구수": "MZ_POP_CNT"}, inplace=True)

    # 인구 밀도 데이터 로드
    density_df = pd.read_excel(path_str + '인구밀도.xlsx')
    density_df['GU_DONG'] = density_df['GU'] + density_df['DONG']
    df['GU_DONG'] = df['GU'] + df['DONG']
    density_df.drop(['GU', 'DONG', 'POP', 'DENSITY'], axis=1, inplace=True)

    # 행정동 데이터, 밀도 데이터 병합
    tmp = pd.merge(df, density_df, on='GU_DONG')
    tmp.drop(['GU_DONG'], axis=1, inplace=True)

    # 컬럼 순서 정렬
    tmp = tmp[['GU', 'DONG', 'DONG_CODE', 'AREA', 'ACADEMY_NUM', 'KINDER_NUM', 'FIRE_NUM',
               'ELE_SCH_NUM', 'MID_SCH_NUM', 'HIGH_SCH_NUM', 'CCTV_NUM', 'POLICE_NUM',
               'BIKE_NUM', 'CAR_SHR_NUM', 'SUBWAY_NUM', 'SAFE_DLVR_NUM', 'DPTM_NUM',
               'ANI_HSPT_NUM', 'PHARM_NUM', 'LEISURE_NUM', 'KIDS_NUM', 'SPORT_NUM',
               'GYM_NUM', 'GOLF_NUM', 'STARBUCKS_NUM', 'MC_NUM', 'CON_NUM',
               'NOISE_VIBRATION_NUM', 'CHILD_MED_NUM', 'CAFE_NUM', 'PARK_NUM',
               'HOSPITAL_NUM', 'BUS_CNT', 'RETAIL_NUM', 'COLIVING_NUM', 'MZ_POP_CNT', 'VEGAN_CNT']]
    # 불필요 컬럼 제거
    tmp = tmp.drop(['SPORT_NUM'], axis=1)
    return tmp


def assembling_features(df):
    global tmp_df
    # 피쳐합
    tmp_df = df.copy()
    # 교통
    tmp_df['교통'] = tmp_df['SUBWAY_NUM'] + 0.93 * tmp_df['BUS_CNT'] + 0.06 * tmp_df['BIKE_NUM']
    tmp_df = tmp_df.drop(['SUBWAY_NUM', 'BUS_CNT', 'BIKE_NUM'], axis=1)

    # 교육
    tmp_df['교육'] = (0.07) * tmp_df['MID_SCH_NUM'] + (0.03) * tmp_df['HIGH_SCH_NUM'] + tmp_df['ACADEMY_NUM'] * (0.7) + (
        0.9) * tmp_df['ELE_SCH_NUM']
    tmp_df = tmp_df.drop(['MID_SCH_NUM', 'HIGH_SCH_NUM', 'ACADEMY_NUM', 'ELE_SCH_NUM'], axis=1)

    # 육아
    tmp_df['육아'] = tmp_df['CHILD_MED_NUM'] + tmp_df['KINDER_NUM']
    tmp_df = tmp_df.drop(['CHILD_MED_NUM', 'KINDER_NUM'], axis=1)

    # 치안
    tmp_df['치안'] = tmp_df['POLICE_NUM'] + tmp_df['CCTV_NUM'] + tmp_df['FIRE_NUM']
    tmp_df = tmp_df.drop(['POLICE_NUM', 'CCTV_NUM', 'FIRE_NUM'], axis=1)

    # 건강
    tmp_df['건강'] = (0.94) * tmp_df['HOSPITAL_NUM'] + tmp_df['PHARM_NUM']
    tmp_df = tmp_df.drop(['HOSPITAL_NUM', 'PHARM_NUM'], axis=1)

    # 편의시설
    tmp_df['편의시설'] = 0.04 * tmp_df['DPTM_NUM'] + 0.44 * tmp_df['CON_NUM'] + 0.25 * tmp_df['CAFE_NUM'] + 0.27 * tmp_df[
        'RETAIL_NUM']
    tmp_df = tmp_df.drop(['DPTM_NUM', 'CON_NUM', 'CAFE_NUM', 'RETAIL_NUM'], axis=1)

    tmp_df.set_index('DONG_CODE', inplace=True)

    return tmp_df


def robust_scaling(df):
    robust_scaler = RobustScaler()

    robust_scaler.fit(df)

    robust_data = robust_scaler.transform(df)
    ro_df = pd.DataFrame(robust_data)
    ro_df.index = df.index
    ro_df.columns = df.columns
    return ro_df

def minmax_scaling(df):
    minmax_scaler = MinMaxScaler()

    minmax_scaler.fit(df)

    minmax_data = minmax_scaler.transform(df)
    minmax_df = pd.DataFrame(minmax_data)
    minmax_df.index = df.index
    minmax_df.columns = df.columns
    return minmax_df

def preprocessing_df():
    area_df = merge_area_data()
    assem_df = assembling_features(area_df)

    tmp_data = assem_df.iloc[:, 3:]
    df = tmp_data.div(assem_df['AREA'], axis=0)

    max_lim_log_list = ["교통", "치안", "교육", "COLIVING_NUM", "STARBUCKS_NUM", "MC_NUM", "NOISE_VIBRATION_NUM", "VEGAN_CNT"]

    for f in max_lim_log_list:
        quan = df[f].quantile(0.95)
        df[f] = np.where(df[f] > quan, quan, df[f])
        df[f] = np.log1p(df[f])

    max_lim_list = ["LEISURE_NUM", "GOLF_NUM", "건강", "편의시설"]
    for f in max_lim_list:
        quan = df[f].quantile(0.95)
        df[f] = np.where(df[f] > quan, quan, df[f])

    ro_df = robust_scaling(df)
    ro_df = ro_df[['교통', '치안', '건강', '편의시설', '교육',
             '육아', 'MZ_POP_CNT', 'COLIVING_NUM', 'VEGAN_CNT', 'KIDS_NUM',
             'PARK_NUM', 'STARBUCKS_NUM', 'MC_NUM', 'NOISE_VIBRATION_NUM',
             'SAFE_DLVR_NUM', 'LEISURE_NUM', 'GYM_NUM', 'GOLF_NUM', 'CAR_SHR_NUM',
             'ANI_HSPT_NUM']]

    return ro_df

def first_clustering(df):
    global tmp_df
    basic_pca = PCA(n_components=2, random_state=0)
    basic_pca_transformed = basic_pca.fit_transform(df)

    # density_data = minmax_norm(density_data)
    first_kmeans = KMeans(n_clusters=4, init='k-means++', max_iter=300, random_state=0)
    first_kmeans.fit(basic_pca_transformed)

    basic_df = tmp_df.copy()
    basic_df['km_cluster'] = first_kmeans.labels_

    basic_df['pca_x'] = basic_pca_transformed[:, 0]
    basic_df['pca_y'] = basic_pca_transformed[:, 1]

    return basic_df, first_kmeans, basic_pca

def second_clustering(basic_df, df,  user_first):
    cluster_num = [3,3,2,0]
    second_cluster = basic_df[basic_df['km_cluster'] == user_first]
    cluster_data = df.loc[second_cluster.index.values]
    second_pca = PCA(n_components=2)
    second_pca_transformed = second_pca.fit_transform(cluster_data)
    second_kmeans = KMeans(n_clusters=cluster_num[user_first], init='k-means++', max_iter=400, random_state=0)
    second_kmeans.fit(second_pca_transformed)

    cluster_tmp = second_cluster.copy()
    cluster_tmp['km_cluster'] = second_kmeans.labels_
    return second_kmeans, second_pca, cluster_tmp

def create_category(df):
    first_category = []
    for column in df.columns[:6]:
        category = []
        for i in range(0,81,20):
            x = (df[column].quantile(i/100) + df[column].quantile((i+20)/100)) / 2
            category.append(x)
        first_category.append(category)

    second_category = []
    for column in df.columns[6:]:
        cate = [df[column].quantile(0.25), df[column].quantile(0.75)]
        second_category.append(cate)

    return first_category, second_category

def user_scaling(first_category, second_category, user , df):
    user_data = [0] * len(user)
    select = [0] * len(user)  # 유저의 카테고리 선택여부 저장

    for i in range(len(user[:6])):  # 첫번째 카테고리에 구간별 중앙값 부여
        if (user[i] != 0):
            user_data[i] = first_category[i][user[i] - 1]
            select[i] = 1
    for j in range(len(user[6:])):  # 두번째 카테고리에 평균을 중앙값으로 부여
        if (user[j + 6] != 0):
            user_data[j + 6] = second_category[j][1]
            select[j + 6] = 1
        else:
            user_data[j + 6] = second_category[j][0]
    user_df = pd.DataFrame(user_data, index=df.columns, columns=['user']).T
    return user_df,select


def weighting(user_df, df, select, user_name):
    weight_df = pd.read_excel('recommend_app/data/1107_가중치.xlsx')
    weight_df.rename(columns={'Unnamed: 0': '분류'}, inplace=True)
    weight_df.fillna(0, inplace=True)
    weight_df.set_index('분류', inplace=True)

    values = user_df.loc[user_name].values
    weight = weight_df[weight_df.columns].values
    w = [1] * len(weight)
    for i in range(len(weight)):
        if(select[i] == 1):
            for k in range(len(weight[i])):
                w[i] += weight[i][k]

    weighted_user_data = []
    for i in range(len(values)):
        weighted_data = values[i] * w[i]
        weighted_user_data.append(weighted_data)
    weighted_user_df = pd.DataFrame(weighted_user_data,index=df.columns,columns=['user']).T
    return weighted_user_df


# 유저 스케일 데이터 입력 시 해당 클러스터 출력 함수
def user_clustering(basic_df, df, user_scaled, first_pca, first_kmeans):
    user_pca = first_pca.transform(user_scaled)
    user_first = first_kmeans.predict(user_pca)[0]

    second_kmeans, second_pca, second_cluster = second_clustering(basic_df, df, user_first)
    user_pca_2 = second_pca.transform(user_scaled)
    user_second = second_kmeans.predict(user_pca_2)[0]
    result_cluster = second_cluster[second_cluster['km_cluster'] == user_second]
    return user_second, result_cluster

def similarity(user_df, df, user_name, num): # 유저 데이터, 유사도 측정을 위한 데이터, 유저 이름, 원하는 순위
    con_data = pd.concat([user_df.loc[[user_name]],df])
    rc_sim = cosine_similarity(con_data,con_data)
    sim_matrix = pd.DataFrame(rc_sim,columns=con_data.index).loc[[0]].T
    rank = sim_matrix[0].sort_values(ascending=False) # 유사도 순서로 정렬
    ranking = rank[1:num+1].index.tolist() # 1~n 위 리스트
    sim = rank[1:num+1].tolist() # 1~n 위 코사인 유사도
    sim_list = []
    for x in sim:
        s = (x+1) / 2 *100
        s = round(s,1)
        sim_list.append(s)
    return sim_list, ranking


def get_dong_cluster(dong_code):
    dong_cluster_data = pd.read_csv(path_str+"동별_cluster_현황.csv")
    dong_info = {1: "서울의 역사가 담긴 동네", 2: "불이 꺼지지 않는 동네", 3: "가족이 살기 좋은 동네", 4: "서울 상경 비기너 추천 동네", 5:"골목이 많은 동네", 6:"금수저 동네", 7:"슬세권 동네", 8:"조용한 동네", 9:"자연친화 동네"}
    dong_tag = {1: ["#한양","#사대문", "#청계천"] , 2:["#유동인구", "#핫플레이스", "#접근성좋음"], 3:["#교육","#육아","#가족이함께살기좋은"], 4:["#원룸이많은","#월세저렴한","#mz인구"], 5:["#주택이많은", "#주변상권좋은","#채식주의자"], 6:["#사고싶은","#없는게없는", "#빌딩이많은"], 7:["#빌라많은", "#상업시설","#편의시설많은"], 8:["#조용한","#러닝","#출퇴근용이한"], 9:["#자연","#명상","#피톤치드"]}

    cluster_num = dong_cluster_data[dong_cluster_data['DONG_CODE'] == dong_code]['km_cluster'].values[0]
    print(cluster_num)
    return dong_info[cluster_num], dong_tag[cluster_num]