from django.shortcuts import render, redirect
from django.http import JsonResponse
import json


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity
import warnings
import os

from recommend_app.models import DongCnt, InfraAdmin, DongCoord
from recommend_app.forms import WeightsForm
import sys
# sys.path.append("C:/workspaces/FINAL_PJT_4_DS&DE/MZ_recommend_system/recommend_app/ML_modeling")

import recommend_app.ML_modeling.recommend_ML as RML
import json
import ast

def index(request):
    return render(request, 'recommend_app/index.html')

def basicSelect(request):

    if not request.user.is_authenticated:
        return redirect('accounts/login')
    if request.method == 'POST':
        form = WeightsForm(request.POST)
        if form.is_valid():
            # 넘어온 데이터 db에 저장
            form.save()
        req_dict = request.POST.dict()
        user = [int(i) for i in list(req_dict.values())[1:-1]]

        df = RML.preprocessing_df()
        graph_data = RML.minmax_scaling(df)

        basic_df, first_kmeans, first_pca = RML.first_clustering(df)
        first_category, second_category = RML.create_category(df)
        user_df, select = RML.user_scaling(first_category, second_category, user, df)
        weighted_user_df = RML.weighting(user_df, df, select, 'user')
        user_scaled = [weighted_user_df.loc['user'].values]
        user_group, user_include_df = RML.user_clustering(basic_df, df, user_scaled, first_pca, first_kmeans)
        sim_list, result_dong_list = RML.similarity(user_df, df.loc[user_include_df.index.values], "user", 3)
        recommend_dong_list = user_include_df.loc[result_dong_list]['DONG'].values
        recommend_gu_list = user_include_df.loc[result_dong_list]['GU'].values
        recommend_code_list = user_include_df.loc[result_dong_list].index.values
        graph_data_list = graph_data.loc[result_dong_list].values.tolist()
        # result = {"dong": recommend_dong_list, "gu" : recommend_gu_list, "code" : recommend_code_list, "weight_user": user}
        result = zip(recommend_gu_list, recommend_dong_list, recommend_code_list,sim_list)

        title, tags = RML.get_dong_cluster(result_dong_list[0])

        return render(request, 'recommend_app/recommend_result.html', {'result': result,'cluster_data' : {'title' : title,"tags" : tags},'graph_data' : graph_data_list})
    else:
        form = WeightsForm()


    return render(request, 'recommend_app/recommendation.html', {'form':form})


def categoryRanking(request):
    if not request.user.is_authenticated:
        return redirect('accounts/login')
    dict_list = []
    cate_list = ['transportation', 'safety', 'noise_vibration_num', 'leisure_num', 'gym_num', 'golf_num', 'park_num', 'facilities', 'medical', 'starbucks_num', 'mc_num', 'vegan_cnt', 'coliving_num', 'education', 'parenting', 'kids_num', 'ani_hspt_num', 'safe_dlvr_num', 'car_shr_num', 'mz_pop_cnt']
    
    for cate in cate_list:
        cnt_list = []
        dong_code_list = []
        dong_list = []
        gu_list = []
        dong_cnt = DongCnt.objects.filter(std_day__exact='2022-11-08').values_list(cate, flat=True).order_by('-' + cate).all()
        dong_code = DongCnt.objects.filter(std_day__exact='2022-11-08').values_list('dong_code', flat=True).order_by('-' + cate).all()
        dong = DongCnt.objects.filter(std_day__exact='2022-11-08').values_list('dong', flat=True).order_by('-' + cate).all()
        gu = DongCnt.objects.filter(std_day__exact='2022-11-08').values_list('gu', flat=True).order_by('-' + cate).all()

        for i in range(0, 5):
            cnt_list.append(dong_cnt[i])
            dong_code_list.append(dong_code[i])
            dong_list.append(dong[i])
            gu_list.append(gu[i])
            
        dictionary = {'category':cate, 'dong_code':dong_code_list, 'gu':gu_list, 'dong':dong_list, 'cnt':cnt_list}
        dict_list.append(dictionary)
    print(dict_list)

    return render(request, 'recommend_app/category_ranking.html', {'ranking':dict_list})

def introduction(request):
    return render(request, 'recommend_app/introduction.html')

def dongDetail(request):

    data = request.POST['dong_info']
    dong_info = data.split("@")
    dong_info_len = len(dong_info)

    # dong_info[0] : 구 이름
    # dong_info[1] : 동 이름
    # dong_info[2] : 동 코드
    
    graph_cate_list = ['교통', '치안', '건강', '편의시설', '교육', '육아', 'MZ인구', '코리빙',\
       '채식', '키즈카페', '공원', '스타벅스', '맥도날드',\
       '조용한동네', '안심택배', '레저시설', '헬스장',\
       '골프', '카쉐어링', '동물병원']
    
    df = RML.preprocessing_df()
    graph_data = RML.minmax_scaling(df)
    graph_mean_data = graph_data.mean()
    graph_mean_data.index = graph_cate_list
    graph_mean_dict = graph_mean_data.to_dict()
    gu_name = dong_info[0]
    dong_name = dong_info[1]
    if dong_info_len == 5:
        select_idx = int(dong_info[4])
        graph_data = ast.literal_eval(dong_info[3])[select_idx]

    else:
        dong_code = int(dong_info[2])
        graph_data = df.loc[dong_code].values


    graph_dict = dict(zip(graph_cate_list, graph_data))
    sorted_graph_data = sorted(graph_dict.items(), key=lambda x: x[1], reverse=True)
    key_list = []
    value_list = []
    for i,j in sorted_graph_data:
        key_list.append(i)
        value_list.append(j)


    dict_list = []
    cate_list = ['transportation', 'safety', 'noise_vibration_num', 'leisure_num', 'gym_num', 'golf_num', 'park_num', 'facilities', 'medical', 'starbucks_num', 'mc_num', 'vegan_cnt', 'coliving_num', 'education', 'parenting', 'kids_num', 'ani_hspt_num', 'safe_dlvr_num', 'car_shr_num', 'mz_pop_cnt']
    
    # for cate in cate_list:
    #     infra_name = InfraAdmin.objects.filter(std_day__exact='2022-10-27').filter(cate__exact=cate).filter(gu__exact=gu_name).filter(dong__exact=dong_name).values_list('name', flat=True).order_by('-name').all()
    #     infra_lat = InfraAdmin.objects.filter(std_day__exact='2022-10-27').filter(cate__exact=cate).filter(gu__exact=gu_name).filter(dong__exact=dong_name).values_list('lat', flat=True).order_by('-name').all()
    #     infra_lon = InfraAdmin.objects.filter(std_day__exact='2022-10-27').filter(cate__exact=cate).filter(gu__exact=gu_name).filter(dong__exact=dong_name).values_list('lon', flat=True).order_by('-name').all()
    #     name_list = []
    #     lat_list = []
    #     lon_list = []
    #     for i in range(len(infra_name)):
    #             name_list.append(infra_name[i])
    #             lat_list.append(float(infra_lat[i]))
    #             lon_list.append(float(infra_lon[i]))
    #     dictionary = {'category':cate, 'name':name_list, 'lat':lat_list, 'lon':lon_list}

    #     dict_list.append(dictionary)
    dong_coord = []
    dong_lat = DongCoord.objects.filter(gu__exact=gu_name).filter(dong__exact=dong_name).values_list('lat', flat=True).all()
    dong_lon = DongCoord.objects.filter(gu__exact=gu_name).filter(dong__exact=dong_name).values_list('lon', flat=True).all()
    for i in range(len(dong_lat)):
            dong_coord.append(float(dong_lat[i]))
            dong_coord.append(float(dong_lon[i]))
    print(dong_coord)
    print(type(dong_coord))

    dong_coordinate = {'lat': dong_coord[0], 'lon': dong_coord[1]}
    data = {"dong_name" : dong_name, "gu_name" : gu_name}

    return render(request, 'recommend_app/dong_detail.html',{'data': data, 'dong_coordinate': dong_coordinate, 'graph_key' : key_list, 'graph_value' : value_list, 'graph_mean_dict':graph_mean_dict})


def facility_info(request):
    dong_name = request.GET['dong_name']
    gu_name = request.GET['gu_name']
    cate = request.GET['cate']

    infra_name = InfraAdmin.objects.filter(std_day__exact='2022-10-27').filter(cate__exact=cate).filter(gu__exact=gu_name).filter(dong__exact=dong_name).values_list('name', flat=True).order_by('-name').all()
    infra_lat = InfraAdmin.objects.filter(std_day__exact='2022-10-27').filter(cate__exact=cate).filter(gu__exact=gu_name).filter(dong__exact=dong_name).values_list('lat', flat=True).order_by('-name').all()
    infra_lon = InfraAdmin.objects.filter(std_day__exact='2022-10-27').filter(cate__exact=cate).filter(gu__exact=gu_name).filter(dong__exact=dong_name).values_list('lon', flat=True).order_by('-name').all()

    name_list = []
    lat_list = []
    lon_list = []

    for i in range(len(infra_name)):
            name_list.append(infra_name[i])
            lat_list.append(float(infra_lat[i]))
            lon_list.append(float(infra_lon[i]))

    dictionary = {'category':cate, 'name':name_list, 'lat':lat_list, 'lon':lon_list}
    return JsonResponse(dictionary)

def similarDong(request):
    if not request.user.is_authenticated:
        return redirect('accounts/login')
    return render(request, 'recommend_app/similar_dong.html')


def similarRecommend(request):
    dong_code = request.POST['dong_code']
    if dong_code == "":
        return render(request, 'recommend_app/similar_dong.html',{'data' : False})
    df = RML.preprocessing_df()
    graph_data = RML.minmax_scaling(df)

    basic_df, first_kmeans, first_pca = RML.first_clustering(df)

    dong_data = df.loc[[int(dong_code)]]

    result_dong_sim, result_dong = RML.similarity(dong_data, df, int(dong_code), 4)
    result_dong_list = result_dong[1:]
    sim_list = result_dong_sim[1:]
    recommend_dong_list = basic_df.loc[result_dong_list]['DONG'].values
    recommend_gu_list = basic_df.loc[result_dong_list]['GU'].values
    recommend_code_list = basic_df.loc[result_dong_list].index.values
    graph_data_list = graph_data.loc[result_dong_list].values.tolist()
    # # result = {"dong": recommend_dong_list, "gu" : recommend_gu_list, "code" : recommend_code_list, "weight_user": user}
    result = zip(recommend_gu_list, recommend_dong_list, recommend_code_list, sim_list)
    title, tags = RML.get_dong_cluster(result_dong_list[0])
    return render(request, 'recommend_app/recommend_result.html', {'result': result,'cluster_data' : {'title' : title,"tags" : tags}, 'graph_data' : graph_data_list})