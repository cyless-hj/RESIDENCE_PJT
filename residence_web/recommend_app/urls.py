from django.urls import path
from . import views

app_name = 'recommend_app'

urlpatterns = [
    path('', views.index, name="index"),
    path('basicSelect', views.basicSelect, name='basicSelect'),
    # path('protoSubmit',views.protoSubmit),
    path('categoryRanking',views.categoryRanking, name='categoryRanking'),
    path('introduction',views.introduction, name='introduction'),
    path('dongDetail',views.dongDetail, name='dongDetail'),
    path('facility_info',views.facility_info, name='facility_info'),
    path('similarDong',views.similarDong, name='similarDong'),
    path('similarRecommend',views.similarRecommend, name='similarRecommend'),

]