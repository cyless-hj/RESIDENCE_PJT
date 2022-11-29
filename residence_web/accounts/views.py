from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login
from accounts.forms import UserForm
from kafka import KafkaConsumer
from django.contrib.auth.decorators import login_required

# Create your views here.
def signup(request):
    # 계정 생성 함수
    if request.method == 'POST':
        form = UserForm(request.POST)
        if form.is_valid():
            # 넘어온 데이터 db에 저장
            form.save()
            # user_name, password1을 추출 해서
            user_name = form.cleaned_data.get('username')
            raw_password = form.cleaned_data.get('password1')
            # 로그인 진행
            user = authenticate(username=user_name, password=raw_password)
            login(request, user)
            return redirect('/')
    else:
        form = UserForm()
    return render(request, 'accounts/signup.html', {'form':form})

@login_required(login_url='accounts:login')
def cal_static(request):    
    consumer = KafkaConsumer('project-session'
                , bootstrap_servers=['localhost:9092']
                , auto_offset_reset='earliest'
                , enable_auto_commit=False,
                consumer_timeout_ms=1000)

    for msg in consumer:
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                        message.offset, message.key,
                                        message.value))

    return redirect('/')