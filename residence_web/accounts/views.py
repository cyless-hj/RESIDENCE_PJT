from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login
from accounts.forms import UserForm

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
