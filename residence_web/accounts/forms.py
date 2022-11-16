from django import forms
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth.models import User
from accounts.models import myuser

class UserForm(UserCreationForm):
    age = forms.CharField(label='연령대')
    gender = forms.CharField(label='성별')
    class Meta:
        model = myuser
        fields = ("username", "age", 'gender')