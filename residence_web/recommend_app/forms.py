from django import forms
from recommend_app.models import CateWeights

class WeightsForm(forms.ModelForm):
    class Meta:
        model = CateWeights
        fields = ("username", 'transportation', 'safety', 'noise_vibration_num', 'leisure_num', 'gym_num', 'golf_num', 'park_num', 'facilities', 'medical', 'starbucks_num', 'mc_num', 'vegan_cnt', 'coliving_num', 'education', 'parenting', 'kids_num', 'ani_hspt_num', 'safe_dlvr_num', 'car_shr_num', 'mz_pop_cnt')