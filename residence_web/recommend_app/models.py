# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models
from django.db import models
from accounts.models import myuser


class CateWeights(models.Model):
    idx = models.BigAutoField(primary_key=True)
    username = models.ForeignKey(myuser, models.DO_NOTHING, db_column='username', to_field='username')
    car_shr_num = models.BigIntegerField(blank=True, null=True)
    ani_hspt_num = models.BigIntegerField(blank=True, null=True)
    leisure_num = models.BigIntegerField(blank=True, null=True)
    gym_num = models.BigIntegerField(blank=True, null=True)
    golf_num = models.BigIntegerField(blank=True, null=True)
    starbucks_num = models.BigIntegerField(blank=True, null=True)
    mc_num = models.BigIntegerField(blank=True, null=True)
    vegan_cnt = models.BigIntegerField(blank=True, null=True)
    safe_dlvr_num = models.BigIntegerField(blank=True, null=True)
    kids_num = models.BigIntegerField(blank=True, null=True)
    noise_vibration_num = models.BigIntegerField(blank=True, null=True)
    park_num = models.BigIntegerField(blank=True, null=True)
    coliving_num = models.BigIntegerField(blank=True, null=True)
    mz_pop_cnt = models.BigIntegerField(blank=True, null=True)
    transportation = models.BigIntegerField(blank=True, null=True)
    education = models.FloatField(blank=True, null=True)
    parenting = models.FloatField(blank=True, null=True)
    safety = models.FloatField(blank=True, null=True)
    medical = models.BigIntegerField(blank=True, null=True)
    facilities = models.BigIntegerField(blank=True, null=True)
    class Meta:
        managed = False
        db_table = 'CATE_WEIGHTS'

class DongCnt(models.Model):
    dong_code = models.BigIntegerField()
    dong = models.CharField(max_length=30, blank=True, null=True)
    gu = models.CharField(max_length=30, blank=True, null=True)
    car_shr_num = models.FloatField(blank=True, null=True)
    ani_hspt_num = models.FloatField(blank=True, null=True)
    leisure_num = models.FloatField(blank=True, null=True)
    gym_num = models.FloatField(blank=True, null=True)
    golf_num = models.FloatField(blank=True, null=True)
    starbucks_num = models.FloatField(blank=True, null=True)
    mc_num = models.FloatField(blank=True, null=True)
    vegan_cnt = models.FloatField(blank=True, null=True)
    safe_dlvr_num = models.FloatField(blank=True, null=True)
    kids_num = models.FloatField(blank=True, null=True)
    noise_vibration_num = models.FloatField(blank=True, null=True)
    park_num = models.FloatField(blank=True, null=True)
    coliving_num = models.FloatField(blank=True, null=True)
    mz_pop_cnt = models.FloatField(blank=True, null=True)
    transportation = models.FloatField(blank=True, null=True)
    education = models.FloatField(blank=True, null=True)
    parenting = models.FloatField(blank=True, null=True)
    safety = models.FloatField(blank=True, null=True)
    medical = models.FloatField(blank=True, null=True)
    facilities = models.FloatField(blank=True, null=True)
    std_day = models.CharField(primary_key=True, max_length=30)
    class Meta:
        managed = False
        db_table = 'DONG_CNT'
        unique_together = (('std_day', 'dong_code'),)

class InfraAdmin(models.Model):
    cate = models.CharField(max_length=30, blank=True, null=True)
    name = models.CharField(max_length=100, blank=True, null=True)
    dong_code = models.BigIntegerField(blank=True, null=True)
    lon = models.DecimalField(max_digits=38, decimal_places=7, blank=True, null=True)
    lat = models.DecimalField(max_digits=38, decimal_places=8, blank=True, null=True)
    std_day = models.CharField(max_length=10, blank=True, null=True)
    gu = models.CharField(max_length=26, blank=True, null=True)
    dong = models.CharField(max_length=26, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'INFRA_ADMIN'

class DongCoord(models.Model):
    dong_code = models.BigIntegerField(primary_key=True)
    si_do = models.CharField(max_length=26, blank=True, null=True)
    gu = models.CharField(max_length=26, blank=True, null=True)
    dong = models.CharField(max_length=26, blank=True, null=True)
    lat = models.FloatField(blank=True, null=True)
    lon = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'DONG_COORD'
