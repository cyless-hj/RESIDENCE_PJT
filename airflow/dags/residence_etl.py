from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

with DAG(
    'residence_etl',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['jhj6740@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
    },
    description='Movie ETL Project',
    schedule=timedelta(days=30),
    start_date=datetime(2022, 10, 28, 0, 30),
    catchup=False,
    tags=['residence_etl'],
) as dag:
    start = DummyOperator(task_id="start")

    with TaskGroup(group_id='Extract1') as ExtractGroup1:
        # t1, t2 and t3 are examples of tasks created by instantiating operators
        e1 = BashOperator(
            task_id='extract_Bike',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract Bike',
        )

        e2 = BashOperator(
            task_id='extract_Academy',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract Academy',
        )

        e3 = BashOperator(
            task_id='extract_Bus',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract Bus',
        )

    with TaskGroup(group_id='Extract2') as ExtractGroup2:
        e4 = BashOperator(
            task_id='extract_CarSharing',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract CarSharing',
        )

        e5 = BashOperator(
            task_id='extract_ChildMed',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract ChildMed',
        )

        e6 = BashOperator(
            task_id='extract_Golf',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract Golf',
        )

    with TaskGroup(group_id='Extract3') as ExtractGroup3:
        e7 = BashOperator(
            task_id='extract_Gym',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract Gym',
        )

        e8 = BashOperator(
            task_id='extract_KidsCafe',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract KidsCafe',
        )

        e9 = BashOperator(
            task_id='extract_Kindergarten',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract Kindergarten',
        )
    with TaskGroup(group_id='Extract4') as ExtractGroup4:
        e10 = BashOperator(
            task_id='extract_SafeDelivery',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract SafeDelivery',
        )

        e11 = BashOperator(
            task_id='extract_Pharmacy',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract Pharmacy',
        )

        e12 = BashOperator(
            task_id='extract_AnimalHospital',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract AnimalHospital',
        )

        e13 = BashOperator(
            task_id='extract_Vegan',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py extract Vegan',
        )

    with TaskGroup(group_id="Transform_Transportation") as TransformGroup1:
        t1 = BashOperator(
            task_id='transform_Bike',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Bike',
        )
        t3 = BashOperator(
            task_id='transform_Bus',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Bus',
        )
        t28 = BashOperator(
            task_id='transform_Subway',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Subway',
            # retries=3,
        )

    with TaskGroup(group_id="Transform_Safety") as TransformGroup2:
        t10 = BashOperator(
            task_id='transform_Safe',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Safe',
        )
        t24 = BashOperator(
            task_id='transform_Cctv',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Cctv',
        )
        t25 = BashOperator(
            task_id='transform_Fire',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Fire',
        )
        t33 = BashOperator(
            task_id='transform_Police',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Police',
        )

    with TaskGroup(group_id="Transform_Convenient_Fac") as TransformGroup3:
        t19 = BashOperator(
            task_id='transform_Cafe',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Cafe',
        )
        t22 = BashOperator(
            task_id='transform_Constore',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Constore',
        )
        t23 = BashOperator(
            task_id='transform_Departmentstore',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Departmentstore',
        )
        t39 = BashOperator(
            task_id='transform_Retail',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Retail',
        )

    with TaskGroup(group_id="Transform_Medical") as TransformGroup4:
        t11 = BashOperator(
            task_id='transform_Pharm',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Pharm',
        )
        t34 = BashOperator(
            task_id='transform_Hospital',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Hospital',
        )

    with TaskGroup(group_id="Transform_Others") as TransformGroup5:
        t27 = BashOperator(
            task_id='transform_NoiseVibration',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform NoiseVibration',
        )
        t35 = BashOperator(
            task_id='transform_Park',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Park',
        )
        t37 = BashOperator(
            task_id='transform_Coliving',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Coliving',
        )

    with TaskGroup(group_id="Transform_LifeStyle") as TransformGroup6:
        t4 = BashOperator(
            task_id='transform_CarSharing',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform CarSharing',
        )
        t6 = BashOperator(
            task_id='transform_Golf',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Golf',
        )
        t7 = BashOperator(
            task_id='transform_Gym',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Gym',
        )
        t26 = BashOperator(
            task_id='transform_Leisure',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Leisure',
        )
        t29 = BashOperator(
            task_id='transform_Sports',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Sports',
        )
        t12 = BashOperator(
            task_id='transform_Animal',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Animal',
        )

    with TaskGroup(group_id="Transform_Education") as TransformGroup7:
        t2 = BashOperator(
            task_id='transform_Academy',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Academy',
        )
        t30 = BashOperator(
            task_id='transform_ElementaryShc',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform ElementaryShc',
        )

        t31 = BashOperator(
            task_id='transform_MiddleShc',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform MiddleShc',
        )

        t32 = BashOperator(
            task_id='transform_HighShc',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform HighShc',
        )

    with TaskGroup(group_id="Transform_Parenting") as TransformGroup8:
        t8 = BashOperator(
            task_id='transform_KidsCafe',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform KidsCafe',
        )

        t9 = BashOperator(
            task_id='transform_Kinder',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Kinder',
        )
        t5 = BashOperator(
            task_id='transform_ChildMed',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform ChildMed',
        )

    with TaskGroup(group_id="Transform_Eating") as TransformGroup9:
        t20 = BashOperator(
            task_id='transform_Starbucks',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Starbucks',
        )
        t21 = BashOperator(
            task_id='transform_Mcdonalds',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Mcdonalds',
        )
        t36 = BashOperator(
            task_id='transform_Vegan',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Vegan',
        )
        
    with TaskGroup(group_id="Transform_Popu") as TransformGroup10:
        t17 = BashOperator(
            task_id='transform_PopuDong',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform PopuDong',
        )
        t18 = BashOperator(
            task_id='transform_PopuGu',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform PopuGu',
        )
        t38 = BashOperator(
            task_id='transform_PopuMZ',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform PopuMZ',
        )

        

    with TaskGroup(group_id="DataMart_Transpotation") as DataMartGroup1:
        d15 = BashOperator(
            task_id='datamart_Bike',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Bike',
        )
        d17 = BashOperator(
            task_id='datamart_Bus',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Bus',
        )
        d26 = BashOperator(
            task_id='datamart_Subway',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Subway',
        )

    with TaskGroup(group_id="DataMart_Safety") as DataMartGroup2:
        d6 = BashOperator(
            task_id='datamart_FireSta',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart FireSta',
        )
        d12 = BashOperator(
            task_id='datamart_SafeDelivery',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart SafeDelivery',
            # retries=3,
        )
        d13 = BashOperator(
            task_id='datamart_Cctv',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Cctv',
        )
        d29 = BashOperator(
            task_id='datamart_Police',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Police',
        )

    with TaskGroup(group_id="DataMart_Convenient_Fac") as DataMartGroup3:
        d23 = BashOperator(
            task_id='datamart_Cafe',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Cafe',
        )
        d36 = BashOperator(
            task_id='datamart_Retail',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Retail',
        )
        d10 = BashOperator(
            task_id='datamart_DepartmentStore',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart DepartmentStore',
        )
        d21 = BashOperator(
            task_id='datamart_ConStore',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart ConStore',
        )

    with TaskGroup(group_id="DataMart_Medical") as DataMartGroup4:
        d14 = BashOperator(
            task_id='datamart_Pharmacy',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Pharmacy',
        )
        d30 = BashOperator(
            task_id='datamart_Hospital',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Hospital',
        )

    with TaskGroup(group_id="DataMart_Others") as DataMartGroup5:
        d9 = BashOperator(
            task_id='datamart_NoiseVibration',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart NoiseVibration',
        )
        d32 = BashOperator(
            task_id='datamart_Park',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Park',
        )
        d34 = BashOperator(
            task_id='datamart_Coliving',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Coliving',
        )

    with TaskGroup(group_id="DataMart_LifeStyle") as DataMartGroup6:
        d11 = BashOperator(
            task_id='datamart_AnimalHospital',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart AnimalHospital',
        )
        d16 = BashOperator(
            task_id='datamart_CarSharing',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart CarSharing',
        )
        d18 = BashOperator(
            task_id='datamart_Leisure',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Leisure',
        )
        d24 = BashOperator(
            task_id='datamart_Gym',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Gym',
        )
        d25 = BashOperator(
            task_id='datamart_Golf',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Golf',
        )
        d27 = BashOperator(
            task_id='datamart_Sport',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Sport',
        )

    with TaskGroup(group_id="DataMart_Edu") as DataMartGroup7:
        d3 = BashOperator(
            task_id='datamart_Academy',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Academy',
        )
        d7 = BashOperator(
            task_id='datamart_Elementary',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Elementary',
        )
        d8 = BashOperator(
            task_id='datamart_HighSchool',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart HighSchool',
        )
        d28 = BashOperator(
            task_id='datamart_MiddleSchool',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart MiddleSchool',
        )

    with TaskGroup(group_id="DataMart_Parenting") as DataMartGroup8:
        d4 = BashOperator(
            task_id='datamart_Kindergarten',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Kindergarten',
        )
        d5 = BashOperator(
            task_id='datamart_ChildMed',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart ChildMed',
        )
        d19 = BashOperator(
            task_id='datamart_KidsCafe',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart KidsCafe',
        )

    with TaskGroup(group_id="DataMart_Eating") as DataMartGroup9:
        d20 = BashOperator(
            task_id='datamart_Starbucks',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Starbucks',
        )
        d22 = BashOperator(
            task_id='datamart_Mcdonalds',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Mcdonalds',
            # retries=3,
        )
        d33 = BashOperator(
            task_id='datamart_Vegan',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Vegan',
        )

    with TaskGroup(group_id="DataMart_Popu") as DataMartGroup10:
        d1 = BashOperator(
            task_id='datamart_PopuGu',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart PopuGu',
        )
        d2 = BashOperator(
            task_id='datamart_PopuDong',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart PopuDong',
        )
        d35 = BashOperator(
            task_id='datamart_PopuMZ',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart PopuMZ',
        )

    with TaskGroup(group_id="Operational_DB") as OperationalGroup1:
        o1 = DummyOperator(
            task_id='Infra'
        )
        o2 = DummyOperator(
            task_id='DongCNT'
        )

    some_other_task = DummyOperator(
        task_id='End'
    )

    d31 = BashOperator(
            task_id='datamart_FinNum',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart FinNum',
        )


    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    #e1 >> e2 >> e3 >> e4 >> e5 >> e6 >> e7 >> e8 >> e9 >> e10 >> e11 >> e12 >> t1 >> t3 >> t5 >> t7 >> t9 >> t11 >> t12 >> t18 >> t20 >> t22 >> t24 >> t26 >> t28 >> t30 >> t32 >> t34 >> t35 >> t2 >> t4 >> t6 >> t8 >> t10 >> t17 >> t19 >> t21 >> t23 >> t25 >> t27 >> t29 >> t31 >> t33 >> d1 >> d2 >> d3 >> d4 >> d5 >> d6 >> d7 >> d8 >> d9 >> d10 >> d11 >> d12 >> d13 >> d14 >> d15 >> d16 >> d17 >> d18 >> d19 >> d20 >> d21 >> d22 >>d23 >> d24 >>d25 >>d26 >> d27 >>d28 >>d29 >>d30 >> d32 >> d31
    
    start >> ExtractGroup1 >> ExtractGroup2 >> ExtractGroup3 >> ExtractGroup4 >> TransformGroup1 >> TransformGroup2 >> TransformGroup3 >> TransformGroup4 >> TransformGroup5 >> TransformGroup6 >> TransformGroup7 >> TransformGroup8 >> TransformGroup9 >> TransformGroup10 >> DataMartGroup1 >> DataMartGroup2 >> DataMartGroup3 >> DataMartGroup4 >> DataMartGroup5 >> DataMartGroup6 >> DataMartGroup7 >> DataMartGroup8 >> DataMartGroup9 >> DataMartGroup10 >> d31 >> OperationalGroup1 >> some_other_task