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
    # schedule=timedelta(days=1),
    start_date=datetime(2022, 10, 28, 0, 30),
    catchup=False,
    tags=['residence_etl'],
) as dag:
    start = DummyOperator(task_id="start")

    with TaskGroup(group_id='Extract') as ExtractGroup:
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

    with TaskGroup(group_id="Transform") as TransformGroup:
        t1 = BashOperator(
            task_id='transform_Bike',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Bike',
            # retries=3,
        )

        t2 = BashOperator(
            task_id='transform_Academy',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Academy',
        )

        t3 = BashOperator(
            task_id='transform_Bus',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Bus',
        )

        t4 = BashOperator(
            task_id='transform_CarSharing',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform CarSharing',
        )

        t5 = BashOperator(
            task_id='transform_ChildMed',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform ChildMed',
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

        t10 = BashOperator(
            task_id='transform_Safe',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Safe',
        )

        t11 = BashOperator(
            task_id='transform_Pharm',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Pharm',
        )

        t12 = BashOperator(
            task_id='transform_Animal',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Animal',
        )

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

        t19 = BashOperator(
            task_id='transform_Cafe',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Cafe',
        )

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

        t26 = BashOperator(
            task_id='transform_Leisure',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Leisure',
        )

        t27 = BashOperator(
            task_id='transform_NoiseVibration',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform NoiseVibration',
        )

        t28 = BashOperator(
            task_id='transform_Subway',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Subway',
            # retries=3,
        )

        t29 = BashOperator(
            task_id='transform_Sports',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Sports',
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

        t33 = BashOperator(
            task_id='transform_Police',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Police',
        )

        t34 = BashOperator(
            task_id='transform_Hospital',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Hospital',
        )

        t35 = BashOperator(
            task_id='transform_Park',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Park',
        )

        t36 = BashOperator(
            task_id='transform_Vegan',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Vegan',
        )

        t37 = BashOperator(
            task_id='transform_Coliving',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Coliving',
        )

        t38 = BashOperator(
            task_id='transform_PopuMZ',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform PopuMZ',
        )

        t39 = BashOperator(
            task_id='transform_Retail',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py transform Retail',
        )

    with TaskGroup(group_id="DataMart") as DataMartGroup:
        d1 = BashOperator(
            task_id='datamart_PopuGu',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart PopuGu',
        )

        d2 = BashOperator(
            task_id='datamart_PopuDong',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart PopuDong',
            # retries=3,
        )

        d3 = BashOperator(
            task_id='datamart_Academy',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Academy',
        )

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

        d6 = BashOperator(
            task_id='datamart_FireSta',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart FireSta',
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

        d9 = BashOperator(
            task_id='datamart_NoiseVibration',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart NoiseVibration',
        )

        d10 = BashOperator(
            task_id='datamart_DepartmentStore',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart DepartmentStore',
        )

        d11 = BashOperator(
            task_id='datamart_AnimalHospital',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart AnimalHospital',
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

        d14 = BashOperator(
            task_id='datamart_Pharmacy',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Pharmacy',
        )

        d15 = BashOperator(
            task_id='datamart_Bike',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Bike',
        )

        d16 = BashOperator(
            task_id='datamart_CarSharing',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart CarSharing',
        )

        d17 = BashOperator(
            task_id='datamart_Bus',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Bus',
        )

        d18 = BashOperator(
            task_id='datamart_Leisure',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Leisure',
        )

        d19 = BashOperator(
            task_id='datamart_KidsCafe',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart KidsCafe',
        )

        d20 = BashOperator(
            task_id='datamart_Starbucks',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Starbucks',
        )

        d21 = BashOperator(
            task_id='datamart_ConStore',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart ConStore',
        )

        d22 = BashOperator(
            task_id='datamart_Mcdonalds',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Mcdonalds',
            # retries=3,
        )

        d23 = BashOperator(
            task_id='datamart_Cafe',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Cafe',
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

        d26 = BashOperator(
            task_id='datamart_Subway',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Subway',
        )

        d27 = BashOperator(
            task_id='datamart_Sport',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Sport',
        )

        d28 = BashOperator(
            task_id='datamart_MiddleSchool',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart MiddleSchool',
        )

        d29 = BashOperator(
            task_id='datamart_Police',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Police',
        )

        d30 = BashOperator(
            task_id='datamart_Hospital',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Hospital',
        )

        d32 = BashOperator(
            task_id='datamart_Park',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Park',
        )

        d33 = BashOperator(
            task_id='datamart_Vegan',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Vegan',
        )

        d34 = BashOperator(
            task_id='datamart_Coliving',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Coliving',
        )

        d35 = BashOperator(
            task_id='datamart_PopuMZ',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart PopuMZ',
        )

        d36 = BashOperator(
            task_id='datamart_Retail',
            cwd='/home/big/study/residence_etl',
            bash_command='python3 main.py datamart Retail',
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
    start >> ExtractGroup >> TransformGroup >> DataMartGroup >> d31 >> some_other_task