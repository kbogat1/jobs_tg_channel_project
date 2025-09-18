import duckdb
import pendulum
import pandas as pd
import requests

def get_data_jobify(industry='data-science', ACCESS_KEY=None, SECRET_KEY=None):
    today = pendulum.today().date().strftime('%Y-%m-%d')
    
    connect = duckdb.connect()
    
    connect.sql(
        f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;
            
            COPY (
                SELECT * FROM read_json_auto('https://jobicy.com/api/v2/remote-jobs?&industry={industry}')
            ) TO 's3://jobicy-bucket/jobicy-bucket_{industry}_{today}.json';
"""
    )
    connect.close()
    
    
def get_data_hh_prof_roles(ACCESS_KEY=None, SECRET_KEY=None):
    connect = duckdb.connect()
    
    connect.sql(
        f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;
            
            COPY (
                SELECT * FROM read_json_auto('https://api.hh.ru/professional_roles')
            ) TO 's3://head-hunter/professional_roles.json';
"""
    )
    connect.close()
    
def extract_hh_prof_roles(user_name=None, password=None):
    url = "https://api.hh.ru/professional_roles"
    data = requests.get(url).json()["categories"]
    it_prof_roles = tuple(filter(lambda x: x["name"] == "Информационные технологии", data))[0]["roles"]
    df = pd.DataFrame(it_prof_roles, 
                      columns=["id", "name", "accept_incomplete_resumes", "is_default", "select_deprecated", "search_deprecated"])
    connection = duckdb.connect()
    
    connection.sql(
        f"""CREATE SECRET dwh_postgres (
                TYPE postgres,
                HOST postgres_dwh,
                PORT 5432,
                DATABASE postgres,
                USER '{user_name}',
                PASSWORD '{password}'
);

ATTACH '' AS postgres_db_one (TYPE postgres, SECRET dwh_postgres);

INSERT INTO postgres_db_one.ods_hh_ru.professional_roles_tmp (
    id,
    name,
    accept_incomplete_resumes,
    is_default,
    select_deprecated,
    search_deprecated
)
select  id,
        name,
        cast(accept_incomplete_resumes as boolean),
        cast(is_default as boolean),
        cast(select_deprecated as boolean),
        cast(search_deprecated as boolean)
from df;

with sorce_cte as (
    select  pr.id,
            prt.id as prt_id,
            prt.name,
            prt.accept_incomplete_resumes,
            prt.is_default,
            prt.select_deprecated,
            prt.search_deprecated
    from postgres_db_one.ods_hh_ru.professional_roles pr
    full join postgres_db_one.ods_hh_ru.professional_roles_tmp prt
    on 1=1
    and pr.id = prt.id
)

insert into postgres_db_one.ods_hh_ru.professional_roles (
    id,
    name,
    accept_incomplete_resumes,
    is_default,
    select_deprecated,
    search_deprecated,
    is_deleted,
    change_date
)

select  prt_id,
        name,
        accept_incomplete_resumes,
        is_default,
        select_deprecated,
        search_deprecated,
        False,
        CURRENT_TIMESTAMP
from sorce_cte
where 1=1
and id is null;

update postgres_db_one.ods_hh_ru.professional_roles as pr set name = prt.name, 
                                                            accept_incomplete_resumes = prt.accept_incomplete_resumes, 
                                                            is_default = prt.is_default, 
                                                            select_deprecated = prt.select_deprecated, 
                                                            search_deprecated = prt.search_deprecated, 
                                                            is_deleted = False, 
                                                            change_date = CURRENT_TIMESTAMP
from postgres_db_one.ods_hh_ru.professional_roles_tmp as prt
where 1=1
and pr.id = prt.id;

update postgres_db_one.ods_hh_ru.professional_roles as pr set (is_deleted, change_date) = (True, CURRENT_TIMESTAMP) where id not in
(select id
from postgres_db_one.ods_hh_ru.professional_roles_tmp as prt);
"""
    )
    
    connection.close()
    

    


