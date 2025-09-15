import duckdb
import pendulum

def get_data(industry='data-science', ACCESS_KEY=None, SECRET_KEY=None):
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
    
    


