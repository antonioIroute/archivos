from botocore.exceptions import ClientError #type: ignore
from pyspark.sql.functions import col  #type: ignore
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DecimalType, DateType #type: ignore
from utils.logging_config import setup_logging
from utils.utils import sanitize_for_log, validate_safe_path, validate_config_path, validate_zip_path, get_allowed_environments, S3_PREFIX
import pymysql.cursors #type: ignore
import json
import boto3 #type: ignore
import zipfile
import io
import re

class MySQLConnector: 
    def __init__(self, spark, config_path=None, env=None):
        self.spark = spark
        self.logger = setup_logging()
        self.host = None
        self.port = None
        self.database = None
        self.region = None
        self.secret_arn = None
        self.user = None
        self.password = None
        self.env = env 

        # Validar env si se proporciona
        if env:
            self.env = validate_safe_path(env, get_allowed_environments())

        if not config_path: 
            if not self.env: 
                raise ValueError("Debe proporcionar un valor para 'env' si 'config_path' no está definido")
            # Usar env validado para construir path seguro
            config_path = f"config/{self.env}.json"
    
        self._load_config_json(config_path)

        if self.secret_arn:
            self._load_credentials()

    def _detect_download_zip(self, config_path):
        """Extrae el archivo ZIP de S3"""
        if not config_path.endswith('.zip'):
            raise ValueError("La ruta proporcionada no es un archivo ZIP")
        
        s3_path = config_path.replace(S3_PREFIX, '')
        bucket_name, key = s3_path.split('/', 1)
                                        
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        zip_content = response['Body'].read()
                    
        with zipfile.ZipFile(io.BytesIO(zip_content), 'r') as zip_ref:
            # Validar self.env antes de usar en paths
            if not self.env:
                raise ValueError("Environment no está definido")
            
            validated_env = validate_safe_path(self.env, get_allowed_environments())
            
            possible_paths = [
                f"config/{validated_env}.json",
                f"config\\{validated_env}.json",
                f"{validated_env}.json"
            ]   

            # Validar todos los paths en el ZIP antes de procesar
            zip_contents = zip_ref.namelist()        
            for zip_path in zip_contents:
                try:
                    validate_zip_path(zip_path)
                except ValueError: 
                    self.logger.warning("Path no seguro ignorado en ZIP")

            for path in possible_paths:     
                if path in zip_contents:
                    validated_path = validate_zip_path(path)
                    with zip_ref.open(validated_path) as f:
                        config_content = f.read().decode('utf-8')
                        config = json.loads(config_content)
                    return config
            
            raise FileNotFoundError(f"No se encontró el archivo de configuración para {sanitize_for_log(validated_env)} en el ZIP")

    def _load_config_json(self, config_path: str):
        """Carga la configuración desde un archivo JSON, ya sea local o en S3"""
        try:
            validated_config_path = validate_config_path(config_path)

            if validated_config_path.startswith(S3_PREFIX):
                if validated_config_path.endswith('.zip'):
                    config = self._detect_download_zip(validated_config_path)
                else:
                    s3_path = validated_config_path.replace(S3_PREFIX, '')
                    bucket_name, key = s3_path.split('/', 1)
                    
                    s3_client = boto3.client('s3')
                    response = s3_client.get_object(Bucket=bucket_name, Key=key)
                    config_content = response['Body'].read().decode('utf-8')
                    config = json.loads(config_content)
            else:
                with open(validated_config_path, 'r') as config_file:
                    config = json.load(config_file)

            if 'mysql' in config: 
                mysql_config = config['mysql']
                self.host = mysql_config.get('host')
                self.port = mysql_config.get('port', 3306)
                self.database = mysql_config.get('database')
                self.user = mysql_config.get('user') 
                self.password = mysql_config.get('password')
            
            if 'aws' in config:
                aws_config = config['aws']
                self.region = aws_config.get('region')
                self.secret_arn = aws_config.get('secret_arn')

        except Exception as e:
            self.logger.error(f"Error al cargar la configuración: {sanitize_for_log(str(e))}")
            raise

    def _load_credentials(self):
        """Carga las credenciales desde AWS Secrets Manager"""
        try:
            session = boto3.Session(region_name=self.region)
            self.credentials = self.get_secret(self.secret_arn, self.region, session)
            
            if self.credentials:
                if 'username' in self.credentials:
                    self.user = self.credentials['username']
                
                if 'password' in self.credentials:
                    self.password = self.credentials['password']
        except Exception as e:
            self.logger.error(f"Error al cargar credenciales: {sanitize_for_log(str(e))}")

    def get_secret(self, secret_arn, region_name, session):
        client = session.client(service_name="secretsmanager", region_name=region_name)
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_arn)
            secret_string = get_secret_value_response["SecretString"]
            return json.loads(secret_string)
        except ClientError as e:
            self.logger.error(f"No se pudo consultar el secret name: {sanitize_for_log(secret_arn)}\n{sanitize_for_log(str(e))}")
            return None
        
    def _get_connection(self):
        """Establece una conexión a la base de datos MySQL"""
        try:
            #Validar que todos los parámetros requeridos estén presentes
            if not self.host:
                raise ValueError("Host de base de datos no configurado")
            if not self.user:
                raise ValueError("Usuario de base de datos no configurado")
            if not self.password:
                raise ValueError("Password de base de datos no configurado")
            if not self.database:
                raise ValueError("Nombre de base de datos no configurado")

            # Asegurar que port sea int
            port = self.port if self.port is not None else 3306
            if not isinstance(port, int):
                port = int(port)
                
            connection = pymysql.connect(
                 host=str(self.host),
                port=port,
                user=str(self.user),
                password=str(self.password),
                database=str(self.database),
                cursorclass=pymysql.cursors.DictCursor,
                
            )
            return connection
        except Exception as e:
            self.logger.error(f"Error al establecer la conexión: {sanitize_for_log(str(e))}")
            return None

    def _define_schema(self):
        """Define el esquema fijo para la tabla adq_t_operac_diaria_op"""
        return StructType([
            StructField("op_codcom", LongType(), True),
            StructField("op_pan", StringType(), True),
            StructField("op_numaut", StringType(), True),
            StructField("op_fecfac", DateType(), True),
            StructField("op_numrefrem", LongType(), True),
            StructField("op_numreffacrem", IntegerType(), True),
            StructField("op_impfacfac", DecimalType(17, 2), True),
            StructField("op_codterm", StringType(), True),
            StructField("op_descodmar", StringType(), True),
            StructField("op_inderror", StringType(), True),
            StructField("op_codcadena", StringType(), True),
            StructField("op_descadena", StringType(), True),
            StructField("op_nomcomred", StringType(), True),
            StructField("op_secope", LongType(), True),
        ])

    def _clean_data_for_spark(self, result):
        """Limpia los datos antes de crear el DataFrame"""
        cleaned_result = []
        
        for row in result:
            cleaned_row = {}
            for key, value in row.items():
                if value is None:
                    if key in ['op_codcom', 'op_pan', 'op_numaut', 'op_codterm', 
                             'op_descodmar', 'op_inderror', 'op_codcadena', 
                             'op_descadena', 'op_nomcomred', 'op_secope',
                             'op_numrefrem', 'op_numreffacrem']:
                        cleaned_row[key] = ""
                    elif key in ['op_impfacfac']:
                        cleaned_row[key] = 0.0
                    else:
                        cleaned_row[key] = None
                else:
                    cleaned_row[key] = value
            
            cleaned_result.append(cleaned_row)
        
        return cleaned_result

    def _validate_table_name(self, table_name):
        """Valida el nombre de tabla para prevenir SQL injection"""
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Nombre de tabla inválido")
        
        # Solo permitir nombres de tabla seguros
        allowed_tables = [
            'adq_t_operac_diaria_op', 
            'adq_t_operac_diaria_ce',
            'adq_t_pag_comercio_fa',
            'adq_t_pag_comercio_ca'
        ]
        
        if table_name not in allowed_tables:
            raise ValueError(f"Tabla no permitida: {table_name}")
        
        return table_name

    def _validate_column_names(self, columns):
        """Valida nombres de columnas para prevenir SQL injection"""
        if columns == "*":
            return columns
        
        if isinstance(columns, str):
            column_list = [col.strip() for col in columns.split(',')]
        elif isinstance(columns, list):
            column_list = columns
        else:
            raise ValueError("Columnas deben ser string o lista")
        
        # Whitelist de columnas permitidas
        allowed_columns = [
            'op_codcom', 'op_pan', 'op_numaut', 'op_fecfac', 'op_numrefrem',
            'op_numreffacrem', 'op_impfacfac', 'op_codterm', 'op_descodmar',
            'op_inderror', 'op_codcadena', 'op_descadena', 'op_nomcomred',
            'op_secope', 'op_processdate'
        ]
        
        for col in column_list:
            if col not in allowed_columns:
                raise ValueError(f"Columna no permitida: {col}")
        
        return columns

    def execute_query_with_filters(self, table_name, filter_conditions=None, columns="*"):
        """Ejecuta una consulta SQL con filtros dinámicos"""
        try:
            # Validar inputs
            validated_table = self._validate_table_name(table_name)
            validated_columns = self._validate_column_names(columns)
            
            conn = self._get_connection()
            if not conn:
                return None
            
            query = f"SELECT {validated_columns} FROM {validated_table}"
            
            params = []
            if filter_conditions and len(filter_conditions) > 0:
                where_clauses = []
                for column, value in filter_conditions.items():
                    # Validar nombre de columna
                    self._validate_column_names([column])
                    where_clauses.append(f"{column} = %s")
                    params.append(value)
                
                query += " WHERE " + " AND ".join(where_clauses)
            
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(query, params)
            result = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            if result:
                cleaned_result = self._clean_data_for_spark(result)
                
                try:
                    schema = self._define_schema()
                    df = self.spark.createDataFrame(cleaned_result, schema)
                except Exception:
                    df = self.spark.createDataFrame(cleaned_result)
                
                if 'op_fecfac' in df.columns:
                    df = df.withColumn("op_fecfac_str", col("op_fecfac").cast("string"))

                return df
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"Error al ejecutar consulta con filtros: {sanitize_for_log(str(e))}")
            return None
            
    def execute_batch_query(self, table_name, filter_list, columns="*"):
        """Ejecuta múltiples consultas en lote y une los resultados"""
        try:
            results_df = None
            
            for i, filters in enumerate(filter_list):
                df = self.execute_query_with_filters(table_name, filters, columns)
                
                if df is not None:
                    if results_df is None:
                        results_df = df
                    else:
                        try:
                            results_df = results_df.unionAll(df)
                        except Exception:
                            results_df = results_df.unionByName(df, allowMissingColumns=True)
            
            return results_df
                
        except Exception as e:
            self.logger.error(f"Error al ejecutar consultas en lote: {sanitize_for_log(str(e))}")
            return None

    def _build_latest_date_query(self, table_name, filter_conditions=None, columns="*", date_column="op_processdate"):
        """Construye la consulta SQL para obtener registros con fecha más reciente"""
        # Validar inputs
        validated_table = self._validate_table_name(table_name)
        validated_columns = self._validate_column_names(columns)
        self._validate_column_names([date_column])
        
        unique_keys = ["op_codcom", "op_pan", "op_numaut", "op_fecfac", "op_numrefrem", "op_numreffacrem"]
        
        where_conditions = []
        params = []
        if filter_conditions and len(filter_conditions) > 0:
            for column, value in filter_conditions.items():
                # Validar nombre de columna
                self._validate_column_names([column])
                where_conditions.append(f"{column} = %s")
                params.append(value)
        
        where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
        WITH ranked_records AS (
            SELECT 
                {validated_columns},
                ROW_NUMBER() OVER (
                    PARTITION BY {', '.join(unique_keys)}
                    ORDER BY {date_column} DESC
                ) as row_num
            FROM {validated_table}
            {where_clause}
        )
        SELECT * FROM ranked_records WHERE row_num = 1
        """
        
        return query, params

    def execute_query_with_latest_date(self, table_name, filter_conditions=None, columns="*", date_column="op_processdate"):
        """Ejecuta una consulta SQL con filtros dinámicos y devuelve solo los registros con la fecha más reciente"""
        try:
            conn = self._get_connection()
            if not conn:
                return None
                
            query, params = self._build_latest_date_query(table_name, filter_conditions, columns, date_column)
            
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            result = cursor.fetchall()
            
            # Remover la columna row_num del resultado
            for row in result:
                if 'row_num' in row:
                    del row['row_num']
            
            cursor.close()
            conn.close()
            
            if result:
                cleaned_result = self._clean_data_for_spark(result)
                
                try:
                    schema = self._define_schema()
                    df = self.spark.createDataFrame(cleaned_result, schema)
                except Exception:
                    df = self.spark.createDataFrame(cleaned_result)
                
                if 'op_fecfac' in df.columns:
                    df = df.withColumn("op_fecfac_str", col("op_fecfac").cast("string"))

                return df
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"Error al ejecutar consulta con filtros para fecha más reciente: {sanitize_for_log(str(e))}")
            return None

    def execute_batch_query_latest_date(self, table_name, filter_list, columns="*", date_column="op_processdate"):
        """Ejecuta múltiples consultas en lote y une los resultados, devolviendo solo los registros más recientes"""
        try:
            results_df = None
            
            for i, filters in enumerate(filter_list):
                df = self.execute_query_with_latest_date(table_name, filters, columns, date_column)
                
                if df is not None:
                    if results_df is None:
                        results_df = df
                    else:
                        try:
                            results_df = results_df.unionAll(df)
                        except Exception:
                            results_df = results_df.unionByName(df, allowMissingColumns=True)
            
            return results_df
                
        except Exception as e:
            self.logger.error(f"Error al ejecutar consultas en lote con filtro para fecha más reciente: {sanitize_for_log(str(e))}")
            return None
