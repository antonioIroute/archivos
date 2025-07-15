#!/usr/bin/env python3
import sys
import tempfile
import zipfile
import io
import os
import json
import boto3 #type: ignore
import logging
import re
from pathlib import Path
from awsglue.utils import getResolvedOptions  # type: ignore
from pyspark.context import SparkContext # type: ignore
from awsglue.context import GlueContext # type: ignore
from awsglue.job import Job # type: ignore
from utils.utils import sanitize_for_log, validate_safe_path, get_allowed_environments, load_config_from_s3_zip

sys.path.insert(0, '/home/glue_user/workspace')

# Ahora intenta importar
try:
    from utils.data_processor import DataProcessor
    from utils.db_connection import MySQLConnector
except ImportError as e:
    raise ImportError(f"No se pudo importar DataProcessor o MySQLConnector: {e}")

def setup_logging():
    """
    Configura el logging básico
    
    Returns:
        Logger configurado
    """
    logger = logging.getLogger("DataConsolidado")
    logger.setLevel(logging.INFO)
    
    # Comprobar si ya hay handlers para evitar duplicados
    if not logger.handlers:
        # Crear handler para consola
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formato del log
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        # Agregar handler al logger
        logger.addHandler(console_handler)
    
    return logger

def load_config(logger, env, running_in_glue=True):
    """
    Carga la configuración de un archivo local o desde un zip en S3 dependiendo si estamos en Glue o Local.

    Args:
        logger: Logger configurado.
        env: Entorno (dev, iroute, bb, prod, test).
        running_in_glue: Si estamos ejecutando en Glue o localmente.

    Returns:
        Configuración como diccionario.
    """
    logger.info(f"Cargando configuración para entorno: {sanitize_for_log(env)}, running_in_glue: {running_in_glue}")

    if running_in_glue:
        # Cargar desde S3 usando función segura
        s3_zip_path = 's3://bb-consolidado/scripts/utils.zip'
        return load_config_from_s3_zip(s3_zip_path, env, logger)
    else:
        # Cargar desde archivo local
        validated_env = validate_safe_path(env, get_allowed_environments())
        config_path = f"config/{validated_env}.json"

        # Validar path local
        allowed_local_paths = [
            'config/dev.json',
            'config/iroute.json',
            'config/bb.json',
            'config/prod.json',
            'config/test.json'
        ]

        if config_path not in allowed_local_paths:
            raise ValueError(f"Path local no permitido: {sanitize_for_log(config_path)}")

        try:
            with open(config_path, 'r') as config_file:
                config = json.load(config_file)
                logger.info(f"Configuración {sanitize_for_log(config_path)} cargada correctamente desde archivo local.")
                return config
        except FileNotFoundError:
            logger.error(f"Archivo de configuración {sanitize_for_log(config_path)} no encontrado.")
            raise
        except Exception as e:
            logger.error(f"Error al cargar configuración: {sanitize_for_log(str(e))}")
            raise

def is_running_in_glue():
    """
    Determina si el código está ejecutándose en AWS Glue
    
    Returns:
        Boolean: True si está ejecutándose en Glue, False en otro caso
    """
    # Verificar si existen variables de entorno específicas de Glue
    return ('GLUE_PYTHON_VERSION' in os.environ or 
            'GLUE_VERSION' in os.environ or 
            '--JOB_NAME' in sys.argv)

def parse_environment_argument(running_in_glue, logger):
    """
    Parsea el argumento de entorno desde línea de comandos o Glue.
    
    Args:
        running_in_glue: Boolean indicando si se ejecuta en AWS Glue
        logger: Logger para registrar eventos
        
    Returns:
        str: Nombre del entorno
        
    Raises:
        ValueError: Si no se encuentra el argumento env
    """
    env = None
    
    if running_in_glue:
        try:
            arg_names = ['JOB_NAME', 'env']
            args = getResolvedOptions(sys.argv, arg_names)
            env = args.get('env')
            #logger.info(f"Argumentos recibidos en Glue: {sanitize_for_log(args)}")
        except Exception as e:
            logger.warning(f"Error al obtener el argumento 'env' de Glue: {sanitize_for_log(e)}")
            env = _extract_env_from_argv(logger)
    else:
        env = _extract_env_from_argv(logger)
    
    if not env:
        error_msg = "El parámetro 'env' es obligatorio y debe ser proporcionado mediante --env"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    #logger.info(f"Usando entorno:  {sanitize_for_log(env)}")
    return env


def _extract_env_from_argv(logger):
    """
    Extrae el argumento --env de sys.argv manualmente.
    
    Args:
        logger: Logger para registrar eventos
        
    Returns:
        str or None: Valor del entorno si se encuentra
    """
    if '--env' not in sys.argv:
        return None
        
    try:
        env_index = sys.argv.index('--env') + 1
        if env_index < len(sys.argv):
            env = sys.argv[env_index]
            return env
    except (ValueError, IndexError) as e:
        logger.warning(f"No se pudo obtener el argumento env manualmente: {e}")
    
    return None


def initialize_spark_context(running_in_glue, logger):
    """
    Inicializa el contexto de Spark y Glue.
    
    Args:
        running_in_glue: Boolean indicando si se ejecuta en AWS Glue
        logger: Logger para registrar eventos
        
    Returns:
        tuple: (spark_session, job_instance, updated_running_flag)
    """
    try:
        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = None
        
        if running_in_glue and '--JOB_NAME' in sys.argv:
            job = Job(glue_context)
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env'])
            job.init(args['JOB_NAME'], args)
            
        return spark, job, running_in_glue
        
    except Exception as e:
        logger.warning(f"No se pudo inicializar como trabajo de Glue: {str(e)}. Inicializando como SparkSession local")
        
        from pyspark.sql import SparkSession #type: ignore
        
        spark = SparkSession.builder \
            .appName("GlueLocalJob") \
            .config("spark.executor.memory", "16g") \
            .config("spark.driver.memory", "16g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.executor.instances", "4") \
            .config("spark.executor.cores", "4") \
            .config("spark.memory.fraction", "0.8") \
            .getOrCreate()
        
        sc = spark.sparkContext
        _ = GlueContext(sc)  # Creamos el contexto pero no lo retornamos
        
        return spark, None, False


def build_s3_configuration(config, running_in_glue, logger):
    """
    Construye la configuración de S3 basada en el modo de ejecución.
    
    Args:
        config: Diccionario de configuración
        running_in_glue: Boolean indicando si se ejecuta en AWS Glue
        logger: Logger para registrar eventos
        
    Returns:
        dict: Configuración de S3
        
    Raises:
        ValueError: Si falta configuración requerida para S3
    """
    application_config = config.get('application', {})
    use_local_files = application_config.get('use_local_files', False)

    if use_local_files and not running_in_glue:
        #Usar directorio temporal del usuario
        user_temp_dir = Path.home() /  'tmp' / 'myapp'
        user_temp_dir.mkdir(parents=True, exist_ok=True)

        #Crea directorio temporal con permisos
        safe_temp_dir = tempfile.mkdtemp(
            prefix='consolidado-',
            dir=str(user_temp_dir)
        )
    
        os.chmod(safe_temp_dir, 0o700)
        return _build_local_s3_config(safe_temp_dir, logger)
    else:
        return _build_remote_s3_config(config.get('aws', {}), logger)

def _build_local_s3_config(local_base_dir, logger):
    """
    Construye configuración S3 para modo local.
    
    Args:
        local_base_dir: Directorio base local
        logger: Logger para registrar eventos
        
    Returns:
        dict: Configuración S3 local
    """
    base_path = os.path.join(local_base_dir, "S3/bb-emisormdp-datastore")
    
    config = {
        's3_bucket': None,
        's3_input_prefix_fa_ca': os.path.join(base_path, "input-files/pagos_comercios/"),
        's3_input_prefix_op_ce': os.path.join(base_path, "input-files/operaciones_diarias/"),
        's3_out_put_prefix': os.path.join(base_path, "end-files/consolidado-medianet/"),
        's3_bucket_salida': "bb-adquirenciamdp-archivosfinales-dev2" 

    }
    
    logger.info("Modo LOCAL: Usando archivos locales")
    return config


def _build_remote_s3_config(aws_config, logger):
    """
    Construye configuración S3 para modo remoto.
    
    Args:
        aws_config: Configuración AWS
        logger: Logger para registrar eventos
        
    Returns:
        dict: Configuración S3 remota
        
    Raises:
        ValueError: Si falta el bucket de S3
    """
    s3_bucket = aws_config.get('s3_bucket')
    if not s3_bucket:
        error_msg = "No se encontró el bucket de S3 en la configuración"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Obtener prefijos y asegurar que terminen con '/'
    s3_input_prefix_fa_ca = _ensure_trailing_slash(aws_config.get('s3_input_prefix_fa_ca', ''))
    s3_input_prefix_op_ce = _ensure_trailing_slash(aws_config.get('s3_input_prefix_op_ce', ''))
    s3_output_prefix = _ensure_trailing_slash(aws_config.get('s3_output_prefix', ''))
    s3_bucket_salida = aws_config.get('s3_bucket_salida')
    if not s3_bucket_salida:
        logger.error("No se encontró 's3_bucket_salida' en la configuración AWS")
        raise ValueError("Falta 's3_bucket_salida' en la configuración AWS")

    
    logger.info("Modo S3: Leyendo y guardando archivos en Amazon S3")
    logger.info(f"Bucket: {s3_bucket}")
    logger.info(f"Prefijo de entrada FA/CA: {s3_input_prefix_fa_ca}")
    logger.info(f"Prefijo de entrada OP/CE: {s3_input_prefix_op_ce}")
    logger.info(f"Prefijo de salida: {s3_output_prefix}")
    
    return {
        's3_bucket': s3_bucket,
        's3_input_prefix_fa_ca': s3_input_prefix_fa_ca,
        's3_input_prefix_op_ce': s3_input_prefix_op_ce,
        's3_output_prefix': s3_output_prefix,
        's3_bucket_salida': s3_bucket_salida 
    }


def _ensure_trailing_slash(path):
    """
    Asegura que la ruta termine con '/'.
    
    Args:
        path: Ruta a verificar
        
    Returns:
        str: Ruta con '/' al final
    """
    return path + '/' if path and not path.endswith('/') else path


def setup_mysql_connector(config, env, running_in_glue, spark, logger):
    """
    Configura el conector MySQL si está habilitado.
    
    Args:
        config: Diccionario de configuración
        env: Nombre del entorno
        running_in_glue: Boolean indicando si se ejecuta en AWS Glue
        spark: Sesión de Spark
        logger: Logger para registrar eventos
        
    Returns:
        MySQLConnector or None: Instancia del conector MySQL
    """
    enriquecer_con_mysql = config.get('application', {}).get('use_mysql_enrichment', False)
    logger.info(f"Enriquecimiento con MySQL: {enriquecer_con_mysql}")
    
    if not enriquecer_con_mysql:
        return None
    
    if running_in_glue:
        s3_zip_path = 's3://bb-consolidado/scripts/utils.zip'  # -->Banco

        logger.info(f"Inicializando MySQLConnector con configuración en ZIP: {s3_zip_path}")
        
        return MySQLConnector(
            spark=spark,
            config_path=s3_zip_path,
            env=env
        )
    else:
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
            "config", 
            f"{env}.json"
        )
        return MySQLConnector(spark=spark, config_path=config_path)


def initialize_data_processor(spark, logger, s3_config, config):
    """
    Inicializa el procesador de datos.
    
    Args:
        spark: Sesión de Spark
        logger: Logger para registrar eventos
        s3_config: Configuración de S3
        config: Configuración completa
        
    Returns:
        DataProcessor: Instancia del procesador
    """
    keywords = config.get('application', {}).get('keywords', [])
    
    return DataProcessor(
        spark=spark,
        logger=logger,
        s3_config=s3_config,
        output_path=None,
        keywords=keywords,
        app_config=config.get('application', {})
    )


def main():
    """
    Función principal que coordina todo el procesamiento de datos.
    """
    logger = setup_logging()
    spark = None
    job = None
    
    try:
        # 1. Determinar entorno de ejecución
        running_in_glue = is_running_in_glue()
        logger.info(f"¿Ejecutando en AWS Glue?: {running_in_glue}")
        
        # 2. Parsear argumentos
        env = parse_environment_argument(running_in_glue, logger)
        
        # 3. Inicializar Spark
        spark, job, running_in_glue = initialize_spark_context(running_in_glue, logger)
        
        # 4. Cargar configuración
        logger.info(f"Cargando configuración con entorno: {env}, running_in_glue: {running_in_glue}")
        config = load_config(logger, env=env, running_in_glue=running_in_glue)
        logger.info(f"Configuración cargada: {json.dumps(config, indent=2)}")
        
        # 5. Construir configuración S3
        s3_config = build_s3_configuration(config, running_in_glue, logger)
        
        # 6. Inicializar procesador de datos
        processor = initialize_data_processor(spark, logger, s3_config, config)
        
        # 7. Configurar MySQL si es necesario
        mysql_connector = setup_mysql_connector(config, env, running_in_glue, spark, logger)
        if mysql_connector:
            processor.mysql = mysql_connector
        
        # 8. Ejecutar procesamiento
        processor.run()
        
        logger.info("Procesamiento completado exitosamente")
        
        # 9. Finalizar job de Glue si es necesario
        if job:
            job.commit()
            
    except Exception as e:
        logger.error(f"Error en el procesamiento: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        # Detener Spark
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()