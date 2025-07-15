

from pyspark.sql.functions import col, row_number, desc, regexp_replace #type: ignore
from pyspark.sql.window import Window #type: ignore
from pyspark.sql import functions as F #type: ignore
from pyspark import StorageLevel #type: ignore
from utils.db_connection import MySQLConnector
from utils.enriquecer_db import enriquecer_con_bd
from functools import reduce
from datetime import datetime, timedelta
from pyspark.sql import DataFrame #type: ignore
from typing import Optional
from utils.utils import sanitize_for_log, validate_safe_path
import boto3 #type: ignore
import unicodedata
import re
import os
import shutil
import glob

S3_PREFIX = "s3://"

class DataProcessor:
    def __init__(self, spark, s3_config ,logger, output_path, keywords=None, app_config=None,):
        """
        Args:
            spark: Sesión de Spark
            s3_config: Configuración de S3 (bucket, prefijos)
            logger: Logger configurado
            output_path: Ruta donde se guardarán los resultados
            keywords: Lista de palabras clave para filtrar
            app_config: Configuración adicional 
        """
        self.spark = spark
        self.logger = logger
        self.output_path = output_path
        self.process_date = None
        self.mysql: Optional[MySQLConnector] = None
        self.app_config = app_config or {}

        # Configuración de archivos locales vs S3
        self.use_local_files = self.app_config.get('use_local_files', False)
        self.local_base_dir = self.app_config.get('local_base_dir', '')
        self.min_file_size = self.app_config.get('MIN_FILE_SIZE', 1024)
        self.default_date = self.app_config.get('date_default', '2025-05-14')

        # Configuración S3
        self.s3_bucket = s3_config['s3_bucket']
        self.s3_input_prefix_fa_ca = s3_config['s3_input_prefix_fa_ca']
        self.s3_input_prefix_op_ce = s3_config['s3_input_prefix_op_ce']
        self.s3_output_prefix = s3_config.get('s3_output_prefix', 'end-files/consolidado-medianet/')
        self.s3_bucket_salida = s3_config['s3_bucket_salida']

        # Determinar prefijo base una sola vez
        self.base_prefix = self._get_base_prefix()
        
        # Configurar rutas de datos
        self.data_paths = self._configure_data_paths()
        
        # Configurar ruta de salida
        self.output_full_path = self._configure_output_path()

        self.KEYWORDS = keywords if keywords else []
        self.DATE_FORMAT = "%Y-%m-%d"

        # Definir configuración de columnas una sola vez
        self._configure_columns()

    def _get_base_prefix(self):
        """Determina el prefijo base según el entorno (local o S3)"""
        if self.use_local_files:
            return ""
        return f"{S3_PREFIX}{self.s3_bucket}/"

    def _configure_data_paths(self):
        """Configura todas las rutas de datos de entrada"""
        return {
            'op': f"{self.base_prefix}{self.s3_input_prefix_op_ce}adq_tc_t_operac_diaria_op",
            'fa': f"{self.base_prefix}{self.s3_input_prefix_fa_ca}adq_tc_t_pag_comercio_fa",
            'ca': f"{self.base_prefix}{self.s3_input_prefix_fa_ca}adq_tc_t_pag_comercio_ca",
            'ce': f"{self.base_prefix}{self.s3_input_prefix_op_ce}adq_tc_t_operac_diaria_ce"
        }

    def _configure_output_path(self):
        """Configura la ruta de salida"""
        if self.output_path:
            return self.output_path
        return f"{self.base_prefix}{self.s3_output_prefix}"

    def _configure_columns(self):
        """Configura todas las definiciones de columnas requeridas"""
        self.required_columns = {
            'op': ["od_nomcomred", "od_codcom1", "od_secope", "od_impfacfac", "od_codterm", 
                   "od_descodmar", "od_inderror"],
            'fa': ["pg_codcom", "pg_secope", "pg_pan", "pg_numaut", "pg_totcuotas", "pg_numcuotact", 
                   "pg_codhash2", "pg_fecfac", "pg_valorcom", "pg_valorretf", "pg_valoriva", 
                   "pg_valorretiva", "pg_valivacom", "pg_valorcons", "pg_indnorcor", "pg_tipofac", 
                   "pg_destipfac", "pg_voucher", "pg_inddebcre", "pg_codmar", "pg_tipfran", 
                   "pg_desfra", "pg_codcadena", "pg_codholding", "pg_numrefrem", "pg_numreffacrem", "pg_recap"],
            'ca': ["pg_descadena"],
            'ce': ["od_codcom1", "od_numrefrem", "od_numreffacrem", "od_codconeco", "od_prapleco"]
        }

        # Configuración para análisis de completitud
        self.campos_de_op = ["IMPFACFAC",  "DESCODMAR"]

        self.essential_columns = [
            "CODCOM", "PAN", "NUMAUT", "TOTCUOTAS", "NUMCUOTACT", "FECFAC",
            "VALORCOM", "VALORRETF", "VALORIVA", "VALORRETIVA", "VALIVACOM", 
            "VALORCONS", "INDNORCOR", "TIPOFAC", "DESTIPFAC", "VOUCHER",
            "INDDEBCRE", "CODMAR", "TIPFRAN", "DESTIPFRAN", 
            "CODCADENA", "DESCADENA", "PRAPLECO", "IMPFACFAC", "DESCODMAR", "RECAP"
        ]

        # Campos opcionales que no afectan la completitud
        self.optional_fields = ["CODHASH2", "CODHOLDING", "INDERROR", "CODTERM"]

        # Columnas finales para el resultado
        self.final_columns = [
            "CODCOM", "PAN", "NUMAUT", "TOTCUOTAS", "NUMCUOTACT", "CODHASH2", "FECFAC", 
            "IMPFACFAC", "VALORCOM", "VALORRETF", "VALORIVA", "VALORRETIVA", "VALIVACOM", 
            "PRAPLECO", "VALORCONS", "INDNORCOR", "TIPOFAC", "DESTIPFAC", "CODTERM", 
            "VOUCHER", "INDDEBCRE", "CODMAR", "DESCODMAR", "TIPFRAN", "DESTIPFRAN", 
            "INDERROR", "CODCADENA", "DESCADENA", "CODHOLDING", "RECAP"
        ]

    def _extract_s3_bucket_and_prefix(self, s3_path):
        """Extrae el nombre del bucket y el prefijo desde una ruta S3"""
        if not s3_path.startswith(S3_PREFIX):
            error_msg = f"Error: La ruta {s3_path} no parece ser una ruta de S3 válida"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        s3_path_without_prefix = s3_path.replace(S3_PREFIX, "")
        parts = s3_path_without_prefix.split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        
        # Asegurar que termina con '/'
        if prefix and not prefix.endswith('/'):
            prefix += '/'
            
        return bucket_name, prefix

    def _validate_file_size(self, file_path):
        """Valida el tamaño del archivo"""
        try: 
            if self.use_local_files:
                if not os.path.exists(file_path):
                    self.logger.error(f"El archivo local {sanitize_for_log(file_path)} no existe")
                    return False
                else:
                    bucket_name, key = self._extract_s3_bucket_and_prefix(file_path)
                    if key.endswith('/'):
                        key = key[:-1]

                    s3_client = boto3.client('s3')
                    metadata = s3_client.head_object(Bucket=bucket_name, Key=key)
                    file_size = metadata['ContentLength']

                    if file_size < self.min_file_size:
                        raise ValueError(f"El archivo {file_path} es demasiado pequeño: {file_size} bytes. Mínimo requerido: {self.min_file_size} bytes")
                    
                    return True
        except Exception as e:
            self.logger.error(f"Error al validar el tamaño del archivo {sanitize_for_log(file_path)}: {sanitize_for_log(str(e))}")
            return False

    def _get_date_pattern_and_prefix(self, file_type):
        """Obtiene el patrón de fecha y el prefijo según el tipo de archivo"""
        if file_type in ['op', 'ce']:
            return r".*?_processdate=(\d{4}-\d{2}-\d{2})", "od_processdate"
        elif file_type in ['fa', 'ca']:
            return r".*?_processdate=(\d{4}-\d{2}-\d{2})", "pg_processdate"
        else:
            raise ValueError(f"Tipo de archivo no reconocido: {file_type}")

    def find_date_subdirectories(self, base_directory, file_type):
        """Encuentra subdirectorios con formato de fecha en LOCAL o en S3"""
        date_pattern, _ = self._get_date_pattern_and_prefix(file_type)
        pattern = re.compile(date_pattern)

        if self.use_local_files:
            return self._find_local_dates(base_directory, pattern)
        else:
            return self._find_s3_dates(base_directory, pattern)

    def _find_local_dates(self, base_directory, pattern):
        """Busca fechas en directorios locales"""
        if not os.path.exists(base_directory):
            self.logger.error(f"El directorio local {sanitize_for_log(base_directory)} no existe")
            return []

        date_folders = []
        for folder_name in os.listdir(base_directory):
            match = pattern.match(folder_name)
            if match:
                date_folders.append(match.group(1))
        
        self.logger.info(f"Fechas encontradas localmente {base_directory}: {date_folders}")
        
        return date_folders

    def _find_s3_dates(self, base_directory, pattern):
        """Busca fechas en directorios S3"""
        if not base_directory.startswith("s3://"):
            self.logger.error(f"La ruta {sanitize_for_log(base_directory)} no es una ruta válida de S3")
            return []
        
        bucket_name, base_prefix = self._extract_s3_bucket_and_prefix(base_directory)
        self.logger.info(f"Bucket: {bucket_name}, Base prefix: {base_prefix}")

        try:
            s3_client = boto3.client('s3')
            date_folders = []
            paginator = s3_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=bucket_name, Prefix=base_prefix, Delimiter='/'):
                 if 'CommonPrefixes' in page:
                    for prefix_obj in page['CommonPrefixes']:
                        prefix_path = prefix_obj.get('Prefix', '')
                        match = pattern.search(prefix_path)
                        if match:
                            date_folders.append(match.group(1))

            self.logger.info(f"Fechas encontradas en S3: {date_folders}")
            return date_folders
        
        except Exception as e:
            self.logger.error(f"Error al buscar subdirectorios en S3:  {sanitize_for_log(str(e))}")
            return []

    def validate_date_folders(self):
        """Valida fechas disponibles y selecciona la fecha de procesamiento"""
        folder_dates = {}
        current_date = datetime.now().strftime(self.DATE_FORMAT) # Obtener la fecha actual

        # Buscar carpetas de fecha para cada tipo de archivo en S3
        for file_type, file_dir in self.data_paths.items():
            dates = self.find_date_subdirectories(file_dir, file_type)
            folder_dates[file_type] = set(dates) # Guardar todas las fechas, no solo las válidas
            #self.logger.info(f"Fechas encontradas para {file_type}: {dates}")

        # Determinar fecha de procesamiento
        process_date = self._determine_process_date(folder_dates, current_date)

        if not process_date:
            self.logger.error("No se encontraron carpetas válidas para el procesamiento.")
            return None
        
        self.logger.info(f"Fecha seleccionada para procesamiento: {process_date}")
        return process_date
    
    def _determine_process_date(self, folder_dates, current_date):
        """Determina la fecha de procesamiento: por defecto, la del día anterior a la fecha actual."""
        # Calcular la fecha de ayer
        yesterday = (datetime.strptime(current_date, self.DATE_FORMAT) - timedelta(days=1)).strftime(self.DATE_FORMAT)
        self.logger.info(f"Intentando usar la fecha de ayer: {yesterday}")

        # 1. Usar la fecha de ayer si está en todas las carpetas
        if all(yesterday in dates for dates in folder_dates.values() if dates):
            self.logger.info(f"Usando la fecha de ayer: {yesterday}")
            return yesterday

        # 2. Si no, buscar la fecha más reciente anterior a hoy común en todas las carpetas
        # Obtener la intersección de fechas
        common_dates = None
        for dates in folder_dates.values():
            if dates:
                if common_dates is None:
                    common_dates = set(dates)
                else:
                    common_dates = common_dates.intersection(dates)

        if common_dates:
            # Filtrar solo fechas menores a la actual
            prev_dates = [d for d in common_dates if d < current_date]
            if prev_dates:
                selected_date = sorted(prev_dates)[-1]  # La más reciente anterior a hoy
                self.logger.info(f"Usando la fecha común más reciente anterior a hoy: {selected_date}")
                return selected_date

        self.logger.error("No se encontró una fecha válida anterior a la actual en todas las carpetas.")
        return None

    def _get_s3_files(self, directory, file_type):
        """Obtiene la lista de archivos desde un directorio S3"""
        bucket_name, dir_prefix = self._extract_s3_bucket_and_prefix(directory)
        
        self.logger.info(f"Accediendo al bucket S3: {bucket_name} con prefijo: {dir_prefix}")
        s3_client = boto3.client('s3')

        try:
            files = []
            paginator = s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket_name, Prefix=dir_prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.csv'):
                            files.append(f"{S3_PREFIX}{bucket_name}/{obj['Key']}")
            
            self.logger.info(f"Encontrados {len(files)} archivos CSV para {file_type}")
            return files
            
        except Exception as e:
            self.logger.error(f"Error al listar archivos en S3: {sanitize_for_log(str(e))}")
            raise
  
    def list_files(self, directory, file_type):
        """Lista los archivos en un directorio (local o S3)"""
        if self.use_local_files:
            if not os.path.exists(directory):
                self.logger.error(f"El directorio local {sanitize_for_log(directory)} no existe")
                return []
            return [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".csv")]
        else:
            return self._get_s3_files(directory, file_type)

    def _get_file_type_from_path(self, directory):
        """Determina el tipo de archivo basado en la ruta"""
        for file_type, dir_path in self.data_paths.items():
            if directory.lower().startswith(dir_path.lower()):
                return file_type
        return None

    def _build_directory_with_date(self, directory, file_type):
        """Construye la ruta del directorio incluyendo la fecha de procesamiento"""
        if not file_type or not self.process_date:
            return directory
        
        _, date_prefix = self._get_date_pattern_and_prefix(file_type)
        date_folder = f"{date_prefix}={self.process_date}"
        
        if self.use_local_files:
            return os.path.join(directory, date_folder)
        else:
            return f"{directory}/{date_folder}"

    def read_csv_files(self, directory):
        """Lee archivos CSV con lógica unificada y validación de tamaño"""
        file_type = self._get_file_type_from_path(directory)

        if not file_type:
            raise ValueError(f"No se pudo determinar el tipo de archivo desde la ruta: {directory}")
        
        # Construir ruta con fecha
        modified_directory = self._build_directory_with_date(directory, file_type)

        # Intenetar obtener archivos
        files = self._get_files_with_fallback(modified_directory, directory ,file_type)
        
        if not files:
            raise ValueError(f"No se encontraron archivos CSV en  {directory}")

        return self._load_and_union_csv_files(files)
    
    ## Verificar que pasa si quitamos este metodo
    def _get_files_with_fallback(self, primary_directory, fallback_directory, file_type):
        """Obtiene archivos con mecanismo de fallback"""
        try:
            files = self.list_files(primary_directory, file_type)
            if files:
                return files
            self.logger.warning(f"No se encontraron archivos en {primary_directory}, probando ruta original")
        except Exception as e:
            self.logger.warning(f"Error al acceder a {primary_directory}: {str(e)}")
        
        return self.list_files(fallback_directory, file_type)

    def _load_and_union_csv_files(self, files):
        """Carga y une múltiples archivos CSV con validación de tamaño"""
        self.logger.info(f"Cargando {len(files)} archivos CSV...")
        dataframes = []

        for file in files:
            # Validar tamaño del archivo
            self._validate_file_size(file)
            
            # Cargar con detección automática de delimitador
            df = self._load_csv_with_delimiter_detection(file)
            dataframes.append(df)

        if dataframes:
            return reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dataframes)
        return None

    def _load_csv_with_delimiter_detection(self, file):
        """Carga CSV con detección automática del delimitador"""
        self.logger.info(f"Cargando archivo: {file}")
        
        # Intentar primero con punto y coma
        df = self.spark.read.option("header", True).option("sep", ";").csv(file)
        if len(df.columns) <= 2:
            self.logger.warning("Pocas columnas con ';', reintentando con ','")
            df = self.spark.read.option("header", True).option("sep", ",").csv(file)
        return df

    def clean_colname(self, col):
        """Normaliza un nombre de columna"""
        return unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('utf-8').strip().lower()

    def validate_columns(self, dataframe, file_type):
        """Valida columnas requeridas usando la configuración centralizada"""
        required_columns = self.required_columns.get(file_type, [])
        if not required_columns:
            self.logger.warning(f"No hay columnas requeridas definidas para el tipo: {file_type}")
            return

        df_columns_clean = [self.clean_colname(col) for col in dataframe.columns]
        required_columns_clean = [self.clean_colname(col) for col in required_columns]

        missing_columns = [col for col in required_columns_clean if col not in df_columns_clean]

        if missing_columns:
            error_msg = f"Faltan columnas en {sanitize_for_log(file_type.upper())}: {sanitize_for_log(', '.join(missing_columns))}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

    def load_and_validate_data(self):
        """Carga y valida todos los DataFrames necesarios"""
        try:
            # Validar fechas una sola vez
            self.process_date = self.validate_date_folders()
            if not self.process_date:
                    raise ValueError("No se encontraron carpetas con fechas válidas")
            
            # Cargar y procesar cada tipo de archivo
            dataframes = {}
            for file_type, file_path in self.data_paths.items():
                self.logger.info(f"Cargando datos {file_type.upper()}...")

                df = self.read_csv_files(file_path)
                # Persistir los datos en memoria y disco
                if df is None:
                    raise ValueError(f"No se pudo cargar ningún DataFrame {df}")
                df = df.repartition(50).persist(StorageLevel.MEMORY_AND_DISK)
            
                # Aplicar transformaciones específicas
                if file_type == 'fa':
                    df = df.withColumn("pg_fecfac", F.to_date(F.col("pg_fecfac"), "yyyy-MM-dd"))

                # Validar columnas
                self.validate_columns(df, file_type)
                
                dataframes[file_type] = df
                self.logger.info(f"Registros cargados en {file_type.upper()}: {df.count()}")

            return dataframes['op'], dataframes['fa'], dataframes['ca'], dataframes['ce']
            
        except Exception as e:
            self.logger.error(f"Error al cargar y validar datos: {sanitize_for_log(str(e))}")
            raise

    def process_ce_data(self, df_ce):
        try:
            filtered_ce = df_ce.filter(col("od_codconeco").isin(["7550", "7500"]))
            filtered_ce_count = filtered_ce.count()
            self.logger.info(f"Registros en CE después de filtrar por od_codconeco: {filtered_ce_count}")

            if filtered_ce_count == 0:
                self.logger.warning("No se encontraron registros válidos en CE")
                return None

            # Obtener registros más recientes
            window_spec = Window.partitionBy("od_codcom1", "od_numrefrem", "od_numreffacrem").orderBy(desc("od_numorden"))
            ce_with_row_num = filtered_ce.withColumn("row_num", row_number().over(window_spec))
            latest_ce = ce_with_row_num.filter(col("row_num") == 1).drop("row_num")

            # Seleccionar columnas necesarias
            result_ce = latest_ce.select(
                col("od_codcom1").alias("CODCOM"),
                col("od_numrefrem").alias("NUMREFREM"),
                col("od_numreffacrem").alias("NUMREFFACREM"),
                col("od_prapleco").alias("PRAPLECO")
            )

            self.logger.info(f"Registros CE procesados: {result_ce.count()}")
            return result_ce
            
        except Exception as e:
            self.logger.error(f"Error al procesar datos de CE: {sanitize_for_log(str(e))}")
            raise

    def split_complete_incomplete_data(self, df):
        """Divide datos en completos e incompletos manteniendo TODA la data de FA"""
        # Contar registros iniciales para verificación
        initial_count = df.count()
        self.logger.info(f"Registros iniciales FA para análisis: {initial_count}")
        
        # Los datos OP son los que vienen del JOIN con la tabla OP
        campos_op_disponibles = [col for col in self.campos_de_op if col in df.columns]
        
        self.logger.info(f"Campos OP disponibles para evaluar: {campos_op_disponibles}")
        
        if not campos_op_disponibles:
            self.logger.warning("No hay campos OP disponibles, todos los registros irán a incompletos")
            # Si no hay campos OP, todos van a incompletos
            df_complete = df.limit(0)  # DataFrame vacío con el mismo schema
            df_for_enrichment = df
        else:
            # Construir expresión para verificar si tiene datos OP completos
            # Un registro tiene datos OP completos si TODOS los campos OP tienen valores válidos
            tiene_datos_op_expr = None
            for col_name in campos_op_disponibles:
                col_check = (col(col_name).isNotNull()) & (col(col_name) != "") & (col(col_name) != "null")
                tiene_datos_op_expr = col_check if tiene_datos_op_expr is None else tiene_datos_op_expr & col_check
            
            # Marcar registros
            if tiene_datos_op_expr is None:
                marked_df = df.withColumn("tiene_datos_op", F.lit(False))
            else:
                marked_df = df.withColumn("tiene_datos_op", 
                                        F.when(tiene_datos_op_expr.isNull(), False)
                                        .otherwise(tiene_datos_op_expr))
                
            # DIVISIÓN SIMPLE:
            # 1. Registros CON datos OP completos -> van a completos
            df_complete = marked_df.filter(col("tiene_datos_op") == True)
            
            # 2. Registros SIN datos OP completos (incluye nulls) -> van para enriquecimiento
            df_for_enrichment = marked_df.filter(
                (col("tiene_datos_op") == False) | 
                (col("tiene_datos_op").isNull())
            )
            
            # Limpiar columnas auxiliares
            df_complete = df_complete.drop("tiene_datos_op")
            df_for_enrichment = df_for_enrichment.drop("tiene_datos_op")
        
        # Contar cada categoría
        complete_count = df_complete.count()
        enrichment_count = df_for_enrichment.count()
        
        self.logger.info(f"Registros CON datos OP (completos): {complete_count}")
        self.logger.info(f"Registros SIN datos OP (para enriquecimiento): {enrichment_count}")
        
        # Verificar que no perdemos registros
        total_categorized = complete_count + enrichment_count
        self.logger.info(f"Total categorizado: {total_categorized}")
        
        if total_categorized != initial_count:
            self.logger.error(f"PÉRDIDA DE DATOS EN CATEGORIZACIÓN: Inicial {initial_count} vs Categorizado {total_categorized}")
            raise ValueError(f"Pérdida de datos: Inicial {sanitize_for_log(str(initial_count))} vs Categorizado {sanitize_for_log(str(total_categorized))}")
        else:
            self.logger.info("VERIFICACIÓN EXITOSA: No se perdieron registros en la categorización")
        
        return df_complete, df_for_enrichment

    def process_data(self, df_op, df_fa, df_ca, df_ce):
        """Procesa los datos manteniendo TODA la data de FA"""
        # Conteos iniciales
        fa_initial_count = df_fa.count()
        self.logger.info(f"OBJETIVO PRINCIPAL: Mantener {fa_initial_count} registros de FA")

        # Filtrar OP por keywords si existen
        filtered_op = self._filter_op_by_keywords(df_op)
        
        # Procesar CE
        ce_data = self.process_ce_data(df_ce)

        # Transformar columnas
        renamed_fa = self.rename_fa_columns(df_fa)
        selected_op = self._select_op_columns(filtered_op)
        
        # CAMBIO CRÍTICO: Usar LEFT JOIN para mantener TODA la data de FA
        self.logger.info("Realizando LEFT JOIN para mantener TODA la data de FA...")
        
        # JOIN FA + OP (LEFT JOIN - mantiene toda FA)
        joined_df = renamed_fa.join(selected_op, on=["CODCOM", "SECOPE"], how="left")
        
        # Verificar inmediatamente después del join principal
        after_op_join_count = joined_df.count()
        self.logger.info(f"Registros después de JOIN FA+OP: {after_op_join_count}")
        
        if after_op_join_count != fa_initial_count:
            self.logger.error(f"PÉRDIDA CRÍTICA en JOIN FA+OP: FA={sanitize_for_log(str(fa_initial_count))}, Post-JOIN={sanitize_for_log(str(after_op_join_count))}")
            raise ValueError(f"Pérdida de datos en JOIN principal: {fa_initial_count} -> {after_op_join_count}")

        # Estadísticas del join
        registros_con_op = joined_df.filter(
            col("IMPFACFAC").isNotNull() & 
            col("CODTERM").isNotNull() & 
            col("DESCODMAR").isNotNull()
        ).count()
        
        registros_sin_op = after_op_join_count - registros_con_op
        
        self.logger.info(f"Registros de FA CON datos OP: {registros_con_op}")
        self.logger.info(f"Registros de FA SIN datos OP (necesitan enriquecimiento): {registros_sin_op}")

        # JOIN con CE si hay datos (LEFT JOIN)
        if ce_data is not None and ce_data.count() > 0:
            self.logger.info("Uniendo con CE para obtener prapleco (LEFT JOIN)...")
            joined_df = joined_df.join(
                ce_data,
                (joined_df["CODCOM"] == ce_data["CODCOM"]) & 
                (joined_df["NUMREFREM"] == ce_data["NUMREFREM"]) & 
                (joined_df["NUMREFFACREM"] == ce_data["NUMREFFACREM"]),
                how="left"
            ).drop(ce_data["CODCOM"]).drop(ce_data["NUMREFREM"]).drop(ce_data["NUMREFFACREM"])
            
            # Verificar después del join con CE
            after_ce_join_count = joined_df.count()
            self.logger.info(f"Registros después de JOIN con CE: {after_ce_join_count}")
            
            if after_ce_join_count != fa_initial_count:
                self.logger.error(f"PERDIDA en JOIN con CE: {sanitize_for_log(str(fa_initial_count))} -> {sanitize_for_log(str(after_ce_join_count))}")
                raise ValueError("Pérdida de datos en JOIN con CE")
        else:
            self.logger.warning("No se encontraron datos válidos en CE para unir")

        # JOIN con CA para descripción de cadena (LEFT JOIN)
        self.logger.info("Uniendo con tabla CA para obtener descripción de cadena (LEFT JOIN)...")
        joined_df = joined_df.join(
            df_ca.select(
                F.col("pg_codcadena").alias("CODCADENA_JOIN"),
                F.col("pg_descadena").alias("DESCADENA")
            ),
            joined_df["CODCADENA"] == F.col("CODCADENA_JOIN"),
            how="left"
        ).drop("CODCADENA_JOIN")

        # Verificación final antes de finalizar
        before_finalize_count = joined_df.count()
        self.logger.info(f"Registros antes de finalizar: {before_finalize_count}")
        
        if before_finalize_count != fa_initial_count:
            self.logger.error(f"PERDIDA antes de finalizar:  {sanitize_for_log(str(fa_initial_count))} -> {sanitize_for_log(str(before_finalize_count))}")
            raise ValueError("Perdida de datos antes de finalizar")

        # Limpiar y finalizar
        final_df = self._finalize_dataframe(joined_df)
        
        # Verificar integridad final
        final_count = final_df.count()
        self.logger.info(f"Registros FINALES: {final_count}")
        
        if final_count != fa_initial_count:
            self.logger.error(f"PERDIDA FINAL:  FA={sanitize_for_log(str(fa_initial_count))}, Final={sanitize_for_log(str(final_count))}")
            raise ValueError(f"Pérdida de datos final: {fa_initial_count} -> {final_count}")
        else:
            self.logger.info("✅ ÉXITO: Se mantuvo TODA la data de FA")
        
        return final_df

    def _filter_op_by_keywords(self, df_op):
        """Filtra OP por keywords si están definidas"""
        if self.KEYWORDS:
            keywords_str = "|".join(self.KEYWORDS)
            self.logger.info(f"Filtrando OP por keywords: {keywords_str}")
            return df_op.filter(F.col("od_nomcomred").rlike("(?i)" + keywords_str))
        else:
            self.logger.info("Sin filtros de keywords definidos")
            return df_op

    def _select_op_columns(self, df_op):
        """Selecciona columnas necesarias de OP"""
        return df_op.select(
            F.col("od_codcom1").alias("CODCOM"),
            F.col("od_secope").alias("SECOPE"),
            F.col("od_impfacfac").alias("IMPFACFAC"),
            F.col("od_codterm").alias("CODTERM"),
            F.col("od_descodmar").alias("DESCODMAR"),
            F.col("od_inderror").alias("INDERROR")
        )
    
    def _finalize_dataframe(self, joined_df):
        """Limpia y finaliza el DataFrame con las columnas requeridas"""
        self.logger.info("Finalizando DataFrame...")
        
        # Limpiar valores vacíos
        for column in joined_df.columns:
            joined_df = joined_df.withColumn(column, 
                                        F.when(F.col(column) == "", None).otherwise(F.col(column)))

        # Seleccionar columnas finales
        valid_final_columns = [col for col in self.final_columns if col in joined_df.columns]
        final_df = joined_df.select(*valid_final_columns)
        
        return final_df
    
    def rename_fa_columns(self, df):
        """
        Renombra las columnas del DataFrame FA
        
        Args:
            df: DataFrame FA
            
        Returns:
            DataFrame con columnas renombradas
        """
        mapping = {
            "pg_codcom": "CODCOM",
            "pg_secope": "SECOPE",
            "pg_pan": "PAN",
            "pg_numaut": "NUMAUT",
            "pg_totcuotas": "TOTCUOTAS",
            "pg_numcuotact": "NUMCUOTACT",
            "pg_codhash2": "CODHASH2",
            "pg_fecfac": "FECFAC",
            "pg_valorcom": "VALORCOM",
            "pg_valorretf": "VALORRETF",
            "pg_valoriva": "VALORIVA",
            "pg_valorretiva": "VALORRETIVA",
            "pg_valivacom": "VALIVACOM",
            "pg_valorcons": "VALORCONS",
            "pg_indnorcor": "INDNORCOR",
            "pg_tipofac": "TIPOFAC",
            "pg_destipfac": "DESTIPFAC",
            "pg_voucher": "VOUCHER",
            "pg_inddebcre": "INDDEBCRE",
            "pg_codmar": "CODMAR",
            "pg_tipfran": "TIPFRAN",
            "pg_desfra": "DESTIPFRAN",
            "pg_codcadena": "CODCADENA",
            "pg_codholding": "CODHOLDING",
            "pg_numrefrem": "NUMREFREM",
            "pg_numreffacrem": "NUMREFFACREM",
            "pg_recap": "RECAP",
        }
        
        for old_col, new_col in mapping.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)
            else:
                self.logger.warning(f"Columna {old_col} no encontrada para renombrar a {new_col}")
            
        return df

    def save_result(self, df_complete, df_incomplete):
        """
        Guarda los DataFrames de datos completos e incompletos como archivos CSV planos (sin carpetas),
        tanto en local como en S3.
        """
        self.logger.info("Validando cantidades antes de guardar:")
        df_complete.cache()
        df_incomplete.cache()

        complete_count = df_complete.count()
        incomplete_count = df_incomplete.count()
        self.logger.info(f"Guardando {complete_count} registros completos y {incomplete_count} registros incompletos")

        # Limpieza de miles en IMPFACFAC y recortar RECAP si existen
        df_complete = self._clean_impfacfac_and_recap(df_complete)
        df_incomplete = self._clean_impfacfac_and_recap(df_incomplete)

        # Determinar la fecha para el nombre del archivo
        if self.process_date:
            date_str = self.process_date.replace("-", "")
        else:
            date_str = datetime.now().strftime("%Y%m%d")
            self.logger.warning(f"No se detectó una fecha de proceso, usando la fecha actual: {date_str}")

        file_name_complete = f"BBCONSOLIDADOMNT{date_str}.csv"
        file_name_incomplete = f"BBCONSOLIDADOMNT{date_str}_INCOMPLETO.csv"

        # Guardar ambos archivos usando los métodos específicos
        if complete_count > 0:
            if self.use_local_files:
                self._save_csv_local(df_complete, file_name_complete)
            else:
                self._save_csv_s3(df_complete, file_name_complete)
        
        if incomplete_count > 0:
            if self.use_local_files:
                self._save_csv_local(df_incomplete, file_name_incomplete)
            else:
                self._save_csv_s3(df_incomplete, file_name_incomplete)

    def _save_csv_local(self, df, file_name):
        """
        Guarda un DataFrame como CSV en el sistema de archivos local.
        """
        row_count = df.count()
        self.logger.info(f"Guardando DataFrame local con {row_count} registros en {file_name}")

        if row_count == 0:
            self.logger.warning(f"El DataFrame para {file_name} está vacío. No se generará archivo.")
            return

        temp_dir = os.path.join(self.local_base_dir, f"temp_{file_name}")
        final_path = os.path.join(self.local_base_dir, file_name)
        
        try:
            self.logger.info(f"Guardando archivo local en directorio temporal: {temp_dir}")
            df.coalesce(1).write.mode("overwrite") \
                .option("header", True) \
                .option("sep", ";") \
                .csv(temp_dir)
            
            # Buscar el archivo part generado
            part_file = glob.glob(os.path.join(temp_dir, "part-*.csv"))
            if part_file:
                shutil.move(part_file[0], final_path)
                self.logger.info(f"✅ Archivo generado exitosamente en local: {final_path}")
            else:
                self.logger.error(f"No se encontró archivo CSV generado en {sanitize_for_log(temp_dir)}")
                if os.path.exists(temp_dir):
                    files_in_temp = os.listdir(temp_dir)
                    self.logger.error(f"Archivos encontrados en {sanitize_for_log(temp_dir)}: {sanitize_for_log(str(files_in_temp))}")
                raise FileNotFoundError(f"No se pudo generar el archivo local {file_name}")
        except Exception as e:
            self.logger.error(f"Error al guardar archivo local {sanitize_for_log(file_name)}: {sanitize_for_log(str(e))}")
            raise
            
        finally:
            # Limpiar directorio temporal
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

    def _save_csv_s3(self, df, file_name):
        """
        Guarda un DataFrame como CSV directamente en S3 sin crear carpetas intermedias.
        El archivo se guarda como file_name.csv directamente en el bucket.
        """

        s3_bucket_salida = self.s3_bucket_salida
        
        s3_client = boto3.client('s3')
        
        try:
            # PASO 1: Escribir a S3 en carpeta temporal (como hacía el código original)
            temp_s3_path = f"s3a://{s3_bucket_salida}/temp_{file_name.replace('.csv', '')}"
            
            self.logger.info(f"Escribiendo temporalmente a S3: {temp_s3_path}")
            df.coalesce(1).write.mode("overwrite") \
                .option("header", True) \
                .option("sep", ";") \
                .csv(temp_s3_path)
            
            # PASO 2: Listar archivos en la carpeta temporal
            s3_prefix = f"temp_{file_name.replace('.csv', '')}/"
            response = s3_client.list_objects_v2(Bucket=s3_bucket_salida, Prefix=s3_prefix)
            
            if 'Contents' not in response:
                raise FileNotFoundError(f"No se encontraron archivos en {s3_prefix}")
            
            # PASO 3: Encontrar el archivo part-*.csv
            part_file_key = None
            for obj in response['Contents']:
                if obj['Key'].endswith('.csv') and 'part-' in obj['Key']:
                    part_file_key = obj['Key']
                    break
            
            if not part_file_key:
                available_files = [obj['Key'] for obj in response['Contents']]
                self.logger.error(f"No se encontró archivo part-*.csv. Archivos disponibles: {sanitize_for_log(str(available_files))}")
                raise FileNotFoundError(f"No se encontró archivo part en {s3_prefix}")
            
            # PASO 4: Copiar el archivo part a la ubicación final con el nombre deseado
            final_s3_key = file_name
            
            self.logger.info(f"Copiando de {part_file_key} a {final_s3_key}")
            s3_client.copy_object(
                Bucket=s3_bucket_salida,
                CopySource={'Bucket': s3_bucket_salida, 'Key': part_file_key},
                Key=final_s3_key
            )
            
            # PASO 5: Limpiar la carpeta temporal
            self.logger.info(f"Limpiando carpeta temporal {s3_prefix}")
            for obj in response['Contents']:
                s3_client.delete_object(Bucket=s3_bucket_salida, Key=obj['Key'])
            
            self.logger.info(f"✅ Archivo generado exitosamente en S3: s3://{s3_bucket_salida}/{final_s3_key}")
                
        except Exception as e:
            self.logger.error(f"Error al guardar archivo en S3 {sanitize_for_log(file_name)}: {sanitize_for_log(str(e))}")
            # Intentar limpiar en caso de error
            try:
                s3_prefix = f"temp_{file_name.replace('.csv', '')}/"
                response = s3_client.list_objects_v2(Bucket=s3_bucket_salida, Prefix=s3_prefix)
                if 'Contents' in response:
                    for obj in response['Contents']:
                        s3_client.delete_object(Bucket=s3_bucket_salida, Key=obj['Key'])
            except Exception as cleanup_err:
                    self.logger.error(f"Error al limpiar archivos temporales en S3: {sanitize_for_log(str(cleanup_err))}")
            raise

    def _clean_impfacfac_and_recap(self, df):
        """Limpia IMPFACFAC Y RECORTA RECAP SI EXISTEN"""
        if "IMPFACFAC" in df.columns:
            df = df.withColumn(
                "IMPFACFAC",
                regexp_replace(col("IMPFACFAC").cast("string"), ",", "")
            )
        if "RECAP" in df.columns:
            df = df.withColumn(
                "RECAP",
                F.substring(col("RECAP").cast("string"), -8, 8)
            )
        return df

    def run(self):
        """
        Ejecuta todo el proceso de ETL con el nuevo enfoque de mantener FA completo
        """
        self.logger.info("Iniciando proceso de ETL con mantenimiento completo de FA")
        
        # Cargar y validar datos
        self.logger.info("Paso 1: Cargando y validando datos")
        df_op, df_fa, df_ca, df_ce = self.load_and_validate_data()
        
        fa_count = df_fa.count()
        op_count = df_op.count()
        ca_count = df_ca.count()
        ce_count = df_ce.count()
        
        self.logger.info(f"Conteos iniciales - FA: {fa_count}, OP: {op_count}, CA: {ca_count}, CE: {ce_count}")
        
        # CAMBIO: El objetivo es mantener TODA la data de FA
        self.logger.info(f"Objetivo: Mantener los {fa_count} registros de FA")

        # Procesar datos (ahora mantiene FA completo)
        self.logger.info("Paso 2: Procesando datos (manteniendo FA completo)")
        final_df = self.process_data(df_op, df_fa, df_ca, df_ce)
        
        # Verificar que mantenemos toda la data de FA
        final_count = final_df.count()
        self.logger.info(f"Registros después de procesar: {final_count}")
        
        if final_count != fa_count:
            self.logger.error(f"ERROR CRÍTICO: Se perdieron registros de FA. Inicial: {sanitize_for_log(str(fa_count))}, Final: {sanitize_for_log(str(final_count))}")
            # Aquí podríamos lanzar una excepción si es crítico
            # raise ValueError(f"Pérdida de data: FA inicial {fa_count} vs Final {final_count}")
        else:
            self.logger.info("ÉXITO: Se mantuvo toda la data de FA")

        # Liberar memoria de los DataFrames originales
        df_op.unpersist(blocking=True)
        df_fa.unpersist(blocking=True)
        df_ca.unpersist(blocking=True)
        df_ce.unpersist(blocking=True)
        
        # Dividir datos en completos e incompletos (nueva lógica)
        self.logger.info("Paso 3: Identificando datos completos vs. datos que necesitan enriquecimiento")
        df_complete, df_for_enrichment = self.split_complete_incomplete_data(final_df)
        
        complete_count = df_complete.count()
        enrichment_count = df_for_enrichment.count()
        
        self.logger.info(f"Datos completos: {complete_count}")
        self.logger.info(f"Datos para enriquecimiento: {enrichment_count}")
        self.logger.info(f"Total: {complete_count + enrichment_count} (debe ser igual a FA inicial: {fa_count})")
        
        # Liberar memoria del DataFrame final
        final_df.unpersist(blocking=True)
        
        # Enriquecer datos faltantes desde la base de datos
        self.logger.info("Paso 4: Enriqueciendo datos faltantes con información de la BD")

        if hasattr(self, 'mysql') and self.mysql is not None and enrichment_count > 0:
            self.logger.info(f"Iniciando enriquecimiento de {enrichment_count} registros")
            df_enriquecido, df_incompleto_final = enriquecer_con_bd(df_for_enrichment, self.mysql, self.logger)
            
            enriquecido_count = df_enriquecido.count()
            incompleto_final_count = df_incompleto_final.count()
            
            self.logger.info(f"Resultado enriquecimiento: {enriquecido_count} enriquecidos, {incompleto_final_count} siguen incompletos")
        else:
            if enrichment_count > 0:
                self.logger.warning(f"Sin conexión BD: {enrichment_count} registros quedarán incompletos")
            df_enriquecido = df_for_enrichment.limit(0)  # DataFrame vacío
            df_incompleto_final = df_for_enrichment
            enriquecido_count = 0
            incompleto_final_count = enrichment_count
        
        # Liberar memoria del DataFrame para enriquecimiento
        df_for_enrichment.unpersist(blocking=True)
        
        # Unir datos completos con los enriquecidos
        if enriquecido_count > 0:
            self.logger.info(f"Uniendo {complete_count} completos + {enriquecido_count} enriquecidos")
            df_completo_final = df_complete.unionByName(df_enriquecido, allowMissingColumns=True)
        else:
            df_completo_final = df_complete
        
        completo_final_count = df_completo_final.count()
        
        # Verificación final de integridad
        total_final = completo_final_count + incompleto_final_count
        self.logger.info(f"Verificación final: Completos: {completo_final_count}, Incompletos: {incompleto_final_count}, Total: {total_final}")
        
        if total_final != fa_count:
            self.logger.error(f"ERROR: Total final ({sanitize_for_log(str(total_final))}) != FA inicial ({sanitize_for_log(str(fa_count))})")
        else:
            self.logger.info("VERIFICACIÓN EXITOSA: Se mantuvieron todos los registros de FA")
        
        # Guardar resultados
        self.logger.info("Paso 5: Guardando resultados")
        self.save_result(df_completo_final, df_incompleto_final)
        
        # Liberar memoria final
        df_completo_final.unpersist(blocking=True)
        df_incompleto_final.unpersist(blocking=True)
        
        # Resumen final
        self.logger.info("="*50)
        self.logger.info("RESUMEN DEL PROCESO ETL")
        self.logger.info("="*50)
        self.logger.info(f"FA inicial: {fa_count}")
        self.logger.info(f"Registros completos finales: {completo_final_count}")
        self.logger.info(f"Registros incompletos finales: {incompleto_final_count}")
        self.logger.info(f"Total procesado: {total_final}")
        self.logger.info(f"Integridad de datos: {'✅ EXITOSA' if total_final == fa_count else '❌ FALLIDA'}")
        self.logger.info("="*50)
        
        self.logger.info("Proceso de ETL completado")