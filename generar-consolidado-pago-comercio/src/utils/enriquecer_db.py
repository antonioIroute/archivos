from pyspark.sql.functions import col, desc, when, format_number, row_number, lit #type: ignore
from pyspark.sql.window import Window #type: ignore

def obtener_datos_mysql_filtrados(mysql_connector, tabla, filtros_list, logger, use_latest_date=True):
    """
    Obtiene datos desde una tabla MySQL aplicando filtros específicos
    
    Args:
        mysql_connector: Conector MySQL
        tabla (str): Nombre de la tabla
        filtros_list (list): Lista de diccionarios con filtros a aplicar
        logger: Logger para registrar eventos
        use_latest_date (bool): Si es True, obtiene solo registros con la fecha más reciente
        
    Returns:
        DataFrame: Datos filtrados o None si hay error
    """
    try:
        # Verificar si está disponible la función para obtener solo los registros más recientes
        if use_latest_date and hasattr(mysql_connector, 'execute_batch_query_latest_date'):
            logger.info("Ejecutando consulta con filtro para fecha más reciente")
            df_mysql = mysql_connector.execute_batch_query_latest_date(tabla, filtros_list)
        else:
            # Fallback a la función original
            logger.info("Ejecutando consulta sin filtro para fecha más reciente")
            df_mysql = mysql_connector.execute_batch_query(tabla, filtros_list)
        
        if df_mysql is None:
            logger.error(f"No se pudieron obtener datos desde la tabla MySQL {tabla} con los filtros proporcionados.")
            return None
        
        # Verificar si ya existe la columna od_fecfac_str
        if "op_fecfac_str" not in df_mysql.columns and "op_fecfac" in df_mysql.columns:
            # Convertir fecha a string para unión con otros dataframes
            df_mysql = df_mysql.withColumn("op_fecfac_str", col("op_fecfac").cast("string"))
            
        return df_mysql
    except Exception as e:
        logger.error(f"Error al leer datos filtrados de MySQL: {str(e)}")
        return None

def generar_filtros_desde_dataframe(df_incompleto, logger):
    """
    Genera filtros optimizado para grandes volúmenes de datos (3000+ registros)
    """
    try:
        columnas_filtro = {
            "CODCOM": "op_codcom",
            "PAN": "op_pan", 
            "NUMAUT": "op_numaut",
            "FECFAC": "op_fecfac"
        }
        
        # Obtener registros únicos - esto ya reduce considerablemente el volumen
        df_filtros = df_incompleto.select([col(c) for c in columnas_filtro.keys()]).distinct()
        
        total_registros = df_filtros.count()
        logger.info(f"Total de registros únicos para procesar: {total_registros}")
        
        # ESTRATEGIA SEGÚN EL VOLUMEN DE DATOS
        if total_registros <= 1000:
            # Volumen pequeño: collect() directo
            logger.info("Usando estrategia directa para volumen pequeño")
            registros = df_filtros.collect()
            return procesar_registros_para_filtros(registros, columnas_filtro)
            
        elif total_registros <= 10000:
            # Volumen medio: usar take() en chunks
            logger.info("Usando estrategia de chunks para volumen medio")
            return procesar_con_chunks(df_filtros, columnas_filtro, logger)
            
        else:
            # Volumen grande: usar procesamiento distribuido
            logger.info("Usando estrategia distribuida para volumen grande")
            return procesar_distribuido(df_filtros, columnas_filtro, logger)
            
    except Exception as e:
        logger.error(f"Error al generar filtros desde dataframe: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []

def procesar_con_chunks(df_filtros, columnas_filtro, logger):
    """
    Procesa el DataFrame en chunks usando take() y skip()
    Ideal para volúmenes medianos (1K-10K registros)
    """
    try:        
        # Agregar número de fila para poder hacer "paginación"
        window_spec = Window.orderBy(lit(1))
        df_numerado = df_filtros.withColumn("row_num", row_number().over(window_spec))
        
        chunk_size = 1000
        total_registros = df_numerado.count()
        filtros_list = []
        
        for i in range(0, total_registros, chunk_size):
            inicio = i + 1
            fin = min(i + chunk_size, total_registros)
            
            # Filtrar chunk actual
            chunk_df = df_numerado.filter(
                (col("row_num") >= inicio) & (col("row_num") <= fin)
            ).drop("row_num")
            
            # Procesar chunk
            registros_chunk = chunk_df.collect()
            filtros_chunk = procesar_registros_para_filtros(registros_chunk, columnas_filtro)
            filtros_list.extend(filtros_chunk)
            
            logger.info(f"Procesado chunk {i//chunk_size + 1}: registros {inicio}-{fin}, {len(filtros_chunk)} filtros")
            
        return filtros_list
        
    except Exception as e:
        logger.error(f"Error en procesamiento por chunks: {str(e)}")
        raise

def procesar_distribuido(df_filtros, columnas_filtro, logger):
    """
    Procesamiento completamente distribuido para volúmenes muy grandes
    """
    try:
        # Calcular número óptimo de particiones
        total_registros = df_filtros.count()
        registros_por_particion = 1000
        num_particiones = max(1, total_registros // registros_por_particion)
        
        logger.info(f"Reparticionando {total_registros} registros en {num_particiones} particiones")
        
        # Reparticionar para distribuir la carga
        df_reparticionado = df_filtros.repartition(num_particiones)
        
        # Función para procesar cada partición
        def procesar_particion(iterator):
            particion_registros = list(iterator)
            if particion_registros:
                return procesar_registros_para_filtros(particion_registros, columnas_filtro)
            return []
        
        # Aplicar procesamiento distribuido
        resultado_rdd = df_reparticionado.rdd.mapPartitions(procesar_particion)
        
        # Recopilar resultados
        filtros_por_particion = resultado_rdd.collect()
        
        # Aplanar lista
        filtros_list = []
        for filtros_particion in filtros_por_particion:
            if isinstance(filtros_particion, list):
                filtros_list.extend(filtros_particion)
            else:
                # En caso de que sea un solo elemento
                filtros_list.append(filtros_particion)
        
        logger.info(f"Procesamiento distribuido completado: {len(filtros_list)} filtros generados")
        return filtros_list
        
    except Exception as e:
        logger.error(f"Error en procesamiento distribuido: {str(e)}")
        raise

def procesar_registros_para_filtros(registros, columnas_filtro):
    """
    Procesa una lista de registros para generar filtros
    
    Args:
        registros: Lista de registros (Row)
        columnas_filtro: Diccionario con mapeo de columnas
        
    Returns:
        list: Lista de diccionarios con filtros
    """
    filtros_list = []
    
    for reg in registros:
        filtro = {}
        
        # Agregar campos presentes al filtro
        for col_df, col_bd in columnas_filtro.items():
            if hasattr(reg, col_df) and getattr(reg, col_df) is not None and getattr(reg, col_df) != "":
                filtro[col_bd] = getattr(reg, col_df)
        
        # Solo agregar filtros que tengan al menos las claves principales
        if "op_codcom" in filtro and "op_pan" in filtro:
            filtros_list.append(filtro)
    
    return filtros_list

def unir_dataframes(df_incompleto, df_mysql, logger):
    """
    Une el dataframe incompleto con el de MySQL
    """
    try:
        return df_incompleto.alias("incomp").join(
            df_mysql.alias("bd"),
            (col("incomp.CODCOM") == col("bd.op_codcom")) &
            (col("incomp.PAN") == col("bd.op_pan")) &
            (col("incomp.NUMAUT") == col("bd.op_numaut")) &  # AGREGADO: También filtrar por NUMAUT
            (col("incomp.FECFAC") == col("bd.op_fecfac_str")),
            how="left"
        )
    except Exception as e:
        logger.error(f"Error al unir dataframes: {str(e)}")
        raise

def crear_mapeo_columnas_completo():
    """
    Crea el mapeo completo entre columnas del DataFrame y columnas de la BD
    Esto debe incluir TODOS los campos de OP que pueden ser enriquecidos
    """
    return {
        # Campos básicos
        "IMPFACFAC": "bd.op_impfacfac",
        "CODTERM": "bd.op_codterm", 
        "DESCODMAR": "bd.op_descodmar",
        "INDERROR": "bd.op_inderror",
        "CODCADENA": "bd.op_codcadena",
        "DESCADENA": "bd.op_descadena",
        
        # NUEVOS CAMPOS AGREGADOS - Agregar todos los campos de OP que necesites
        "NOMCOMRED": "bd.op_nomcomred",
        "SECOPE": "bd.op_secope",
        
        "NUMREFFACREM": "bd.op_numreffacrem",
        "NUMREFREM": "bd.op_numrefrem",
        
    }


def enriquecer_campos_completo(df_joined, logger):
    """
    Enriquece TODOS los campos del dataframe con datos de MySQL - VERSIÓN COMPLETA
    """
    try:
        logger.info("Iniciando enriquecimiento completo de campos")
        campos_para_enriquecer = crear_mapeo_columnas_completo()
        logger.info(f"Campos disponibles para enriquecer: {list(campos_para_enriquecer.keys())}")

        df_joined, campos_enriquecidos, campos_no_encontrados = _enriquecer_o_agregar_campos(df_joined, campos_para_enriquecer, logger)
        logger.info(f"Campos enriquecidos exitosamente: {campos_enriquecidos}")
        if campos_no_encontrados:
            logger.warning(f"Campos no encontrados para enriquecer: {campos_no_encontrados}")

        df_joined = _formatear_campos_especiales(df_joined)
        return df_joined
    except Exception as e:
        logger.error(f"Error al enriquecer campos completos: {str(e)}")
        raise

def _enriquecer_o_agregar_campos(df, campos_para_enriquecer, logger):
    """
    Enriquecer o agregar campos desde el mapeo de columnas.
    """
    campos_enriquecidos = 0
    campos_no_encontrados = []
    for col_out, col_bd in campos_para_enriquecer.items():
        bd_col_name = col_bd.replace("bd.", "")
        if col_out in df.columns:
            if bd_col_name in df.columns:
                df = df.withColumn(
                    col_out,
                    when(
                        (col(col_out).isNull()) |
                        (col(col_out) == "") |
                        (col(col_out) == "null"),
                        col(col_bd)
                    ).otherwise(col(col_out))
                )
                campos_enriquecidos += 1
                logger.debug(f"Campo {col_out} enriquecido desde {col_bd}")
            else:
                campos_no_encontrados.append(f"{col_out} <- {col_bd}")
        else:
            if bd_col_name in df.columns:
                df = df.withColumn(col_out, col(col_bd))
                campos_enriquecidos += 1
                logger.debug(f"Campo {col_out} agregado desde {col_bd}")
            else:
                campos_no_encontrados.append(f"{col_out} <- {col_bd} (ambos no existen)")
    return df, campos_enriquecidos, campos_no_encontrados

def _formatear_campos_especiales(df):
    """
    Formatea campos especiales como IMPFACFAC y PRAPLECO.
    """
    if "IMPFACFAC" in df.columns:
        df = df.withColumn(
            "IMPFACFAC",
            format_number(col("IMPFACFAC").cast("double"), 2)
        )
    if "PRAPLECO" in df.columns:
        df = df.withColumn(
            "PRAPLECO",
            when((col("PRAPLECO").isNull()) | (col("PRAPLECO") == ""), "0.0000").otherwise(col("PRAPLECO"))
        )
    return df


def evaluar_completitud_registro(row_data, campos_requeridos):
    """
    Evalúa si un registro está completo basado en todos los campos requeridos
    
    Args:
        row_data: Datos del registro
        campos_requeridos: Lista de campos que deben estar completos
        
    Returns:
        bool: True si está completo, False si falta información
    """
    for campo in campos_requeridos:
        if hasattr(row_data, campo):
            valor = getattr(row_data, campo)
            if valor is None or valor == "" or valor == "null":
                return False
        else:
            return False  # Si el campo no existe, no está completo
    return True

def segmentar_por_completitud_mejorado(df_joined, df_incompleto, logger):
    """
    Divide el dataframe entre registros completados y no completados
    VERSIÓN MEJORADA: Evalúa múltiples campos para determinar completitud
    """
    try:
        # CAMPOS CRÍTICOS para considerar un registro como completo
        # Puedes ajustar esta lista según tus necesidades de negocio
        campos_criticos = [
            "IMPFACFAC",    # Importe de facturación
            #"CODTERM",      # Código de terminal  
            "DESCODMAR",    # Descripción código de marca
            # Agregar más campos críticos según necesites
            # "NOMCOMRED",  
            # "CODESTOPE",
        ]
        
        logger.info(f"Evaluando completitud basada en campos críticos: {campos_criticos}")
        
        # Crear condición de completitud dinámica
        condiciones_completitud = []
        
        for campo in campos_criticos:
            if campo in df_joined.columns:
                condicion = (col(campo).isNotNull()) & (col(campo) != "") & (col(campo) != "null")
                condiciones_completitud.append(condicion)
        
        if not condiciones_completitud:
            logger.warning("No hay campos críticos disponibles para evaluar completitud")
            # Si no hay campos críticos, considerar todo como incompleto
            df_enriquecido = df_joined.limit(0)
            df_incompleto_final = df_joined.select(df_incompleto.columns)
        else:
            # Combinar todas las condiciones con AND
            condicion_completado = condiciones_completitud[0]
            for condicion in condiciones_completitud[1:]:
                condicion_completado = condicion_completado & condicion

            # Separar registros completos e incompletos
            df_enriquecido = df_joined.filter(condicion_completado).select(df_incompleto.columns)
            df_incompleto_final = df_joined.filter(~condicion_completado).select(df_incompleto.columns)

        # Contar y reportar resultados
        enriquecidos_count = df_enriquecido.count()
        incompletos_count = df_incompleto_final.count()
        
        logger.info("  - Resultado de segmentación mejorada: ")
        logger.info(f"  - Registros enriquecidos (completos): {enriquecidos_count}")
        logger.info(f"  - Registros aún incompletos: {incompletos_count}")
        
        return df_enriquecido, df_incompleto_final
        
    except Exception as e:
        logger.error(f"Error al segmentar por completitud mejorada: {str(e)}")
        raise

def filtrar_registros_ultimos_processdate(df, logger):
    """
    Filtra un DataFrame de PySpark para mantener solo los registros con el od_processdate más reciente
    para cada combinación única de campos clave.
    """
    try:
        # Verificar si el DataFrame está vacío
        if df is None or df.count() == 0:
            logger.warning("DataFrame vacío o nulo, no se puede filtrar")
            return df
            
        # Verificar si existe la columna od_processdate
        if "op_processdate" not in df.columns:
            logger.warning("La columna op_processdate no existe en el DataFrame, no se puede filtrar")
            return df
            
        # Definir las columnas que identifican un registro único
        unique_keys = ["op_codcom", "op_pan", "op_numaut", "op_fecfac", "op_numrefrem", "op_numreffacrem"]
        
        # Eliminar columnas que no existen en el DataFrame de la lista de claves únicas
        unique_keys = [key for key in unique_keys if key in df.columns]
        
        if not unique_keys:
            logger.warning("No hay columnas clave para agrupar, no se puede filtrar")
            return df
            
        logger.info(f"Filtrando por las columnas únicas: {unique_keys}")
        
        # Definir una ventana para particionar por las columnas clave y ordenar por fecha descendente
        window_spec = Window.partitionBy(*unique_keys).orderBy(desc("op_processdate"))
        
        # Añadir una columna con el número de fila dentro de cada partición
        df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
        
        # Filtrar para mantener solo los registros más recientes
        df_filtered = df_with_row_num.filter(col("row_num") == 1).drop("row_num")
        
        # Registrar conteos para diagnóstico
        count_original = df.count()
        count_filtered = df_filtered.count()
        logger.info(f"Registros originales: {count_original}, después de filtrar: {count_filtered}, eliminados: {count_original - count_filtered}")
        
        return df_filtered
        
    except Exception as e:
        logger.error(f"Error al filtrar registros por fecha más reciente: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return df

def enriquecer_con_bd(df_incompleto, mysql_connector, logger, use_latest_date=True):
    """
    Versión MEJORADA de enriquecimiento que completa TODOS los campos faltantes
    """
    try:
        logger.info("Iniciando proceso de enriquecimiento COMPLETO con MySQL")
        
        # Guardar conteo inicial para diagnóstico
        conteo_inicial = df_incompleto.count()
        logger.info(f"Registros incompletos a procesar: {conteo_inicial}")

        # Si no hay registros incompletos, retornar dataframes vacíos
        if conteo_inicial == 0:
            logger.info("No hay registros incompletos para procesar")
            empty_df = df_incompleto.limit(0)
            return empty_df, empty_df

        # Generar filtros desde el dataframe incompleto
        filtros_list = generar_filtros_desde_dataframe(df_incompleto, logger)
        
        if not filtros_list:
            logger.warning("No se pudieron generar filtros desde el dataframe incompleto")
            return df_incompleto.limit(0), df_incompleto
        
        # Obtener datos filtrados de MySQL (sin filtrar por fecha aún)
        df_mysql = obtener_datos_mysql_filtrados(
            mysql_connector, "adq_t_operac_diaria_op", filtros_list, logger, use_latest_date=False
        )
        
        # Si no se pudo obtener datos de MySQL, retornar los dataframes originales
        if df_mysql is None or df_mysql.count() == 0:
            logger.warning("No se pudieron obtener datos de MySQL")
            return df_incompleto.limit(0), df_incompleto
        
        # Filtrar los registros para quedarnos solo con los más recientes
        if use_latest_date:
            logger.info("Aplicando filtro para obtener solo los registros más recientes")
            df_mysql = filtrar_registros_ultimos_processdate(df_mysql, logger)
        
        # Unir dataframes
        df_joined = unir_dataframes(df_incompleto, df_mysql, logger)
        
        # CAMBIO PRINCIPAL: Usar enriquecimiento completo en lugar del básico
        df_joined = enriquecer_campos_completo(df_joined, logger)
        
        # CAMBIO PRINCIPAL: Usar segmentación mejorada
        df_enriquecido, df_incompleto_final = segmentar_por_completitud_mejorado(df_joined, df_incompleto, logger)
        
        return df_enriquecido, df_incompleto_final

    except Exception as e:
        logger.error(f"Error al enriquecer datos con MySQL: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # En caso de error, retornar el dataframe incompleto original
        return df_incompleto.limit(0), df_incompleto