import re 
import boto3 #type: ignore
import zipfile
import io
import json

from typing import Optional, List

S3_PREFIX = "s3://"

def validate_safe_path(user_input: str, allowed_values: Optional[List[str]] = None, max_length: int = 50) -> str:
    """Valida y sanitiza entrada de usuario para prevenir Path Traversal (CWE-73)."""
    if not user_input or not isinstance(user_input, str):
        raise ValueError("Entrada invalida, debe ser una cadena no vacia")

    if len(user_input) > max_length:
        raise ValueError(f"Entrada demasiado larga. Máximo {max_length} caracteres")

    #Remover caracteres peligrosos
    dangerous_chars = ['..', '/', '\\', '\0', '\n', '\r', '\t']
    for char in dangerous_chars:
        if char in user_input:
            raise ValueError("Caracter no permitido")
    
    #Validar contra WhiteList
    if allowed_values and user_input not in allowed_values:
        raise ValueError("Valor no permitido")

    #Solo permitir caracter alfanumericos, guiones y subguiones
    if not re.match(r'^[a-zA-Z0-9_-]+$', user_input):
        raise ValueError("Solo se permiten estos alfanumericos, guiones y subguiones")
    
    return user_input 

def sanitize_for_log(data):
    """Sanitiza datos para prevenir LOG INJECTION"""
    if data is None:
        return "None"
    
    #Convertir a string si no lo es
    if not isinstance(data, str):
        try:
            data_str = str(data)
        except Exception:
            return "[UNPRINTABLE_DATA]"
    else:
        data_str = data

    #Remover caracteres peligrosos
    sanitized = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', data_str)

    #Remover secuencias ANSI/escape
    sanitized = re.sub(r'\x1b\[[0-9;]*[mGKH]', '', sanitized)

    #Limitar longitud para prevenir log 
    max_length = 500
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length] + "...[TRUNCATED]"
    
    #Escapar caracteres especiales para HTML 
    sanitized = sanitized.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    return sanitized

def validate_config_path(config_path: str) -> str:
    """Valida el path de configuración para prevenir Path Traversal (CWE-73)"""
    if not config_path or not isinstance(config_path, str):
        raise ValueError("Path de configuración inválido")

    # Si es un path S3, validar formato
    if config_path.startswith(S3_PREFIX):
        s3_path = config_path.replace(S3_PREFIX, '')
        # Prevenir path traversal en S3
        if '..' in s3_path or s3_path.startswith('/'):
            raise ValueError("Path S3 no seguro detectado")
        # Validar formato S3 básico
        if not re.match(r'^[a-zA-Z0-9._/-]+\.(json|zip)$', s3_path):
            raise ValueError("Formato de path S3 inválido")
        return config_path

    # Para paths locales, usar whitelist estricta
    allowed_local_paths = [
        'config/dev.json',
        'config/iroute.json',
        'config/bb.json',
        'config/prod.json',
        'config/test.json'
    ]

    if config_path not in allowed_local_paths:
        raise ValueError(f"Path local no permitido: {sanitize_for_log(config_path)}")

    return config_path

def validate_zip_path(path: str) -> str:
    """Valida que el path dentro del ZIP sea seguro"""
    if not path or not isinstance(path, str):
        raise ValueError("Path inválido en ZIP")

    # Prevenir path traversal
    if '..' in path or path.startswith('/') or path.startswith('\\'):
        raise ValueError(f"Path no seguro detectado en ZIP: {sanitize_for_log(path)}")

    # Normalizar separadores
    normalized_path = path.replace('\\', '/')

    # Whitelist de paths permitidos dentro del ZIP
    allowed_patterns = [
        r'^config/[a-zA-Z0-9_-]+\.json$',
        r'^[a-zA-Z0-9_-]+\.json$'
    ]

    if not any(re.match(pattern, normalized_path) for pattern in allowed_patterns):
        raise ValueError(f"Path no permitido en ZIP: {sanitize_for_log(path)}")

    return path

def get_allowed_environments() -> List[str]:
    """Retorna la lista de ambientes permitidos"""
    return ['dev', 'iroute', 'bb', 'prod', 'test']

def load_config_from_s3_zip(s3_zip_path: str, env: str, logger) -> dict:
    """
    Carga configuración desde un ZIP en S3 de forma segura
    """
    if not s3_zip_path or not isinstance(s3_zip_path, str):
        raise ValueError("Path S3 ZIP inválido")

    # Validar formato S3
    if not s3_zip_path.startswith(S3_PREFIX):
        raise ValueError("Debe ser un path S3 válido")

    # Validar environment
    validated_env = validate_safe_path(env, get_allowed_environments())

    try:
        bucket_name, key = s3_zip_path.replace(S3_PREFIX, "").split("/", 1)
        logger.info(f"Cargando configuración desde S3: {sanitize_for_log(s3_zip_path)}")

        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        zip_content = response['Body'].read()

        with zipfile.ZipFile(io.BytesIO(zip_content), 'r') as zip_ref:
            possible_paths = [
                f"config/{validated_env}.json",
                f"config\\{validated_env}.json",
                f"{validated_env}.json"
            ]

            # Validar todos los paths en el ZIP
            zip_contents = zip_ref.namelist()
            for zip_path in zip_contents:
                try:
                    validate_zip_path(zip_path)
                except ValueError:
                    logger.warning(f"Path no seguro ignorado en ZIP: {sanitize_for_log(zip_path)}")

            # Buscar archivo de configuración
            for path in possible_paths:
                if path in zip_contents:
                    # Validar path antes de abrir
                    validated_path = validate_zip_path(path)
                    logger.info(f"Encontrado archivo: {sanitize_for_log(validated_path)}")

                    with zip_ref.open(validated_path) as f:
                        config = json.load(f)
                        logger.info("Configuración cargada correctamente desde ZIP")
                        return config

            raise FileNotFoundError(f"Archivo de configuración para {sanitize_for_log(validated_env)} no encontrado en ZIP")

    except Exception as e:
        logger.error(f"Error cargando configuración desde S3 ZIP: {sanitize_for_log(str(e))}")
        raise
