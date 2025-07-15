import logging
import sys
import os
from datetime import datetime
from utils.utils import sanitize_for_log

def setup_logging(level=logging.INFO):
    """
    Configura y devuelve un logger
    
    Args:
        level: Nivel de logging (default: INFO)
        
    Returns:
        Logger configurado
    """
    # Crear logger
    logger = logging.getLogger("consolidadoBB")
    logger.setLevel(level)
    
    # Limpiar handlers existentes
    if logger.handlers:
        logger.handlers.clear()
    
    # Crear handler para la consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Crear formato de log
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(log_format)
    
    # Añadir handler al logger
    logger.addHandler(console_handler)
    
    # Crear handler para archivo en el modo local
    try:
        log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../logs'))
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        log_file = os.path.join(log_dir, f"consolidadoBB_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(log_format)
        logger.addHandler(file_handler)
    except Exception as e:
        # En entornos como AWS Glue, el logging a archivo puede no estar disponible
        logger.warning(f"No se pudo configurar el logging a archivo: {sanitize_for_log(str(e))}")
    
    return logger