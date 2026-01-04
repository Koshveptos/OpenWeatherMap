from loguru import logger
import sys
from history import load_data

logger.remove()  
logger.add("logs/app.log", rotation="500 MB", level="DEBUG") 
logger.add( sink=sys.stdout, level="INFO") 


df = load_data('')