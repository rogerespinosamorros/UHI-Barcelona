from dotenv import load_dotenv
import os
load_dotenv()
print('cwd:', __import__('os').getcwd())
print('AEMET_API_KEY_present:', bool(os.getenv('AEMET_API_KEY')))
print('Primeros_6_caracteres:', (os.getenv('AEMET_API_KEY') or '')[:6])
