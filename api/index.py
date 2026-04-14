import sys
import os

# Add this file's directory to path so Python can find main.py, bonds_data.py, etc.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import app
