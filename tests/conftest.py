import os
import sys

"""
Configuration globale pour pytest.
"""

# Ajouter les dossiers au path Python
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, 'api'))
sys.path.insert(0, os.path.join(ROOT_DIR, 'dags'))