import os
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from datetime import date

#-----------------------------------------------------------------

arquivo_bronze = "/db_medallion/bronze/nova_xs/novaxs_270"