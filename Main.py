#  ===========================================================================
                        # IMPORT DEPENDENCIES
#  ===========================================================================

import pandas as pd
import numpy as np
import time
import logging
import mysql.connector as mariadb 
from datetime import datetime

# import helper functions
from  Fonctions.extract_load import DataBase
from Fonctions.preprocessing import preprocess_recovery


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.debug("test")

#  ===========================================================================


#  ===========================================================================
                        # LOG CONFIGURATION
#  ===========================================================================
logging.basicConfig(filename='./Log_eff_to_hub.log',
filemode='a', format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
 datefmt='%Y-%m-%d %H:%M:%S', level=logging.DEBUG)
logging.info('Log Started')


#  ===========================================================================
                        # MAIN
#  ===========================================================================

def main():
  
    bdd = DataBase()

    # connect to local server
    bdd_cridentials ={
        'user' : '',
        'password' : '',
        'host' : '',
        'port' : 3306,
        'database' : ''  
    }

    bdd_recovery.connect_to_db(
        bdd_cridentials['user'],
        bdd_cridentials['password'],
        bdd_cridentials['host'],
        bdd_cridentials['port'],
        bdd_cridentials['database'],
    )
    
    bdd2 = DataBase()

    # connect to local server
    bdd2_cridentials ={
        'user' : '',
        'password' : '',
        'host' : '',
        'port' : 3306,
        'database' : ''  
    }

    bdd2_recovery.connect_to_db(
        bdd2_cridentials['user'],
        bdd2_cridentials['password'],
        bdd2_cridentials['host'],
        bdd2_cridentials['port'],
        bdd2_cridentials['database'],
    )


    #  ============================================================================
                        # SELECT DATA FROM HUB where MigrationApproved=0
    #  ============================================================================

    alldata = bdd.query("""SELECT * FROM  <TABLE NAME> """)
    
    logging.info('Number of Records  to be migrate,{}'.format(len(alldata)))


    #  ==============================================================================
                        # Preprocess Recovery Data 
    #  ==============================================================================

    preprocess_recovery(Alldata)

   
    # Close Connections to the databases

    bdd.close()
    bdd2.close()

#  ===========================================================================
            # RUN THE SCRIPT
#  ===========================================================================

if __name__ == '__main__':
    main()
    logging.info('Log Finished')


