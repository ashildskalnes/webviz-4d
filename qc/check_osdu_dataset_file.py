import io
import xtgeo
from webviz_4d._datainput._osdu import DefaultOsduService
import matplotlib
matplotlib.use('TkAgg')
from hashlib import md5

import warnings
from datetime import datetime
warnings.filterwarnings("ignore")
        

def main():
    osdu_service = DefaultOsduService()

    dataset_id = "npequinor-dev:dataset--File.Generic:e48cd079-9145-4520-aab3-6c28c3c9551a"
    print(dataset_id)
    dataset = osdu_service.get_horizon_map(file_id=dataset_id)

    try:
        blob = io.BytesIO(dataset.content)
        md5sum = md5(blob.getbuffer())
        print(md5sum.hexdigest())
        surface = xtgeo.surface_from_file(blob)
        print(surface)
        #surface.quickplot(title=dataset_id)
    except Exception as inst:
        print("ERROR")
        print(type(inst))




    

    


        
    



   

if __name__ == '__main__':
    main()