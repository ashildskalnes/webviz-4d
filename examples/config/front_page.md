### Welcome to the Webviz-4D QC application

This version is developed as QC tool for loading 4D attribute maps from different data sources. The currently possible data sources are:

##### fmu
    - Searching for attribute maps with metadata defined by fmu-datio
    - Loading attribute maps from on-premise disk files created by an FMU execution (file format: xtgeo binary)
    
##### sumo
    - Searching for attribute maps with metadata defined by fmu-datio
    - Loading attribute maps in SUMO created and uploaded by an FMU execution (file format: xtgeo BytesIO blob)
  
##### auto4d_file
    - Searching for attribute maps with metadata defined by auto4d and or decoding from Openworks object names
    - Loading attribute maps from on-premise disk files copied from OpenWorks by auto4d (file format: xtgeo binary)
    
##### osdu
    - Searching for attribute maps with metadata defined by SeismicAttributeInterpretation 0.4.2 schema 
    - Metadata ingested by auto4d (created by auto4d and/or decoding from Openworks object names)
    - Loading attribute maps in OSDU Core (file format: xtgeo BytesIO blob)
    
##### rddms
    - Searching for attribute maps with metadata defined by defined by SeismicAttributeInterpretation 0.4.2 schema
    - Metadata from manually created json-files
    - Loading attribute maps in OSDU RDDMS (file format: resqml/json)
