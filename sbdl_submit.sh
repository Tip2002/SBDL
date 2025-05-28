spark-submit --master yarn --deploy-mode cluster \
--py-files sdbl_lib.zip \
--files conf/sdbl.conf,conf/spark.conf,log4j.properties \
--driver-core 2 \
--conf spark.driver.memoryOverhead=1G
sbdl_main.py qa 2022-08-02



